defmodule FavnRunner do
  @moduledoc """
  Runtime runner boundary facade for manifest-pinned execution.

  `FavnRunner` implements the runner client contract used by the orchestrator
  and plugin/runtime integrations. It is not an ordinary stable authoring API.
  """

  @behaviour Favn.Contracts.RunnerClient

  alias Favn.Contracts.RelationInspectionRequest
  alias Favn.Contracts.RelationInspectionResult
  alias Favn.Contracts.RunnerCancellation
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Version
  alias Favn.Manifest.ExecutionPackage
  alias Favn.RuntimeInput.Resolution
  alias Favn.SQLAsset.Runtime, as: SQLAssetRuntime
  alias FavnRunner.ContextBuilder
  alias FavnRunner.ManifestResolver
  alias FavnRunner.ManifestStore
  alias FavnRunner.RuntimeInputResolver
  alias FavnRunner.Server
  alias FavnRunner.SQLRuntimePreflight

  @type execution_id :: String.t()

  @doc """
  Reports whether the runner server process is available.

  This is a local process availability check only. It does not submit work or
  validate runtime dependencies behind the runner boundary.
  """
  @spec readiness() :: :ok | {:error, :runner_not_available}
  def readiness do
    case Process.whereis(Server) do
      pid when is_pid(pid) -> :ok
      nil -> {:error, :runner_not_available}
    end
  end

  @doc """
  Returns redacted runner availability diagnostics.
  """
  @impl true
  @spec diagnostics(keyword()) :: {:ok, map()} | {:error, term()}
  def diagnostics(opts \\ []) when is_list(opts) do
    Server.diagnostics(opts)
  end

  @doc """
  Registers one pinned manifest version in the runner.
  """
  @impl true
  @spec register_manifest(Version.t(), keyword()) :: :ok | {:error, term()}
  def register_manifest(version, opts \\ [])

  def register_manifest(%Version{} = version, opts) when is_list(opts),
    do: Server.register_manifest(version, opts)

  @doc "Checks whether an exact manifest identity is already compiled by the runner."
  @impl true
  @spec ensure_manifest(String.t(), String.t(), keyword()) ::
          :ok | :missing | {:error, term()}
  def ensure_manifest(manifest_version_id, content_hash, opts \\ [])
      when is_binary(manifest_version_id) and is_binary(content_hash) and is_list(opts) do
    ManifestStore.ensure(manifest_version_id, content_hash,
      server: Keyword.get(opts, :manifest_store, FavnRunner.ManifestStore)
    )
  end

  @doc "Atomically registers and leases one manifest identity for an active run."
  @impl true
  @spec acquire_manifest(Version.t(), String.t(), DateTime.t(), [Favn.Ref.t()], keyword()) ::
          :ok | {:error, term()}
  def acquire_manifest(
        %Version{} = version,
        lease_id,
        %DateTime{} = expires_at,
        planned_asset_refs,
        opts \\ []
      )
      when is_binary(lease_id) and is_list(planned_asset_refs) and is_list(opts) do
    manifest_store = Keyword.get(opts, :manifest_store, FavnRunner.ManifestStore)

    with :ok <-
           ManifestStore.acquire(version, lease_id, expires_at,
             server: manifest_store,
             timeout: Keyword.get(opts, :timeout, 30_000)
           ) do
      case SQLRuntimePreflight.run(version, planned_asset_refs) do
        :ok ->
          :ok

        {:error, _diagnostic} = error ->
          :ok = ManifestStore.release(lease_id, server: manifest_store)
          error
      end
    end
  end

  @doc "Releases an active-run manifest lease."
  @impl true
  @spec release_manifest(String.t(), keyword()) :: :ok
  def release_manifest(lease_id, opts \\ []) when is_binary(lease_id) and is_list(opts) do
    ManifestStore.release(lease_id,
      server: Keyword.get(opts, :manifest_store, FavnRunner.ManifestStore)
    )
  end

  @doc "Renews an active-run manifest lease."
  @impl true
  @spec renew_manifest(String.t(), DateTime.t(), keyword()) :: :ok | {:error, term()}
  def renew_manifest(lease_id, %DateTime{} = expires_at, opts \\ [])
      when is_binary(lease_id) and is_list(opts) do
    ManifestStore.renew(lease_id, expires_at,
      server: Keyword.get(opts, :manifest_store, FavnRunner.ManifestStore)
    )
  end

  @doc """
  Submits one manifest-pinned work request for asynchronous execution.
  """
  @impl true
  @spec submit_work(RunnerWork.t(), keyword()) :: {:ok, execution_id()} | {:error, term()}
  def submit_work(%RunnerWork{} = work, opts \\ []) when is_list(opts) do
    Server.submit_work(work, opts)
  end

  @doc "Resolves dynamic SQL inputs before work is submitted or SQL is rendered."
  @impl true
  @spec resolve_runtime_inputs(RunnerWork.t(), keyword()) ::
          {:ok, Resolution.t() | nil} | {:error, term()}
  def resolve_runtime_inputs(%RunnerWork{} = work, opts \\ []) when is_list(opts) do
    with_scoped_manifest_lease(work, opts, &do_resolve_runtime_inputs(&1, opts))
  end

  defp do_resolve_runtime_inputs(%RunnerWork{} = work, opts) do
    with {:ok, asset_ref} <- ManifestResolver.resolve_target_ref(work),
         {:ok, manifest, asset, relation_by_module} <-
           ManifestStore.fetch_execution_bundle(
             work.manifest_lease_id,
             work.manifest_version_id,
             work.manifest_content_hash,
             asset_ref,
             work.execution_package,
             server: Keyword.get(opts, :manifest_store, FavnRunner.ManifestStore)
           ),
         {:ok, package} <- ExecutionPackage.verify_for_asset(work.execution_package, asset) do
      resolve_asset_runtime_inputs(asset, package, manifest, relation_by_module, work, opts)
    end
  end

  defp resolve_asset_runtime_inputs(
         _asset,
         %ExecutionPackage{sql_execution: %{runtime_inputs: nil}},
         _manifest,
         _relation_by_module,
         _work,
         _opts
       ),
       do: {:ok, nil}

  defp resolve_asset_runtime_inputs(
         asset,
         %ExecutionPackage{sql_execution: %{runtime_inputs: resolver}} = package,
         manifest,
         relation_by_module,
         work,
         opts
       ) do
    execution_id = "resolve_" <> (work.asset_step_id || work.run_id)

    with {:ok, context} <- ContextBuilder.build(work, asset, execution_id),
         {:ok, _definition, final_context, final_opts} <-
           SQLAssetRuntime.prepare_manifest_execution(
             asset,
             package,
             manifest,
             relation_by_module,
             work,
             context
           ),
         {:ok, resolution} <-
           RuntimeInputResolver.resolve(
             resolver,
             final_context,
             final_context.params,
             resolver_opts(opts, final_opts)
           ) do
      lineage = RuntimeInputResolver.lineage(resolution)

      Resolution.new(%{
        resolver: resolution.resolver,
        params: resolution.params,
        input_identity: lineage.input_identity,
        metadata: lineage.input_metadata,
        sensitive_params: resolution.sensitive_params,
        duration_ms: resolution.duration_ms
      })
    else
      {:error, error} ->
        retryable? = resolver_retryable?(error)

        {:error,
         RunnerError.normalize(error,
           phase: error_phase(error),
           retryable?: retryable?,
           retry_after_ms: resolver_retry_after(error),
           outcome: if(retryable?, do: :safe_failure, else: :unknown)
         )}
    end
  end

  defp resolve_asset_runtime_inputs(
         _asset,
         _package,
         _manifest,
         _relation_by_module,
         _work,
         _opts
       ),
       do: {:ok, nil}

  defp resolver_retryable?(%{details: details}) when is_map(details),
    do: Map.get(details, :asset_retryable?, Map.get(details, "asset_retryable?", false)) == true

  defp resolver_retryable?(_error), do: false

  defp resolver_opts(caller_opts, final_opts) do
    timeout_ms =
      [Keyword.get(caller_opts, :timeout_ms), Keyword.get(final_opts, :timeout_ms)]
      |> Enum.filter(&(is_integer(&1) and &1 > 0))
      |> Enum.min(fn -> nil end)

    caller_opts
    |> Keyword.merge(Keyword.take(final_opts, [:deadline, :cancel_token]))
    |> maybe_put_timeout(timeout_ms)
  end

  defp maybe_put_timeout(opts, timeout_ms) when is_integer(timeout_ms) and timeout_ms > 0,
    do: Keyword.put(opts, :timeout_ms, timeout_ms)

  defp maybe_put_timeout(opts, _timeout_ms), do: opts

  defp error_phase(error) when is_map(error), do: Map.get(error, :phase, :runtime_inputs)
  defp error_phase(_error), do: :runtime_inputs

  defp resolver_retry_after(%{details: details}) when is_map(details),
    do: Map.get(details, :retry_after_ms, Map.get(details, "retry_after_ms"))

  defp resolver_retry_after(_error), do: nil

  @doc """
  Waits for one execution result.
  """
  @impl true
  @spec await_result(execution_id(), timeout(), keyword()) ::
          {:ok, RunnerResult.t()} | {:error, term()}
  def await_result(execution_id, timeout \\ 5_000, opts \\ [])

  def await_result(execution_id, timeout, opts)
      when is_binary(execution_id) and is_integer(timeout) and timeout > 0 and is_list(opts) do
    Server.await_result(execution_id, timeout, opts)
  end

  def await_result(_execution_id, _timeout, _opts), do: {:error, :invalid_await_args}

  @doc """
  Cancels one in-flight execution.
  """
  @impl true
  @spec cancel_work(execution_id(), RunnerCancellation.t(), keyword()) ::
          {:ok, RunnerCancellation.outcome()} | {:error, RunnerError.t()}
  def cancel_work(execution_id, reason \\ %{}, opts \\ [])

  def cancel_work(execution_id, reason, opts)
      when is_binary(execution_id) and is_map(reason) and is_list(opts) do
    Server.cancel_work(execution_id, RunnerCancellation.from_map(reason), opts)
  end

  def cancel_work(_execution_id, _reason, _opts) do
    {:error,
     RunnerError.normalize(:invalid_cancel_args,
       kind: :boundary,
       type: :invalid_cancel_args,
       retryable?: false
     )}
  end

  @doc """
  Subscribes a process to live logs for one runner execution.
  """
  @impl true
  @spec subscribe_execution_logs(execution_id(), pid(), keyword()) :: :ok | {:error, term()}
  def subscribe_execution_logs(execution_id, subscriber, opts \\ [])

  def subscribe_execution_logs(execution_id, subscriber, opts)
      when is_binary(execution_id) and is_pid(subscriber) and is_list(opts) do
    Server.subscribe_execution_logs(execution_id, subscriber, opts)
  end

  def subscribe_execution_logs(_execution_id, _subscriber, _opts),
    do: {:error, :invalid_log_subscription_args}

  @doc """
  Unsubscribes a process from live logs for one runner execution.
  """
  @impl true
  @spec unsubscribe_execution_logs(execution_id(), pid(), keyword()) :: :ok
  def unsubscribe_execution_logs(execution_id, subscriber, opts \\ [])

  def unsubscribe_execution_logs(execution_id, subscriber, opts)
      when is_binary(execution_id) and is_pid(subscriber) and is_list(opts) do
    Server.unsubscribe_execution_logs(execution_id, subscriber, opts)
  end

  def unsubscribe_execution_logs(_execution_id, _subscriber, _opts), do: :ok

  @doc """
  Runs one safe read-only relation inspection request through the runner boundary.
  """
  @impl true
  @spec inspect_relation(RelationInspectionRequest.t(), keyword()) ::
          {:ok, RelationInspectionResult.t()} | {:error, term()}
  def inspect_relation(%RelationInspectionRequest{} = request, opts \\ []) when is_list(opts) do
    Server.inspect_relation(request, opts)
  end

  @doc """
  Runs one work request synchronously through the same runner server boundary.
  """
  @spec run(RunnerWork.t(), keyword()) :: {:ok, RunnerResult.t()} | {:error, term()}
  def run(%RunnerWork{} = work, opts \\ []) when is_list(opts) do
    with_scoped_manifest_lease(work, opts, fn leased_work ->
      timeout = Keyword.get(opts, :timeout, 5_000)

      case preflight_work_scope(leased_work, opts) do
        :ok ->
          with {:ok, execution_id} <- submit_work(leased_work, opts) do
            await_result(execution_id, timeout, opts)
          end

        {:error, {%FavnRunner.ManifestHandle{} = handle, diagnostic}} ->
          {:ok, Server.preflight_failed_result(leased_work, handle, diagnostic)}

        {:error, reason} ->
          {:error, reason}
      end
    end)
  end

  defp preflight_work_scope(%RunnerWork{} = work, opts) do
    manifest_store = Keyword.get(opts, :manifest_store, FavnRunner.ManifestStore)

    with {:ok, handle} <-
           ManifestStore.fetch_handle(
             work.manifest_version_id,
             work.manifest_content_hash,
             server: manifest_store
           ) do
      case SQLRuntimePreflight.run(handle, RunnerWork.planned_asset_refs(work),
             server: manifest_store
           ) do
        :ok -> :ok
        {:error, diagnostic} -> {:error, {handle, diagnostic}}
      end
    end
  end

  defp with_scoped_manifest_lease(%RunnerWork{manifest_lease_id: lease_id} = work, _opts, fun)
       when is_binary(lease_id),
       do: fun.(work)

  defp with_scoped_manifest_lease(%RunnerWork{} = work, opts, fun) do
    lease_id = "direct:" <> Base.url_encode64(:crypto.strong_rand_bytes(18), padding: false)
    expires_at = DateTime.add(DateTime.utc_now(), 60, :second)
    manifest_store = Keyword.get(opts, :manifest_store, FavnRunner.ManifestStore)

    with :ok <-
           ManifestStore.acquire_registered(
             work.manifest_version_id,
             work.manifest_content_hash,
             lease_id,
             expires_at,
             server: manifest_store
           ) do
      try do
        fun.(%{work | manifest_lease_id: lease_id})
      after
        ManifestStore.release(lease_id, server: manifest_store)
      end
    end
  end
end
