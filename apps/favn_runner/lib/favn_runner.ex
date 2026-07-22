defmodule FavnRunner do
  @moduledoc """
  Runtime runner boundary facade for manifest-pinned execution.

  `FavnRunner` implements the runner client contract used by the orchestrator
  and plugin/runtime integrations. Packaged releases self-verify their baked
  runner descriptor before the server starts. Manifest, work, and inspection
  operations fail before cache or worker activity unless their required release
  id exactly matches that verified descriptor. It is not an ordinary stable
  authoring API.
  """

  @behaviour Favn.Contracts.RunnerClient

  alias Favn.Contracts.RelationInspectionRequest
  alias Favn.Contracts.RelationInspectionResult
  alias Favn.Contracts.GenerationActivationRequest
  alias Favn.Contracts.GenerationActivationResult
  alias Favn.Contracts.GenerationDiscardRequest
  alias Favn.Contracts.GenerationDiscardResult
  alias Favn.Contracts.GenerationMarker
  alias Favn.Contracts.GenerationMarkerInitializationRequest
  alias Favn.Contracts.GenerationMarkerInitializationResult
  alias Favn.Contracts.GenerationReconciliationRequest
  alias Favn.Contracts.GenerationReconciliationResult
  alias Favn.Contracts.RunnerCancellation
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Version
  alias Favn.Manifest.ExecutionPackage
  alias Favn.RuntimeInput.Resolution
  alias Favn.SQLAsset.Runtime, as: SQLAssetRuntime
  alias FavnRunner.ContextBuilder
  alias FavnRunner.GenerationWork
  alias FavnRunner.GenerationOperations
  alias FavnRunner.Lifecycle
  alias FavnRunner.ManifestResolver
  alias FavnRunner.ManifestStore
  alias FavnRunner.ReleaseVerifier
  alias FavnRunner.RuntimeInputResolver
  alias FavnRunner.Server
  alias FavnRunner.SQLRuntimePreflight
  alias FavnRunner.Shutdown

  @type execution_id :: String.t()

  @doc """
  Reports whether the verified runner runtime and bounded dependencies are ready.

  The check covers lifecycle admission, the runner server and required runtime
  processes, extensions, the manifest store, and configured data-plane adapters.
  """
  @spec readiness() ::
          :ok
          | {:error, :runner_not_available | :runner_not_ready | :runner_release_not_verified}
  def readiness do
    with {:ok, %{ready?: true, status: :ready}} <- diagnostics() do
      :ok
    else
      {:error, :runner_not_available} = error -> error
      {:error, :runner_release_not_verified} = error -> error
      _not_ready -> {:error, :runner_not_ready}
    end
  end

  @doc "Returns bounded identity for the runner release verified at startup."
  @spec release_info() :: {:ok, map()} | {:error, :runner_release_not_verified}
  def release_info, do: ReleaseVerifier.release_info()

  @doc """
  Returns redacted runner availability diagnostics.
  """
  @impl true
  @spec diagnostics(keyword()) :: {:ok, map()} | {:error, term()}
  def diagnostics(opts \\ []) when is_list(opts) do
    Server.diagnostics(opts)
  end

  @doc "Begins the irreversible bounded runner drain used before shutdown."
  @spec drain(keyword()) :: {:ok, map()}
  def drain(opts \\ []) when is_list(opts), do: Shutdown.drain(opts)

  @doc """
  Registers one pinned manifest version in the runner.
  """
  @impl true
  @spec register_manifest(Version.t(), keyword()) :: :ok | {:error, term()}
  def register_manifest(version, opts \\ [])

  def register_manifest(%Version{} = version, opts) when is_list(opts) do
    with_admission(opts, fn ->
      with :ok <- ReleaseVerifier.verify_required_release(version.required_runner_release_id) do
        Server.register_manifest(version, opts)
      end
    end)
  end

  @doc "Checks whether an exact release-bound manifest is already compiled by the runner."
  @impl true
  @spec ensure_manifest(Version.t(), keyword()) :: :ok | :missing | {:error, term()}
  def ensure_manifest(version, opts \\ [])

  def ensure_manifest(%Version{} = version, opts) when is_list(opts) do
    with :ok <- ReleaseVerifier.verify_required_release(version.required_runner_release_id) do
      ManifestStore.ensure(version.manifest_version_id, version.content_hash,
        server: Keyword.get(opts, :manifest_store, FavnRunner.ManifestStore)
      )
    end
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
    with_admission(opts, fn ->
      manifest_store = Keyword.get(opts, :manifest_store, FavnRunner.ManifestStore)

      with :ok <- ReleaseVerifier.verify_required_release(version.required_runner_release_id),
           :ok <-
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
    end)
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
    with_admission(opts, fn -> Server.submit_work(work, opts) end)
  end

  @doc "Resolves dynamic SQL inputs before work is submitted or SQL is rendered."
  @impl true
  @spec resolve_runtime_inputs(RunnerWork.t(), keyword()) ::
          {:ok, Resolution.t() | nil} | {:error, term()}
  def resolve_runtime_inputs(%RunnerWork{} = work, opts \\ []) when is_list(opts) do
    with_admission(opts, fn ->
      with :ok <- ReleaseVerifier.verify_required_release(work.required_runner_release_id) do
        with_scoped_manifest_lease(work, opts, &do_resolve_runtime_inputs(&1, opts))
      end
    end)
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
      work = %{work | execution_package: package}

      with :ok <-
             GenerationWork.validate(
               work,
               asset,
               manifest,
               Keyword.get(opts, :manifest_store, FavnRunner.ManifestStore)
             ) do
        resolve_asset_runtime_inputs(asset, package, manifest, relation_by_module, work, opts)
      end
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
    with_admission(opts, fn ->
      with :ok <- ReleaseVerifier.verify_required_release(request.required_runner_release_id) do
        Server.inspect_relation(request, opts)
      end
    end)
  end

  @doc "Returns explicit target-generation capabilities for one manifest asset."
  @impl true
  @spec generation_capabilities(Version.t(), Favn.Ref.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def generation_capabilities(%Version{} = version, asset_ref, opts \\ [])
      when is_tuple(asset_ref) and is_list(opts) do
    with_admission(opts, fn ->
      with :ok <- ReleaseVerifier.verify_required_release(version.required_runner_release_id) do
        GenerationOperations.capabilities(version, asset_ref)
      end
    end)
  end

  @doc "Returns the current sidecar marker for one manifest target."
  @impl true
  @spec generation_marker(Version.t(), Favn.Ref.t(), keyword()) ::
          {:ok, GenerationMarker.t() | nil} | {:error, term()}
  def generation_marker(%Version{} = version, asset_ref, opts \\ [])
      when is_tuple(asset_ref) and is_list(opts) do
    with_admission(opts, fn ->
      with :ok <- ReleaseVerifier.verify_required_release(version.required_runner_release_id) do
        GenerationOperations.marker(version, asset_ref)
      end
    end)
  end

  @doc "Initializes the sidecar marker for one successfully materialized initial generation."
  @impl true
  @spec initialize_generation_marker(GenerationMarkerInitializationRequest.t(), keyword()) ::
          {:ok, GenerationMarkerInitializationResult.t()} | {:error, term()}
  def initialize_generation_marker(%GenerationMarkerInitializationRequest{} = request, opts \\ [])
      when is_list(opts) do
    with_admission(opts, fn ->
      with :ok <- GenerationMarkerInitializationRequest.validate(request),
           {:ok, version} <- generation_version(request, opts) do
        GenerationOperations.initialize_marker(request, version)
      end
    end)
  end

  @doc "Atomically activates one validated target-generation candidate."
  @impl true
  @spec activate_generation(GenerationActivationRequest.t(), keyword()) ::
          {:ok, GenerationActivationResult.t()} | {:error, term()}
  def activate_generation(%GenerationActivationRequest{} = request, opts \\ [])
      when is_list(opts) do
    with_admission(opts, fn ->
      with :ok <- GenerationActivationRequest.validate(request),
           {:ok, version} <- generation_version(request, opts) do
        GenerationOperations.activate(request, version)
      end
    end)
  end

  @doc "Reconciles the marker and relations for a possibly committed activation."
  @impl true
  @spec reconcile_generation(GenerationReconciliationRequest.t(), keyword()) ::
          {:ok, GenerationReconciliationResult.t()} | {:error, term()}
  def reconcile_generation(
        %GenerationReconciliationRequest{activation: activation} = request,
        opts \\ []
      )
      when is_list(opts) do
    with_admission(opts, fn ->
      with :ok <- GenerationReconciliationRequest.validate(request),
           {:ok, version} <- generation_version(activation, opts) do
        GenerationOperations.reconcile(request, version)
      end
    end)
  end

  @doc "Discards one non-active candidate generation idempotently."
  @impl true
  @spec discard_generation(GenerationDiscardRequest.t(), keyword()) ::
          {:ok, GenerationDiscardResult.t()} | {:error, term()}
  def discard_generation(%GenerationDiscardRequest{} = request, opts \\ [])
      when is_list(opts) do
    with_admission(opts, fn ->
      with :ok <- GenerationDiscardRequest.validate(request),
           {:ok, version} <- generation_version(request, opts) do
        GenerationOperations.discard(request, version)
      end
    end)
  end

  defp with_admission(opts, fun) do
    Lifecycle.with_admission(fun, Keyword.get(opts, :lifecycle, Lifecycle))
  end

  defp generation_version(request, opts) do
    with :ok <- ReleaseVerifier.verify_required_release(request.required_runner_release_id),
         {:ok, version} <-
           ManifestStore.fetch(request.manifest_version_id, request.manifest_content_hash,
             server: Keyword.get(opts, :manifest_store, FavnRunner.ManifestStore)
           ),
         :ok <- ReleaseVerifier.verify_required_release(version.required_runner_release_id),
         true <-
           version.required_runner_release_id == request.required_runner_release_id or
             {:error, :runner_release_mismatch} do
      {:ok, version}
    end
  end

  @doc """
  Runs one work request synchronously through the same runner server boundary.
  """
  @spec run(RunnerWork.t(), keyword()) :: {:ok, RunnerResult.t()} | {:error, term()}
  def run(%RunnerWork{} = work, opts \\ []) when is_list(opts) do
    with_admission(opts, fn ->
      with :ok <- ReleaseVerifier.verify_required_release(work.required_runner_release_id) do
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
