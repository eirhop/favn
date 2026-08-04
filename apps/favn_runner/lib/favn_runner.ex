defmodule FavnRunner do
  @moduledoc """
  Runtime runner boundary facade for manifest-pinned execution.

  Packaged releases install the operator-owned `FAVN_RUNNER_RELEASE_ID`.
  Runner-task preparation and execution reject work unless its required release
  exactly matches that verified identity. This is not an authoring API.
  """

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
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest
  alias Favn.Manifest.ExecutionPackage
  alias Favn.Manifest.Version
  alias Favn.RuntimeInput.Resolution
  alias Favn.SQLAsset.Runtime, as: SQLAssetRuntime
  alias FavnRunner.ContextBuilder
  alias FavnRunner.GenerationWork
  alias FavnRunner.GenerationOperations
  alias FavnRunner.Inspection
  alias FavnRunner.Lifecycle
  alias FavnRunner.ManifestResolver
  alias FavnRunner.ManifestStore
  alias FavnRunner.ReleaseVerifier
  alias FavnRunner.RuntimeInputResolver
  alias FavnRunner.SQLRuntimePreflight
  alias FavnRunner.Drain

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
  @spec diagnostics(keyword()) :: {:ok, map()} | {:error, term()}
  def diagnostics(opts \\ []) when is_list(opts) do
    lifecycle = Keyword.get(opts, :lifecycle, Lifecycle)
    manifest_store = Keyword.get(opts, :manifest_store, ManifestStore)

    with {:ok, release} <- ReleaseVerifier.release_info() do
      ready? = Process.whereis(manifest_store) != nil

      {:ok,
       %{
         ready?: ready?,
         status: if(ready?, do: :ready, else: :not_ready),
         release: release,
         lifecycle: Lifecycle.diagnostics(lifecycle),
         manifest_cache: ManifestStore.diagnostics(server: manifest_store)
       }}
    end
  end

  @doc "Begins the irreversible bounded runner drain used before shutdown."
  @spec drain(keyword()) :: {:ok, map()}
  def drain(opts \\ []) when is_list(opts), do: Drain.drain(opts)

  @doc """
  Registers one pinned manifest version in the runner.
  """
  @spec register_manifest(Version.t(), keyword()) :: :ok | {:error, term()}
  def register_manifest(version, opts \\ [])

  def register_manifest(%Version{} = version, opts) when is_list(opts) do
    with_admission(opts, fn ->
      ManifestStore.register(version, opts)
    end)
  end

  @doc "Checks whether an exact release-bound manifest is already compiled by the runner."
  @spec ensure_manifest(Version.t(), keyword()) :: :ok | :missing | {:error, term()}
  def ensure_manifest(version, opts \\ [])

  def ensure_manifest(%Version{} = version, opts) when is_list(opts) do
    with {:ok, _release_id} <- current_manifest_release(version) do
      ManifestStore.ensure(version.manifest_version_id, version.content_hash,
        server: Keyword.get(opts, :manifest_store, FavnRunner.ManifestStore)
      )
    end
  end

  @doc "Atomically registers and leases one manifest identity for an active run."
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

      with {:ok, release_id} <- current_manifest_release(version),
           :ok <-
             ManifestStore.acquire_for_release(version, release_id, lease_id, expires_at,
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
  @spec release_manifest(String.t(), keyword()) :: :ok
  def release_manifest(lease_id, opts \\ []) when is_binary(lease_id) and is_list(opts) do
    ManifestStore.release(lease_id,
      server: Keyword.get(opts, :manifest_store, FavnRunner.ManifestStore)
    )
  end

  @doc "Renews an active-run manifest lease."
  @spec renew_manifest(String.t(), DateTime.t(), keyword()) :: :ok | {:error, term()}
  def renew_manifest(lease_id, %DateTime{} = expires_at, opts \\ [])
      when is_binary(lease_id) and is_list(opts) do
    ManifestStore.renew(lease_id, expires_at,
      server: Keyword.get(opts, :manifest_store, FavnRunner.ManifestStore)
    )
  end

  @doc "Resolves dynamic SQL inputs before work is submitted or SQL is rendered."
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
        # Runtime-input resolution runs strictly before any materialization
        # SQL, so a failure here cannot have touched the target: the outcome
        # is a safe failure regardless of whether retrying it makes sense.
        {:error,
         RunnerError.normalize(error,
           phase: error_phase(error),
           retryable?: resolver_retryable?(error),
           retry_after_ms: resolver_retry_after(error),
           outcome: :safe_failure
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

  defp resolver_retry_after(%{details: details}) when is_map(details),
    do: Map.get(details, :retry_after_ms, Map.get(details, "retry_after_ms"))

  defp resolver_retry_after(_error), do: nil

  @doc """
  Runs one safe read-only relation inspection request through the runner boundary.
  """
  @spec inspect_relation(RelationInspectionRequest.t(), keyword()) ::
          {:ok, RelationInspectionResult.t()} | {:error, term()}
  def inspect_relation(%RelationInspectionRequest{} = request, opts \\ []) when is_list(opts) do
    with_admission(opts, fn ->
      with :ok <- ReleaseVerifier.verify_required_release(request.required_runner_release_id),
           {:ok, version} <- generation_version(request, opts) do
        Inspection.inspect_relation(request, version, request.required_runner_release_id)
      end
    end)
  end

  @doc "Returns explicit target-generation capabilities for one manifest asset."
  @spec generation_capabilities(Version.t(), Favn.Ref.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def generation_capabilities(%Version{} = version, asset_ref, opts \\ [])
      when is_tuple(asset_ref) and is_list(opts) do
    with_admission(opts, fn ->
      with {:ok, asset} <- generation_asset(version, asset_ref, opts) do
        GenerationOperations.capabilities(asset)
      end
    end)
  end

  @doc """
  Returns the current sidecar marker for one manifest target.

  Marker reads require a matching physical-relation instance by default.
  `require_relation_instance?: false` is reserved for Favn's authorized managed
  rebuild and discard paths; recovery and unknown-outcome reconciliation must
  keep the strict default.
  """
  @spec generation_marker(Version.t(), Favn.Ref.t(), keyword()) ::
          {:ok, GenerationMarker.t() | nil} | {:error, term()}
  def generation_marker(%Version{} = version, asset_ref, opts \\ [])
      when is_tuple(asset_ref) and is_list(opts) do
    with_admission(opts, fn ->
      with {:ok, asset} <- generation_asset(version, asset_ref, opts) do
        GenerationOperations.marker(asset,
          require_relation_instance?: Keyword.get(opts, :require_relation_instance?, true)
        )
      end
    end)
  end

  @doc "Initializes the sidecar marker for one successfully materialized initial generation."
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

  defp generation_asset(%Version{manifest: %Manifest{}} = version, asset_ref, _opts) do
    with {:ok, _release_id} <- current_manifest_release(version) do
      ManifestResolver.resolve_asset(version, asset_ref)
    end
  end

  defp generation_asset(%Version{manifest: nil} = version, asset_ref, opts) do
    manifest_store = Keyword.get(opts, :manifest_store, FavnRunner.ManifestStore)

    with {:ok, _release_id} <- current_manifest_release(version),
         {:ok, handle} <-
           ManifestStore.fetch_handle(version.manifest_version_id, version.content_hash,
             server: manifest_store
           ) do
      ManifestStore.fetch_asset(handle, asset_ref, server: manifest_store)
    end
  end

  defp generation_version(request, opts) do
    with :ok <- ReleaseVerifier.verify_required_release(request.required_runner_release_id),
         {:ok, version} <-
           ManifestStore.fetch(request.manifest_version_id, request.manifest_content_hash,
             server: Keyword.get(opts, :manifest_store, FavnRunner.ManifestStore)
           ),
         true <-
           request.required_runner_release_id in Map.values(version.runner_releases) or
             {:error, :runner_release_mismatch} do
      {:ok, version}
    end
  end

  defp current_manifest_release(%Version{runner_releases: runner_releases}) do
    with {:ok, release} <- ReleaseVerifier.verified_release(),
         true <-
           release.runner_release_id in Map.values(runner_releases) or
             {:error, :manifest_runner_release_mismatch} do
      {:ok, release.runner_release_id}
    end
  end

  defp with_scoped_manifest_lease(%RunnerWork{manifest_lease_id: lease_id} = work, _opts, fun)
       when is_binary(lease_id),
       do: fun.(work)

  defp with_scoped_manifest_lease(%RunnerWork{} = work, opts, fun) do
    lease_id =
      "runtime-input:" <> Base.url_encode64(:crypto.strong_rand_bytes(18), padding: false)

    expires_at = DateTime.add(DateTime.utc_now(), 60, :second)
    manifest_store = Keyword.get(opts, :manifest_store, ManifestStore)

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
