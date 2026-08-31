defmodule FavnOrchestrator.ManifestDeploymentDispatcher do
  @moduledoc false

  use GenServer

  require Logger

  alias Favn.Manifest.Index
  alias Favn.Manifest.Version
  alias FavnOrchestrator.ManifestActivationDiagnostics
  alias FavnOrchestrator.ManifestDeploymentClaimHeartbeat
  alias FavnOrchestrator.ManifestDeployments
  alias FavnOrchestrator.ManifestIndexCache
  alias FavnOrchestrator.ManifestMemory
  alias FavnOrchestrator.ManifestMemory.Slot
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.Manifests
  alias FavnOrchestrator.Persistence.CommandIdempotency
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Results.ManifestDeployment, as: Deployment
  alias FavnOrchestrator.Persistence.Results.RuntimeState
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.RuntimeConfig
  alias FavnOrchestrator.TargetCompatibilityPlanner

  @poll_ms 1_000
  @claim_seconds 45
  @heartbeat_ms 15_000
  @default_concurrency 1

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    inspection_timeout_ms =
      Keyword.get(
        opts,
        :inspection_timeout_ms,
        TargetCompatibilityPlanner.default_inspection_timeout_ms()
      )

    if is_integer(inspection_timeout_ms) and inspection_timeout_ms > 0 do
      state = %{
        owner: "#{RuntimeConfig.instance_id()}:manifest-deployments",
        concurrency: Keyword.get(opts, :concurrency, @default_concurrency),
        inspection_timeout_ms: inspection_timeout_ms,
        preparation_timeout_ms:
          Keyword.get(opts, :preparation_timeout_ms, ManifestMemory.bounds().worker_timeout),
        capacity_check: Keyword.get(opts, :capacity_check, &ManifestMemory.ensure_headroom/0),
        version_size_check:
          Keyword.get(opts, :version_size_check, &ManifestMemory.valid_version_size?/1),
        manifest_slot: Keyword.get(opts, :manifest_slot, Slot),
        active: %{}
      }

      send(self(), :poll)
      {:ok, state}
    else
      {:stop, :invalid_manifest_inspection_timeout}
    end
  end

  @impl true
  def handle_info(:poll, state) do
    state = fill_capacity(state)
    Process.send_after(self(), :poll, @poll_ms)
    {:noreply, state}
  end

  def handle_info({reference, _result}, state) when is_reference(reference) do
    Process.demonitor(reference, [:flush])
    send(self(), :poll)
    {:noreply, %{state | active: Map.delete(state.active, reference)}}
  end

  def handle_info({:DOWN, reference, :process, _pid, reason}, state) do
    case Map.pop(state.active, reference) do
      {nil, _active} ->
        {:noreply, state}

      {operation, active} ->
        Logger.warning("manifest deployment worker stopped",
          workspace_id: operation.workspace_id,
          operation_id: operation.operation_id,
          failure_class: exit_class(reason)
        )

        send(self(), :poll)
        {:noreply, %{state | active: active}}
    end
  end

  defp fill_capacity(state) when map_size(state.active) >= state.concurrency, do: state

  defp fill_capacity(state) do
    expires_at = DateTime.add(DateTime.utc_now(), @claim_seconds, :second)

    case ManifestDeployments.claim_next(state.owner, expires_at) do
      {:ok, nil} ->
        state

      {:ok, operation} ->
        worker_opts =
          Map.take(state, [
            :owner,
            :inspection_timeout_ms,
            :preparation_timeout_ms,
            :capacity_check,
            :version_size_check,
            :manifest_slot
          ])

        task =
          Task.Supervisor.async_nolink(
            FavnOrchestrator.ManifestDeploymentTaskSupervisor,
            fn -> run_claimed(operation, worker_opts) end
          )

        fill_capacity(%{state | active: Map.put(state.active, task.ref, operation)})

      {:error, reason} ->
        Logger.warning("manifest deployment claim unavailable",
          failure_class: failure_class(reason)
        )

        state
    end
  end

  defp run_claimed(operation, state) do
    case ManifestMemory.with_phase(
           :activation,
           fn -> run_claimed_with_phase(operation, state) end,
           slot: state.manifest_slot,
           capacity_check: state.capacity_check
         ) do
      {:error, reason} when reason in [:manifest_capacity_busy, :manifest_capacity_unavailable] ->
        ManifestDeployments.release_claim(operation, state.owner)

      {:error, :memory_capacity_unknown} ->
        ManifestDeployments.release_claim(operation, state.owner)

      result ->
        result
    end
  end

  defp run_claimed_with_phase(operation, state) do
    heartbeat =
      ManifestDeploymentClaimHeartbeat.start(
        fn ->
          ManifestDeployments.renew_claim(
            operation,
            state.owner,
            DateTime.add(DateTime.utc_now(), @claim_seconds, :second)
          )
        end,
        interval_ms: @heartbeat_ms
      )

    try do
      execute(operation, state)
    after
      ManifestDeploymentClaimHeartbeat.stop(heartbeat)
    end
  end

  defp execute(operation, state) do
    case prepare_activation(operation, state) do
      {:ok, version} ->
        execute_prepared(operation, state, version)

      {:error, reason} when reason in [:manifest_worker_timeout, :manifest_worker_failed] ->
        ManifestDeployments.release_claim(operation, state.owner)

      {:error, reason}
      when reason in [:manifest_capacity_unavailable, :memory_capacity_unknown] ->
        ManifestDeployments.release_claim(operation, state.owner)

      {:error, reason} ->
        complete_activation(operation, state.owner, {:error, reason})
    end
  end

  defp execute_prepared(operation, state, version) do
    result = activate(operation, state, version)

    reconciled =
      case result do
        {:error, reason} ->
          if unknown_outcome?(reason),
            do: activate(operation, state, version),
            else: result

        {:ok, _runtime} ->
          result
      end

    case reconciled do
      {:error, reason}
      when reason in [:manifest_capacity_unavailable, :memory_capacity_unknown] ->
        ManifestDeployments.release_claim(operation, state.owner)

      result ->
        complete_activation(operation, state.owner, result)
    end
  end

  defp prepare_activation(operation, state) do
    platform = platform_context(operation)

    case ManifestMemory.manifest_worker(
           fn ->
             with {:ok, version} <-
                    ManifestStore.get_manifest(platform, operation.manifest_version_id),
                  true <- state.version_size_check.(version),
                  {:ok, index} <- Index.build_from_version(version),
                  true <-
                    version
                    |> ManifestIndexCache.entry_retained_bytes(index)
                    |> ManifestMemory.valid_index_size?() do
               version
             else
               false -> {:error, :manifest_memory_budget_exceeded}
               {:error, _reason} = error -> error
             end
           end,
           timeout: state.preparation_timeout_ms
         ) do
      {:ok, %Version{} = version, _retained_bytes} -> {:ok, version}
      {:ok, {:error, reason}, _retained_bytes} -> {:error, reason}
      {:error, reason} -> {:error, reason}
    end
  end

  defp activate(operation, state, version) do
    owner = state.owner
    inspection_timeout_ms = state.inspection_timeout_ms

    platform =
      platform_context(operation)

    workspace =
      SystemContext.workspace(operation.workspace_id, :manifest_deployment_activation,
        roles: [:platform_operator],
        request_id: operation.operation_id
      )

    with :ok <- state.capacity_check.(),
         {:ok, idempotency} <- operation_idempotency(operation) do
      inspection_deadline_at = inspection_deadline_at(operation, inspection_timeout_ms)

      Manifests.deploy(
        platform,
        workspace,
        operation.manifest_version_id,
        ManifestDeployments.fixed_selection(),
        deployment_id: operation.operation_id,
        activation_operation_id: operation.operation_id,
        activation_inspection_timeout_ms: inspection_timeout_ms,
        activation_inspection_deadline_at: inspection_deadline_at,
        activation_progress: fn completed, total ->
          ManifestDeployments.update_progress(operation, owner, completed, total)
        end,
        execution_pool_policy: %{approve_manifest_defaults: true},
        prepared_version: version,
        idempotency: idempotency
      )
    end
  end

  defp platform_context(operation) do
    SystemContext.platform(:manifest_deployment_activation,
      roles: [:platform_operator],
      request_id: operation.operation_id
    )
  end

  @doc false
  @spec inspection_deadline_at(Deployment.t(), pos_integer()) :: DateTime.t()
  def inspection_deadline_at(%Deployment{activating_at: %DateTime{} = activating_at}, timeout_ms)
      when is_integer(timeout_ms) and timeout_ms > 0,
      do: DateTime.add(activating_at, timeout_ms, :millisecond)

  @doc false
  @spec complete_activation(
          Deployment.t(),
          String.t(),
          {:ok, RuntimeState.t()} | {:error, term()}
        ) :: :ok | {:ok, Deployment.t()} | {:error, term()}
  def complete_activation(operation, owner, result) do
    case result do
      {:ok, runtime} ->
        diagnostics = ManifestActivationDiagnostics.to_map(runtime.activation_diagnostics)

        state =
          if diagnostics.unresolved_inspection_count > 0,
            do: :needs_attention,
            else: :succeeded

        ManifestDeployments.complete(operation, owner, state,
          deployment_id: runtime.deployment_id,
          activation_diagnostics: diagnostics
        )

      {:error, %Error{kind: :conflict, details: %{reason: :manifest_activation_in_progress}}} ->
        ManifestDeployments.release_claim(operation, owner)

      {:error, %Error{kind: :conflict, details: %{reason: :command_in_progress}}} ->
        ManifestDeployments.release_claim(operation, owner)

      {:error, reason}
      when reason in [:runtime_starting, :runtime_maintenance, :runtime_draining] ->
        ManifestDeployments.release_claim(operation, owner)

      {:error, reason} ->
        state = if unknown_outcome?(reason), do: :unknown, else: :failed

        ManifestDeployments.complete(operation, owner, state,
          failure_class: failure_class(reason)
        )
    end
  end

  defp operation_idempotency(operation) do
    key_hash = :crypto.hash(:sha256, operation.operation_id)
    {:ok, request_fingerprint} = Base.decode16(operation.request_fingerprint, case: :lower)

    CommandIdempotency.new(
      "manifest.activate",
      :service,
      "manifest-deployment:" <> operation.service_identity,
      key_hash,
      request_fingerprint,
      DateTime.add(DateTime.utc_now(), 365, :day)
    )
  end

  defp unknown_outcome?(%Error{kind: kind}) when kind in [:internal, :unavailable], do: true
  defp unknown_outcome?(_reason), do: false

  defp failure_class(%Error{kind: kind, details: details}) do
    case Map.get(details || %{}, :reason) do
      reason when is_atom(reason) -> Atom.to_string(reason)
      _other -> Atom.to_string(kind)
    end
  end

  defp failure_class(reason) when is_atom(reason), do: Atom.to_string(reason)
  defp failure_class({reason, _detail}) when is_atom(reason), do: Atom.to_string(reason)
  defp failure_class(_reason), do: "activation_failed"

  defp exit_class(:normal), do: "normal"
  defp exit_class(:shutdown), do: "shutdown"
  defp exit_class(_reason), do: "worker_exit"
end
