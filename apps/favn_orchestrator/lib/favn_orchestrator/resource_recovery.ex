defmodule FavnOrchestrator.ResourceRecovery do
  @moduledoc "Submits opt-in linked recovery runs for candidates released by a successful probe."

  use GenServer

  alias Favn.Resource.Ref
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Commands.ClaimResourceRecovery
  alias FavnOrchestrator.Persistence.Commands.CompleteResourceRecovery
  alias FavnOrchestrator.Persistence.Commands.ListPendingResourceRecoveries
  alias FavnOrchestrator.Persistence.Results.ResourceRecoveryBatch
  alias FavnOrchestrator.Persistence.Results.ResourceRecoveryCandidate
  alias FavnOrchestrator.Persistence.Results.ResourceRecoveryWakeup
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.RunManager
  alias FavnOrchestrator.Runs

  @claim_limit 100
  @claim_lease_ms 300_000
  @sweep_limit 100
  @default_sweep_interval_ms 5_000

  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) when is_list(opts),
    do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    state = %{sweep_interval_ms: Keyword.get(opts, :sweep_interval_ms, sweep_interval_ms())}
    send(self(), :sweep)
    {:ok, state}
  end

  @impl true
  def handle_info(:sweep, state) do
    sweep()
    Process.send_after(self(), :sweep, state.sweep_interval_ms)
    {:noreply, state}
  end

  @doc "Schedules bounded recovery work without delaying probe settlement."
  @spec enqueue(String.t(), Ref.t()) :: :ok
  def enqueue(workspace_id, %Ref{} = resource) when is_binary(workspace_id) do
    task = fn -> recover(workspace_id, resource) end

    case Process.whereis(FavnOrchestrator.RunManagerTaskSupervisor) do
      nil ->
        :ok

      _pid ->
        _ = Task.Supervisor.start_child(FavnOrchestrator.RunManagerTaskSupervisor, task)
        :ok
    end
  end

  @doc false
  @spec recover(String.t(), Ref.t()) :: :ok | :retry
  def recover(workspace_id, %Ref{} = resource) do
    owner_id = "resource-recovery:#{System.unique_integer([:positive, :monotonic])}"
    context = SystemContext.workspace(workspace_id, :resource_recovery)

    command = %ClaimResourceRecovery{
      workspace_context: context,
      command_id: owner_id,
      owner_id: owner_id,
      resource: resource,
      limit: @claim_limit,
      claim_lease_ms: @claim_lease_ms,
      occurred_at: DateTime.utc_now()
    }

    case Persistence.stores().resource_circuits.claim_recovery(command) do
      {:ok, %ResourceRecoveryBatch{candidates: candidates}} ->
        candidates
        |> Enum.group_by(& &1.source_run_id)
        |> Enum.map(fn {source_run_id, source_candidates} ->
          submit_group(context, owner_id, source_run_id, source_candidates, resource)
        end)
        |> then(fn results -> if :retry in results, do: :retry, else: :ok end)

      {:error, _reason} ->
        :retry
    end
  end

  defp submit_group(context, owner_id, source_run_id, candidates, resource) do
    node_keys = Enum.map(candidates, & &1.node_key)
    candidate_ids = candidates |> Enum.map(&candidate_id/1) |> Enum.sort()
    recovery_run_id = recovery_run_id(context.workspace_id, source_run_id, candidates, resource)

    opts = [
      run_id: recovery_run_id,
      replay_node_keys: node_keys,
      replay_mode: :resume_from_failure,
      metadata: %{
        resource_recovery: true,
        resource_recovery_source_run_id: source_run_id,
        resource_recovery_resource: resource,
        resource_recovery_candidate_ids: candidate_ids
      },
      trigger: %{
        kind: :resource_recovery,
        source_run_id: source_run_id,
        resource_kind: resource.kind,
        resource_name: resource.name
      }
    ]

    case RunManager.rerun(context, source_run_id, opts) do
      {:ok, ^recovery_run_id} ->
        case complete(context, owner_id, candidates, :submitted, recovery_run_id) do
          :ok -> :ok
          {:error, _reason} -> :retry
        end

      {:error, _reason} ->
        if existing_recovery_run?(context, recovery_run_id, source_run_id, candidate_ids) do
          case complete(context, owner_id, candidates, :submitted, recovery_run_id) do
            :ok -> :ok
            {:error, _reason} -> :retry
          end
        else
          _completion = complete(context, owner_id, candidates, :pending, nil)
          :retry
        end
    end
  end

  defp complete(context, owner_id, candidates, status, recovery_run_id) do
    command = %CompleteResourceRecovery{
      workspace_context: context,
      owner_id: owner_id,
      candidate_ids: Enum.map(candidates, &candidate_id/1),
      status: status,
      recovery_run_id: recovery_run_id,
      occurred_at: DateTime.utc_now()
    }

    Persistence.stores().resource_circuits.complete_recovery(command)
  end

  defp candidate_id(%ResourceRecoveryCandidate{candidate_id: candidate_id}), do: candidate_id

  defp existing_recovery_run?(context, recovery_run_id, source_run_id, candidate_ids) do
    case Runs.get(context, recovery_run_id) do
      {:ok, run} ->
        metadata = run.metadata || %{}

        field(metadata, :resource_recovery) == true and
          field(metadata, :resource_recovery_source_run_id) == source_run_id and
          Enum.sort(field(metadata, :resource_recovery_candidate_ids, [])) == candidate_ids

      {:error, _reason} ->
        false
    end
  end

  defp sweep do
    command = %ListPendingResourceRecoveries{
      platform_context: SystemContext.platform(:resource_recovery_sweep),
      limit: @sweep_limit,
      occurred_at: DateTime.utc_now()
    }

    case Persistence.stores().resource_circuits.list_pending_recoveries(command) do
      {:ok, wakeups} -> Enum.each(wakeups, &enqueue_wakeup/1)
      {:error, _reason} -> :ok
    end
  end

  defp enqueue_wakeup(%ResourceRecoveryWakeup{workspace_id: workspace_id, resource: resource}),
    do: enqueue(workspace_id, resource)

  @doc false
  @spec recovery_run_id(String.t(), String.t(), [ResourceRecoveryCandidate.t()], Ref.t()) ::
          String.t()
  def recovery_run_id(workspace_id, source_run_id, candidates, resource) do
    identity =
      {workspace_id, source_run_id, resource.kind, resource.name,
       candidates |> Enum.map(& &1.candidate_id) |> Enum.sort()}

    "resource-recovery-#{short_hash(identity)}"
  end

  defp short_hash(value) do
    value
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.url_encode64(padding: false)
  end

  defp sweep_interval_ms do
    case Application.get_env(:favn_orchestrator, :resource_recovery_sweep_interval_ms) do
      value when is_integer(value) and value > 0 -> value
      _other -> @default_sweep_interval_ms
    end
  end

  defp field(map, key, default \\ nil),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))
end
