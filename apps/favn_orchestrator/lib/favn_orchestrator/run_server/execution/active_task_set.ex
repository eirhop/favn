defmodule FavnOrchestrator.RunServer.Execution.ActiveTaskSet do
  @moduledoc """
  Explicit set of durable runner tasks currently owned by a run server.

  The set centralizes task ids, admission leases, and materialization claims so
  cancellation, recovery, and cleanup do not depend on a live runner process.
  """

  alias FavnOrchestrator.CancellationOutcome
  alias FavnOrchestrator.ExecutionAdmission
  alias FavnOrchestrator.MaterializationClaims
  alias FavnOrchestrator.RunnerTasks
  alias FavnOrchestrator.RunServer.Snapshots
  alias FavnOrchestrator.RunState

  @type task_id :: String.t()
  @type entry :: map()

  @type t :: %__MODULE__{
          run_id: String.t(),
          entries: %{optional(task_id()) => entry()},
          task_ids: MapSet.t(task_id()),
          leases: %{optional(task_id()) => map()},
          materialization_claims: %{optional(task_id()) => map()}
        }

  defstruct run_id: nil,
            entries: %{},
            task_ids: MapSet.new(),
            leases: %{},
            materialization_claims: %{}

  @doc "Creates an empty work set for a run."
  @spec new(RunState.t()) :: t()
  def new(%RunState{id: run_id}), do: %__MODULE__{run_id: run_id}

  @doc "Builds a work set from runner await/admission entries."
  @spec from_entries(RunState.t(), [entry()]) :: t()
  def from_entries(%RunState{} = run_state, entries) when is_list(entries) do
    Enum.reduce(entries, new(run_state), &add_entry(&2, &1))
  end

  @doc "Adds one active runner entry to the work set."
  @spec add_entry(t(), entry()) :: t()
  def add_entry(%__MODULE__{} = work_set, entry) when is_map(entry) do
    case task_id(entry) do
      nil ->
        work_set

      task_id ->
        %{
          work_set
          | entries: Map.put(work_set.entries, task_id, entry),
            task_ids: MapSet.put(work_set.task_ids, task_id),
            leases: maybe_put(work_set.leases, task_id, Map.get(entry, :lease)),
            materialization_claims:
              maybe_put(
                work_set.materialization_claims,
                task_id,
                Map.get(entry, :materialization_claim)
              )
        }
    end
  end

  @doc false
  @spec retain_task_ids(t(), [term()]) :: t()
  def retain_task_ids(%__MODULE__{} = work_set, task_ids) when is_list(task_ids) do
    Enum.reduce(task_ids, work_set, fn task_id, acc ->
      if is_binary(task_id) and not Map.has_key?(acc.entries, task_id) do
        add_entry(acc, %{task_id: task_id})
      else
        acc
      end
    end)
  end

  @doc "Removes completed work by durable task id."
  @spec complete_entry(t(), term()) :: {entry() | nil, t()}
  def complete_entry(%__MODULE__{} = work_set, task_id) when is_binary(task_id) do
    {entry, entries} = Map.pop(work_set.entries, task_id)

    next = %{
      work_set
      | entries: entries,
        task_ids: MapSet.delete(work_set.task_ids, task_id),
        leases: Map.delete(work_set.leases, task_id),
        materialization_claims: Map.delete(work_set.materialization_claims, task_id)
    }

    {entry, next}
  end

  def complete_entry(%__MODULE__{} = work_set, _task_id), do: {nil, work_set}

  @doc "Returns active durable task ids in deterministic order."
  @spec task_ids(t()) :: [task_id()]
  def task_ids(%__MODULE__{} = work_set) do
    work_set.task_ids
    |> MapSet.to_list()
    |> Enum.sort()
  end

  @doc "Syncs durable in-flight task metadata from this set."
  @spec sync_run_metadata(RunState.t(), t()) :: RunState.t()
  def sync_run_metadata(%RunState{} = run_state, %__MODULE__{} = work_set) do
    ids = task_ids(work_set)

    Snapshots.snapshot_update(run_state,
      runner_task_id: nil,
      metadata:
        run_state.metadata
        |> Map.delete(:active_runner_task_ids)
        |> Map.delete("active_runner_task_ids")
        |> Map.put(:active_runner_task_ids, ids)
    )
  end

  @doc "Requests cancellation and retains work whose cancellation is not yet proven."
  @spec cancel_all(RunState.t(), t(), term()) :: {RunState.t(), t()}
  def cancel_all(%RunState{} = run_state, %__MODULE__{} = work_set, reason) do
    active_ids = Enum.uniq(task_ids(work_set) ++ active_runner_task_ids(run_state))

    cancel_results =
      Enum.map(active_ids, &RunnerTasks.request_cancellation(run_state.workspace_id, &1, reason))

    cancelled_ids =
      cancel_results
      |> Enum.filter(&CancellationOutcome.confirmed?/1)
      |> Enum.map(& &1.task_id)

    next_work_set =
      Enum.reduce(cancelled_ids, work_set, fn id, acc -> elem(complete_entry(acc, id), 1) end)

    run_state =
      run_state
      |> put_cancel_outcomes(cancel_results)
      |> sync_run_metadata(next_work_set)

    {run_state, next_work_set}
  end

  defp put_cancel_outcomes(%RunState{} = run_state, cancel_results) do
    outcomes = Enum.map(cancel_results, &CancellationOutcome.to_map/1)
    metadata = Map.put(run_state.metadata, :cancel_outcomes, outcomes)
    Snapshots.snapshot_update(run_state, metadata: metadata)
  end

  @doc "Releases an entry admission lease. Missing leases are already clean."
  @spec release_entry(entry() | nil) :: :ok
  def release_entry(nil), do: :ok
  def release_entry(%{lease: lease}), do: release_lease(lease)
  def release_entry(_entry), do: :ok

  @doc "Fails an entry materialization claim. Missing claims are already clean."
  @spec fail_entry_claim(entry() | nil, term()) :: :ok
  def fail_entry_claim(nil, _reason), do: :ok

  def fail_entry_claim(entry, reason) when is_map(entry) do
    case MaterializationClaims.fail_entry(entry, reason) do
      :ok -> :ok
      {:error, _reason} -> :ok
    end
  end

  @doc "Releases leases and fails claims for all active entries."
  @spec cleanup_all(t(), term()) :: :ok
  def cleanup_all(%__MODULE__{} = work_set, reason) do
    work_set.entries
    |> Map.values()
    |> Enum.each(fn entry ->
      :ok = release_entry(entry)
      :ok = fail_entry_claim(entry, reason)
    end)

    :ok
  end

  @doc false
  @spec renew_materialization_locks(t()) :: :ok | {:error, term()}
  def renew_materialization_locks(%__MODULE__{} = work_set) do
    Enum.reduce_while(work_set.materialization_claims, :ok, fn {_task_id, claim}, :ok ->
      case MaterializationClaims.renew_operation_lock(claim) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  @doc "Reads in-flight task ids from run metadata."
  @spec active_runner_task_ids(RunState.t()) :: [task_id()]
  def active_runner_task_ids(%RunState{} = run_state) do
    case Map.get(
           run_state.metadata,
           :active_runner_task_ids,
           Map.get(run_state.metadata, "active_runner_task_ids", [])
         ) do
      ids when is_list(ids) -> Enum.filter(ids, &is_binary/1)
      _other -> []
    end
  end

  @doc "Builds a work set from run metadata when entry details are unavailable."
  @spec from_run_metadata(RunState.t()) :: t()
  def from_run_metadata(%RunState{} = run_state) do
    Enum.reduce(active_runner_task_ids(run_state), new(run_state), fn task_id, acc ->
      add_entry(acc, %{task_id: task_id})
    end)
  end

  defp release_lease(nil), do: :ok

  defp release_lease(lease) when is_map(lease) do
    case ExecutionAdmission.release(lease) do
      :ok -> :ok
      {:error, _reason} -> :ok
    end
  end

  defp maybe_put(map, _task_id, nil), do: map
  defp maybe_put(map, task_id, value), do: Map.put(map, task_id, value)

  defp task_id(entry) do
    case Map.get(entry, :task_id) do
      id when is_binary(id) -> id
      _other -> nil
    end
  end
end
