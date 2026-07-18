defmodule FavnOrchestrator.RunServer.Execution.RunWorkSet do
  @moduledoc """
  Explicit set of runner work currently owned by a run server.

  The work set centralizes runner execution ids, admission leases, and
  materialization claims so cancellation and cleanup paths do not maintain
  in-flight metadata by hand.
  """

  alias FavnOrchestrator.CancellationOutcome
  alias FavnOrchestrator.ExecutionAdmission
  alias FavnOrchestrator.MaterializationClaims
  alias FavnOrchestrator.RunExecutionOwnership
  alias FavnOrchestrator.RunServer.Cancellation
  alias FavnOrchestrator.RunServer.Snapshots
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage.JsonSafe

  @type execution_id :: String.t()
  @type entry :: map()

  @type t :: %__MODULE__{
          run_id: String.t(),
          entries: %{optional(execution_id()) => entry()},
          runner_execution_ids: MapSet.t(execution_id()),
          leases: %{optional(execution_id()) => map()},
          materialization_claims: %{optional(execution_id()) => map()}
        }

  defstruct run_id: nil,
            entries: %{},
            runner_execution_ids: MapSet.new(),
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
    case execution_id(entry) do
      nil ->
        work_set

      execution_id ->
        %{
          work_set
          | entries: Map.put(work_set.entries, execution_id, entry),
            runner_execution_ids: MapSet.put(work_set.runner_execution_ids, execution_id),
            leases: maybe_put(work_set.leases, execution_id, Map.get(entry, :lease)),
            materialization_claims:
              maybe_put(
                work_set.materialization_claims,
                execution_id,
                Map.get(entry, :materialization_claim)
              )
        }
    end
  end

  @doc "Removes completed work by runner execution id."
  @spec complete_entry(t(), term()) :: {entry() | nil, t()}
  def complete_entry(%__MODULE__{} = work_set, execution_id) when is_binary(execution_id) do
    {entry, entries} = Map.pop(work_set.entries, execution_id)

    next = %{
      work_set
      | entries: entries,
        runner_execution_ids: MapSet.delete(work_set.runner_execution_ids, execution_id),
        leases: Map.delete(work_set.leases, execution_id),
        materialization_claims: Map.delete(work_set.materialization_claims, execution_id)
    }

    {entry, next}
  end

  def complete_entry(%__MODULE__{} = work_set, _execution_id), do: {nil, work_set}

  @doc "Returns active runner execution ids in deterministic order."
  @spec execution_ids(t()) :: [execution_id()]
  def execution_ids(%__MODULE__{} = work_set) do
    work_set.runner_execution_ids
    |> MapSet.to_list()
    |> Enum.sort()
  end

  @doc "Syncs `runner_execution_id` and in-flight metadata from this work set."
  @spec sync_run_metadata(RunState.t(), t()) :: RunState.t()
  def sync_run_metadata(%RunState{} = run_state, %__MODULE__{} = work_set) do
    ids = execution_ids(work_set)

    Snapshots.snapshot_update(run_state,
      runner_execution_id: List.first(ids),
      metadata: Map.put(run_state.metadata, :in_flight_execution_ids, ids)
    )
  end

  @doc "Cancels every active runner execution id and clears cancelled work."
  @spec cancel_all(RunState.t(), t(), term(), module(), keyword()) :: {RunState.t(), t()}
  def cancel_all(
        %RunState{} = run_state,
        %__MODULE__{} = work_set,
        reason,
        runner_client,
        runner_opts
      ) do
    active_ids = Enum.uniq(execution_ids(work_set) ++ inflight_execution_ids(run_state))

    cancel_results =
      Cancellation.dispatch_runner_work(
        run_state,
        active_ids,
        reason,
        runner_client,
        runner_opts
      )

    cancelled_ids = Enum.map(cancel_results, & &1.execution_id)

    next_work_set =
      Enum.reduce(cancelled_ids, work_set, fn id, acc -> elem(complete_entry(acc, id), 1) end)

    ledger_result =
      RunExecutionOwnership.persist_cancel_outcomes(run_state, cancel_results, reason)

    run_state =
      run_state
      |> put_cancel_outcomes(cancel_results)
      |> put_cancellation_ledger_result(ledger_result)
      |> sync_run_metadata(next_work_set)

    {run_state, next_work_set}
  end

  defp put_cancel_outcomes(%RunState{} = run_state, cancel_results) do
    outcomes = Enum.map(cancel_results, &CancellationOutcome.to_map/1)
    metadata = Map.put(run_state.metadata, :cancel_outcomes, outcomes)
    Snapshots.snapshot_update(run_state, metadata: metadata)
  end

  defp put_cancellation_ledger_result(%RunState{} = run_state, :ok) do
    metadata = Map.delete(run_state.metadata, :cancellation_ledger_persist_error)
    %{run_state | metadata: metadata} |> RunState.with_snapshot_hash()
  end

  defp put_cancellation_ledger_result(%RunState{} = run_state, {:error, reason}) do
    metadata =
      Map.put(
        run_state.metadata,
        :cancellation_ledger_persist_error,
        JsonSafe.error(reason)
      )

    %{run_state | metadata: metadata} |> RunState.with_snapshot_hash()
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

  @doc "Reads in-flight execution ids from run metadata."
  @spec inflight_execution_ids(RunState.t()) :: [execution_id()]
  def inflight_execution_ids(%RunState{} = run_state) do
    case Map.get(
           run_state.metadata,
           :in_flight_execution_ids,
           Map.get(run_state.metadata, "in_flight_execution_ids", [])
         ) do
      ids when is_list(ids) -> Enum.filter(ids, &is_binary/1)
      _other -> []
    end
  end

  @doc "Builds a work set from run metadata when entry details are unavailable."
  @spec from_run_metadata(RunState.t()) :: t()
  def from_run_metadata(%RunState{} = run_state) do
    Enum.reduce(inflight_execution_ids(run_state), new(run_state), fn execution_id, acc ->
      add_entry(acc, %{execution_id: execution_id, runner_execution_id: execution_id})
    end)
  end

  defp release_lease(nil), do: :ok

  defp release_lease(lease) when is_map(lease) do
    case ExecutionAdmission.release(lease) do
      :ok -> :ok
      {:error, _reason} -> :ok
    end
  end

  defp maybe_put(map, _execution_id, nil), do: map
  defp maybe_put(map, execution_id, value), do: Map.put(map, execution_id, value)

  defp execution_id(entry) do
    case Map.get(entry, :execution_id) || Map.get(entry, :runner_execution_id) do
      id when is_binary(id) -> id
      _other -> nil
    end
  end
end
