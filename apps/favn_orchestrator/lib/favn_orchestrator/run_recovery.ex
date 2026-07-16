defmodule FavnOrchestrator.RunRecovery do
  @moduledoc false

  alias FavnOrchestrator.OperationalEvents
  alias FavnOrchestrator.Repair.RuntimeState
  alias FavnOrchestrator.RunManager
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :temporary,
      type: :worker
    }
  end

  @spec start_link(keyword()) :: :ignore | {:error, term()}
  def start_link(opts \\ []) when is_list(opts) do
    if Keyword.get(opts, :enabled, true) do
      reconcile_orphaned_runs()
    end

    :ignore
  end

  @spec reconcile_orphaned_runs() :: :ok
  def reconcile_orphaned_runs do
    {runs, failures} = active_runs()

    runs
    |> Enum.each(fn run ->
      if retry_wait?(run), do: recover_retry_wait(run), else: repair_run(run)
    end)

    Enum.each(failures, fn {status, reason} ->
      emit_failure(nil, {:active_run_scan_failed, status, reason})
    end)

    :ok
  end

  defp active_runs do
    [:pending, :running]
    |> Enum.reduce({[], []}, fn status, {runs, failures} ->
      case Storage.list_runs(status: status) do
        {:ok, matching} -> {matching ++ runs, failures}
        {:error, reason} -> {runs, [{status, reason} | failures]}
      end
    end)
  end

  defp retry_wait?(%RunState{metadata: metadata}) when is_map(metadata),
    do: is_map(Map.get(metadata, :retry_state, Map.get(metadata, "retry_state")))

  defp retry_wait?(%RunState{}), do: false

  defp recover_retry_wait(%RunState{} = run) do
    case RunManager.recover_run(run.id) do
      {:ok, _run_id} ->
        :ok

      {:error, reason} ->
        emit_operator_required(run.id, {:retry_wait_recovery_failed, reason})
        emit_failure(run.id, {:retry_wait_recovery_failed, reason})
    end
  end

  defp repair_run(%RunState{} = run) do
    case RuntimeState.repair(dry_run: false, freshness: false, run_id: run.id) do
      {:ok, _report} -> emit_operator_required(run.id, :unsafe_automatic_recovery_not_proven)
      {:error, report} -> emit_failure(run.id, report.errors)
    end
  end

  defp emit_operator_required(run_id, reason) do
    OperationalEvents.emit(
      :recovery_requires_operator,
      %{},
      %{run_id: run_id, reason: reason},
      level: :warning
    )
  end

  defp emit_failure(run_id, errors) do
    OperationalEvents.emit(
      :run_reconciliation_failed,
      %{},
      %{run_id: run_id, errors: errors},
      level: :error
    )
  end
end
