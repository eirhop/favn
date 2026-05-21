defmodule FavnOrchestrator.RunRecovery do
  @moduledoc false

  alias FavnOrchestrator.OperationalEvents
  alias FavnOrchestrator.Repair.RuntimeState

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
    case RuntimeState.repair(dry_run: false, freshness: false) do
      {:ok, _report} ->
        :ok

      {:error, report} ->
        OperationalEvents.emit(
          :run_reconciliation_failed,
          %{},
          %{errors: report.errors},
          level: :error
        )

        :ok
    end
  end
end
