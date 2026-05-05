defmodule FavnOrchestrator.RunRecovery do
  @moduledoc false

  alias FavnOrchestrator.OperationalEvents
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.TransitionWriter

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
    [:pending, :running]
    |> Enum.each(fn status ->
      case Storage.list_runs(status: status) do
        {:ok, runs} ->
          Enum.each(runs, fn run ->
            terminalize_run(run, orphaned_run_error(run))
          end)

        {:error, reason} ->
          OperationalEvents.emit(
            :run_reconciliation_failed,
            %{},
            %{status: status, reason: reason},
            level: :error
          )
      end
    end)
  end

  defp terminalize_run(%RunState{} = run, error) when is_map(error) do
    failed =
      RunState.transition(run,
        status: :error,
        error: error,
        runner_execution_id: nil,
        metadata: Map.put(run.metadata, :terminal_event_type, :run_failed)
      )

    TransitionWriter.persist_transition(failed, :run_failed, %{
      status: failed.status,
      error: failed.error
    })
  end

  defp orphaned_run_error(%RunState{} = run) do
    %{
      type: :orphaned_run_reconciled,
      scope: :local_single_node,
      previous_status: run.status,
      reconciled_at: DateTime.utc_now()
    }
  end
end
