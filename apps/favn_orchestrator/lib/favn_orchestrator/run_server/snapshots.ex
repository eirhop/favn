defmodule FavnOrchestrator.RunServer.Snapshots do
  @moduledoc false

  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage

  def cancelled_terminal(%RunState{} = run_state, acc_results) do
    snapshot_update(run_state,
      status: :cancelled,
      runner_execution_id: nil,
      result: %{status: :cancelled, asset_results: acc_results, metadata: run_state.metadata}
    )
  end

  def terminalize_failed_run(%RunState{} = failed_run, all_results) do
    snapshot_update(failed_run,
      runner_execution_id: nil,
      result: %{
        status: failed_run.status,
        asset_results: all_results,
        metadata: failed_run.metadata
      }
    )
  end

  def cancelled_state(%RunState{} = run_state) do
    case Storage.get_run(run_state.id) do
      {:ok, %RunState{status: :cancelled} = cancelled} ->
        {:error, cancelled, []}

      _other ->
        cancelled =
          RunState.transition(run_state,
            status: :cancelled,
            runner_execution_id: nil,
            error: {:cancelled, %{reason: :external_cancel_request}}
          )

        {:error, cancelled, []}
    end
  end

  def cancelled_snapshot(%RunState{} = run_state) do
    case Storage.get_run(run_state.id) do
      {:ok, %RunState{status: :cancelled} = cancelled} -> cancelled
      _other -> cancelled_terminal(run_state, [])
    end
  end

  def snapshot_update(%RunState{} = run_state, attrs) when is_list(attrs) do
    run_state
    |> Map.merge(Enum.into(attrs, %{}))
    |> Map.put(:updated_at, DateTime.utc_now())
    |> RunState.with_snapshot_hash()
  end
end
