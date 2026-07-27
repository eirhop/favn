defmodule FavnOrchestrator.RunServer.Snapshots do
  @moduledoc """
  Pure snapshot shaping helpers for run-server terminal paths.

  These helpers refresh timestamps and hashes without advancing the durable
  event sequence; the transition writer owns sequence advancement.
  """

  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.Runs

  @max_terminal_results 128

  @doc "Builds a cancelled terminal snapshot with accumulated runner results."
  @spec cancelled_terminal(RunState.t(), [term()]) :: RunState.t()
  def cancelled_terminal(%RunState{} = run_state, acc_results) do
    snapshot_update(run_state,
      status: :cancelled,
      runner_task_id: nil,
      error: cancelled_error(run_state),
      result: %{
        status: :cancelled,
        asset_results: Enum.take(acc_results, @max_terminal_results),
        metadata: run_state.metadata
      }
    )
  end

  @doc "Adds accumulated results to an already-failed run snapshot."
  @spec terminalize_failed_run(RunState.t(), [term()]) :: RunState.t()
  def terminalize_failed_run(%RunState{} = failed_run, all_results) do
    snapshot_update(failed_run,
      runner_task_id: nil,
      result: %{
        status: failed_run.status,
        asset_results: Enum.take(all_results, @max_terminal_results),
        metadata: failed_run.metadata
      }
    )
  end

  @doc "Returns the stored cancellation snapshot, or builds a cancelled terminal snapshot."
  @spec cancelled_snapshot(RunState.t()) :: RunState.t()
  def cancelled_snapshot(%RunState{workspace_id: workspace_id} = run_state)
      when is_binary(workspace_id) do
    context = SystemContext.workspace(workspace_id, :run_worker)

    case Runs.get(context, run_state.id) do
      {:ok, %RunState{status: :cancelled} = cancelled} -> cancelled
      _other -> cancelled_terminal(run_state, [])
    end
  end

  def cancelled_snapshot(%RunState{} = run_state), do: cancelled_terminal(run_state, [])

  @doc "Removes completed in-flight execution IDs without advancing the durable event sequence."
  @spec clear_inflight_tasks(RunState.t(), [String.t()]) :: RunState.t()
  def clear_inflight_tasks(%RunState{} = run_state, task_ids)
      when is_list(task_ids) do
    removed = Enum.filter(task_ids, &is_binary/1)

    ids =
      run_state.metadata
      |> Map.get(:active_runner_task_ids, [])
      |> normalize_task_ids()
      |> Kernel.--(removed)

    snapshot_update(run_state,
      metadata: Map.put(run_state.metadata, :active_runner_task_ids, ids)
    )
  end

  @doc "Updates a snapshot timestamp and hash without advancing its event sequence."
  @spec snapshot_update(RunState.t(), keyword()) :: RunState.t()
  def snapshot_update(%RunState{} = run_state, attrs) when is_list(attrs) do
    run_state
    |> Map.merge(Enum.into(attrs, %{}))
    |> Map.put(:updated_at, DateTime.utc_now())
    |> RunState.with_snapshot_hash()
  end

  defp cancelled_error(%RunState{error: {:cancelled, _reason} = error}), do: error

  defp cancelled_error(%RunState{metadata: metadata}) when is_map(metadata) do
    reason =
      Map.get(
        metadata,
        :cancel_reason,
        Map.get(metadata, "cancel_reason", :external_cancel_request)
      )

    {:cancelled, reason}
  end

  defp cancelled_error(%RunState{}), do: {:cancelled, %{reason: :external_cancel_request}}

  defp normalize_task_ids(ids) when is_list(ids), do: Enum.filter(ids, &is_binary/1)
  defp normalize_task_ids(_ids), do: []
end
