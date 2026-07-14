defmodule FavnOrchestrator.RunServer.Snapshots do
  @moduledoc """
  Pure snapshot shaping helpers for run-server terminal paths.

  These helpers refresh timestamps and hashes without advancing the durable
  event sequence; the transition writer owns sequence advancement.
  """

  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage

  @doc "Builds a cancelled terminal snapshot with accumulated runner results."
  @spec cancelled_terminal(RunState.t(), [term()]) :: RunState.t()
  def cancelled_terminal(%RunState{} = run_state, acc_results) do
    snapshot_update(run_state,
      status: :cancelled,
      runner_execution_id: nil,
      error: cancelled_error(run_state),
      result: %{status: :cancelled, asset_results: acc_results, metadata: run_state.metadata}
    )
  end

  @doc "Adds accumulated results to an already-failed run snapshot."
  @spec terminalize_failed_run(RunState.t(), [term()]) :: RunState.t()
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

  @doc "Returns the stored cancellation snapshot, or builds a cancelled terminal snapshot."
  @spec cancelled_snapshot(RunState.t()) :: RunState.t()
  def cancelled_snapshot(%RunState{} = run_state) do
    case Storage.get_run(run_state.id) do
      {:ok, %RunState{status: :cancelled} = cancelled} -> cancelled
      _other -> cancelled_terminal(run_state, [])
    end
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
end
