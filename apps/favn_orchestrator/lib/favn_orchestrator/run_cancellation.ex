defmodule FavnOrchestrator.RunCancellation do
  @moduledoc false

  alias FavnOrchestrator.Projector
  alias FavnOrchestrator.RunEvent
  alias FavnOrchestrator.RunState

  @spec request(RunState.t(), map(), DateTime.t()) ::
          {:ok, RunState.t(), RunEvent.t()}
          | {:error, :run_already_terminal | :backfill_parent_cancel_not_supported}
  def request(%RunState{} = run, reason, %DateTime{} = occurred_at) when is_map(reason) do
    cond do
      run.submit_kind in [:backfill_pipeline, :backfill_asset] ->
        {:error, :backfill_parent_cancel_not_supported}

      RunState.finalized?(run) ->
        {:error, :run_already_terminal}

      true ->
        requested =
          RunState.transition(
            run,
            [
              metadata:
                Map.merge(run.metadata, %{
                  cancel_requested: true,
                  cancel_reason: reason,
                  cancel_requested_at: occurred_at
                })
            ],
            occurred_at
          )

        {:ok, requested, Projector.run_event(requested, :run_cancel_requested, %{reason: reason})}
    end
  end

  @spec finish(RunState.t(), map(), DateTime.t()) :: {RunState.t(), RunEvent.t()}
  def finish(%RunState{} = requested, reason, %DateTime{} = occurred_at) when is_map(reason) do
    cancelled =
      RunState.transition(
        requested,
        [
          status: :cancelled,
          error: {:cancelled, reason},
          runner_execution_id: nil,
          metadata:
            Map.merge(requested.metadata, %{
              cancelled: true,
              in_flight_execution_ids: []
            })
        ],
        occurred_at
      )

    {cancelled, Projector.run_event(cancelled, :run_cancelled, %{reason: reason})}
  end
end
