defmodule Favn.Runtime.Projector do
  @moduledoc """
  Projects internal runtime state into the public `%Favn.Run{}` model.
  """

  alias Favn.Run
  alias Favn.Run.AssetResult
  alias Favn.Runtime.State

  @spec to_public_run(State.t()) :: Run.t()
  def to_public_run(%State{} = state) do
    target_outputs = Map.take(state.outputs, state.target_refs)

    %Run{
      id: state.run_id,
      target_refs: state.target_refs,
      plan: state.plan,
      status: public_status(state.run_status),
      event_seq: state.event_seq,
      started_at: state.started_at,
      finished_at: state.finished_at,
      params: state.params,
      retry_policy: state.retry_policy,
      outputs: state.outputs,
      target_outputs: target_outputs,
      asset_results: build_asset_results(state),
      error: state.run_error,
      terminal_reason: state.run_terminal_reason
    }
  end

  defp public_status(:pending), do: :running
  defp public_status(status) when status in [:running, :cancelling, :timing_out], do: :running
  defp public_status(:success), do: :ok
  defp public_status(:cancelled), do: :cancelled
  defp public_status(:timed_out), do: :timed_out
  defp public_status(_status), do: :error

  defp build_asset_results(%State{} = state) do
    Enum.reduce(state.steps, %{}, fn {ref, step}, acc ->
      if include_asset_result?(step) do
        result = %AssetResult{
          ref: ref,
          stage: step.stage,
          status: public_step_status(step.status),
          started_at: step.started_at,
          finished_at: step.finished_at,
          duration_ms: step.duration_ms || 0,
          output: step.output,
          meta: step.meta,
          error: step.error,
          attempt_count: step.attempt,
          max_attempts: step.max_attempts,
          attempts: step.attempts,
          next_retry_at: step.next_retry_at
        }

        Map.put(acc, ref, result)
      else
        acc
      end
    end)
  end

  defp public_step_status(:success), do: :ok
  defp public_step_status(:failed), do: :error
  defp public_step_status(:ready), do: :running
  defp public_step_status(:pending), do: :running
  defp public_step_status(status), do: status

  defp include_asset_result?(step) do
    step.status in [:retrying, :success, :failed, :cancelled, :timed_out] and
      (step.started_at != nil or step.status == :retrying or step.attempt > 0)
  end
end
