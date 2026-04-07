defmodule Favn.Runtime.Projector do
  @moduledoc """
  Projects internal runtime state into the public `%Favn.Run{}` model.
  """

  alias Favn.Run
  alias Favn.Run.AssetResult
  alias Favn.Runtime.State

  @spec to_public_run(State.t()) :: Run.t()
  def to_public_run(%State{} = state) do
    %Run{
      id: state.run_id,
      target_refs: state.target_refs,
      plan: state.plan,
      pipeline: public_pipeline(state.pipeline_context),
      pipeline_context: state.pipeline_context,
      submit_kind: state.submit_kind,
      submit_ref: state.submit_ref,
      backfill: state.backfill,
      max_concurrency: state.max_concurrency,
      timeout_ms: state.timeout_ms,
      status: public_status(state.run_status),
      event_seq: state.event_seq,
      started_at: state.started_at,
      finished_at: state.finished_at,
      params: state.params,
      retry_policy: state.retry_policy,
      replay_mode: state.replay_mode,
      rerun_of_run_id: state.rerun_of_run_id,
      parent_run_id: state.parent_run_id,
      root_run_id: state.root_run_id || state.run_id,
      lineage_depth: state.lineage_depth,
      operator_reason: state.operator_reason,
      asset_results: build_asset_results(state),
      node_results: build_node_results(state),
      error: state.run_error,
      terminal_reason: state.run_terminal_reason
    }
  end

  defp public_pipeline(nil), do: nil

  defp public_pipeline(pipeline_context) when is_map(pipeline_context) do
    %{
      id: Map.get(pipeline_context, :id),
      name: Map.get(pipeline_context, :name),
      trigger: Map.get(pipeline_context, :trigger),
      schedule: Map.get(pipeline_context, :schedule),
      window: Map.get(pipeline_context, :window),
      anchor_window: Map.get(pipeline_context, :anchor_window),
      backfill_range: Map.get(pipeline_context, :backfill_range),
      anchor_ranges: Map.get(pipeline_context, :anchor_ranges, []),
      source: Map.get(pipeline_context, :source),
      outputs: Map.get(pipeline_context, :outputs, [])
    }
  end

  defp public_status(:pending), do: :running
  defp public_status(status) when status in [:running, :cancelling, :timing_out], do: :running
  defp public_status(:success), do: :ok
  defp public_status(:cancelled), do: :cancelled
  defp public_status(:timed_out), do: :timed_out
  defp public_status(_status), do: :error

  defp build_asset_results(%State{} = state) do
    state.steps
    |> Map.values()
    |> Enum.filter(&include_asset_result?/1)
    |> Enum.group_by(& &1.ref)
    |> Enum.reduce(%{}, fn {ref, steps}, acc ->
      canonical = Enum.max_by(steps, &step_sort_key/1)
      Map.put(acc, ref, step_to_asset_result(canonical))
    end)
  end

  defp build_node_results(%State{} = state) do
    state.steps
    |> Enum.reduce(%{}, fn {node_key, step}, acc ->
      if include_asset_result?(step) do
        Map.put(acc, node_key, step_to_asset_result(step))
      else
        acc
      end
    end)
  end

  defp step_to_asset_result(step) do
    %AssetResult{
      ref: step.ref,
      stage: step.stage,
      status: public_step_status(step.status),
      started_at: step.started_at,
      finished_at: step.finished_at,
      duration_ms: step.duration_ms || 0,
      meta: step.meta,
      error: step.error,
      attempt_count: step.attempt,
      max_attempts: step.max_attempts,
      attempts: step.attempts,
      next_retry_at: step.next_retry_at
    }
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

  defp step_sort_key(step) do
    {
      status_rank(step.status),
      datetime_to_micros(step.finished_at),
      datetime_to_micros(step.started_at),
      step.attempt,
      inspect(step.node_key)
    }
  end

  defp status_rank(:failed), do: 5
  defp status_rank(:timed_out), do: 4
  defp status_rank(:cancelled), do: 3
  defp status_rank(:retrying), do: 2
  defp status_rank(:success), do: 1
  defp status_rank(_), do: 0

  defp datetime_to_micros(nil), do: -1
  defp datetime_to_micros(%DateTime{} = datetime), do: DateTime.to_unix(datetime, :microsecond)
end
