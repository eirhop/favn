defmodule FavnOrchestrator.Projector do
  @moduledoc """
  Projects orchestrator run snapshots into persisted event records.
  """

  alias Favn.Run
  alias Favn.Run.AssetResult
  alias Favn.Run.NodeResult
  alias FavnOrchestrator.AssetStepIdentity
  alias FavnOrchestrator.RunEvent
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage

  @spec run_event(RunState.t(), atom(), map()) :: RunEvent.t()
  def run_event(%RunState{} = run_state, event_type, data \\ %{}) when is_atom(event_type) do
    normalized_data = data |> normalize_data() |> put_event_asset_step_id(run_state, event_type)

    RunEvent.from_map(%{
      run_id: run_state.id,
      sequence: run_state.event_seq,
      event_type: event_type,
      entity: event_entity(event_type),
      occurred_at: run_state.updated_at || DateTime.utc_now(),
      status: run_state.status,
      manifest_version_id: run_state.manifest_version_id,
      manifest_content_hash: run_state.manifest_content_hash,
      asset_ref: event_asset_ref(run_state, event_type, normalized_data),
      stage: event_stage(event_type, normalized_data),
      data: normalized_data
    })
  end

  @spec persist_snapshot(RunState.t()) :: :ok | {:error, term()}
  def persist_snapshot(%RunState{} = run_state), do: Storage.put_run(run_state)

  @spec project_run(RunState.t()) :: Run.t()
  def project_run(%RunState{} = run_state) do
    terminal? = run_state.status in [:ok, :partial, :error, :cancelled, :timed_out]
    asset_results = project_asset_results(run_state)
    node_results = project_node_results(run_state, asset_results)

    %Run{
      id: run_state.id,
      manifest_version_id: run_state.manifest_version_id,
      manifest_content_hash: run_state.manifest_content_hash,
      asset_ref: run_state.asset_ref,
      target_refs: run_state.target_refs,
      plan: run_state.plan,
      pipeline: project_pipeline(run_state),
      pipeline_context: Map.get(run_state.metadata, :pipeline_context),
      submit_kind: project_submit_kind(run_state.submit_kind),
      submit_ref: Map.get(run_state.metadata, :pipeline_submit_ref, run_state.asset_ref),
      max_concurrency: project_max_concurrency(run_state),
      timeout_ms: run_state.timeout_ms,
      retry_backoff_ms: run_state.retry_backoff_ms,
      status: run_state.status,
      event_seq: run_state.event_seq,
      started_at: run_state.inserted_at,
      finished_at: if(terminal?, do: run_state.updated_at, else: nil),
      params: run_state.params,
      trigger: run_state.trigger,
      metadata: run_state.metadata,
      result: run_state.result,
      runner_execution_id: run_state.runner_execution_id,
      retry_policy: %{
        max_attempts: run_state.max_attempts,
        retry_backoff_ms: run_state.retry_backoff_ms
      },
      replay_mode: project_replay_mode(run_state),
      backfill: nil,
      rerun_of_run_id: run_state.rerun_of_run_id,
      parent_run_id: run_state.parent_run_id,
      root_run_id: run_state.root_run_id,
      lineage_depth: run_state.lineage_depth,
      operator_reason: project_operator_reason(run_state),
      asset_results: asset_results,
      node_results: node_results,
      error: run_state.error,
      terminal_reason: project_terminal_reason(run_state)
    }
  end

  @spec project_runs([RunState.t()]) :: [Run.t()]
  def project_runs(runs) when is_list(runs), do: Enum.map(runs, &project_run/1)

  defp normalize_data(data) when is_map(data), do: data
  defp normalize_data(_data), do: %{}

  defp event_asset_ref(%RunState{} = run_state, event_type, data)
       when is_atom(event_type) and is_map(data) do
    if step_event_type?(event_type) do
      case Map.get(data, :asset_ref) do
        {module, name} = ref when is_atom(module) and is_atom(name) -> ref
        _ -> run_state.asset_ref
      end
    else
      nil
    end
  end

  defp event_stage(event_type, data) when is_atom(event_type) and is_map(data) do
    if step_event_type?(event_type) do
      case Map.get(data, :stage) do
        stage when is_integer(stage) and stage >= 0 -> stage
        _ -> nil
      end
    else
      nil
    end
  end

  defp event_entity(event_type) when is_atom(event_type) do
    if step_event_type?(event_type), do: :step, else: :run
  end

  defp step_event_type?(event_type) when is_atom(event_type) do
    String.starts_with?(Atom.to_string(event_type), "step_")
  end

  defp project_asset_results(%RunState{result: %{asset_results: results}} = run_state)
       when is_list(results) do
    results
    |> Enum.reduce(%{}, fn result, acc ->
      case asset_result_ref(result) do
        {:ok, ref} -> Map.put(acc, ref, normalize_asset_result(run_state, result, ref))
        :error -> acc
      end
    end)
  end

  defp project_asset_results(_run_state), do: %{}

  defp project_node_results(
         %RunState{result: %{node_results: results}} = run_state,
         _asset_results
       )
       when is_list(results) do
    results
    |> Enum.reduce(%{}, fn result, acc ->
      case node_result_key(result) do
        {:ok, node_key} ->
          Map.put(acc, node_key, normalize_node_result(run_state, result, node_key))

        :error ->
          acc
      end
    end)
  end

  defp project_node_results(%RunState{} = run_state, asset_results) when is_map(asset_results) do
    Enum.into(asset_results, %{}, fn {ref, %AssetResult{} = result} ->
      node_key = {ref, nil}
      {node_key, put_asset_step_id(result, run_state, node_key, ref)}
    end)
  end

  defp put_event_asset_step_id(data, %RunState{} = run_state, event_type)
       when is_map(data) and is_atom(event_type) do
    if step_event_type?(event_type) and not present_binary?(Map.get(data, :asset_step_id)) do
      asset_ref = Map.get(data, :asset_ref) || run_state.asset_ref
      node_key = Map.get(data, :node_key)

      Map.put(
        data,
        :asset_step_id,
        AssetStepIdentity.asset_step_id(run_state.id, node_key, asset_ref)
      )
    else
      data
    end
  end

  defp put_event_asset_step_id(data, _run_state, _event_type), do: data

  defp present_binary?(value), do: is_binary(value) and value != ""

  defp node_result_key(%NodeResult{node_key: node_key}) when is_tuple(node_key),
    do: {:ok, node_key}

  defp node_result_key(%{node_key: node_key, ref: ref}) when is_tuple(node_key) and is_tuple(ref),
    do: {:ok, node_key}

  defp node_result_key(_result), do: :error

  defp project_pipeline(%RunState{} = run_state) do
    metadata = run_state.metadata
    submit_ref = Map.get(metadata, :pipeline_submit_ref)

    case Map.get(metadata, :pipeline_context) do
      context when is_map(context) ->
        context
        |> public_pipeline()
        |> maybe_put_submit_ref(submit_ref)

      _other ->
        if pipeline_origin?(run_state) do
          %{
            resolved_refs: run_state.target_refs,
            deps: Map.get(metadata, :pipeline_dependencies),
            trigger: run_state.trigger
          }
          |> maybe_put_submit_ref(submit_ref)
        else
          nil
        end
    end
  end

  defp project_submit_kind(:manual), do: :asset
  defp project_submit_kind(:pipeline), do: :pipeline
  defp project_submit_kind(:backfill_asset), do: :backfill_asset
  defp project_submit_kind(:backfill_pipeline), do: :backfill_pipeline
  defp project_submit_kind(:rerun), do: :rerun
  defp project_submit_kind(_other), do: :asset

  defp project_max_concurrency(%RunState{metadata: %{pipeline_execution_policy: policy}})
       when is_map(policy) do
    Map.get(policy, :max_concurrency) || Map.get(policy, "max_concurrency")
  end

  defp project_max_concurrency(%RunState{plan: %Favn.Plan{node_stages: node_stages}})
       when is_list(node_stages) do
    node_stages
    |> Enum.map(&length/1)
    |> Enum.max(fn -> 1 end)
  end

  defp project_max_concurrency(_run_state), do: 1

  defp project_replay_mode(%RunState{submit_kind: :rerun, metadata: metadata})
       when is_map(metadata) do
    case Map.get(metadata, :replay_mode) do
      :resume_from_failure -> :resume_from_failure
      :exact_replay -> :exact_replay
      _other -> :exact_replay
    end
  end

  defp project_replay_mode(_run_state), do: :none

  defp project_operator_reason(%RunState{status: :cancelled, error: {:cancelled, reason}}),
    do: reason

  defp project_operator_reason(_run_state), do: nil

  defp project_terminal_reason(%RunState{status: status, error: error})
       when status in [:ok, :partial, :error, :cancelled, :timed_out] do
    %{status: status, error: error}
  end

  defp project_terminal_reason(_run_state), do: nil

  defp asset_result_ref(%AssetResult{ref: ref}) when is_tuple(ref), do: {:ok, ref}
  defp asset_result_ref(%{ref: ref}) when is_tuple(ref), do: {:ok, ref}
  defp asset_result_ref(_result), do: :error

  defp normalize_asset_result(run_state, %AssetResult{} = result, ref) do
    put_asset_step_id(result, run_state, {ref, nil}, ref)
  end

  defp normalize_asset_result(run_state, %{ref: ref} = result, _ref) when is_tuple(ref) do
    started_at = Map.get(result, :started_at, DateTime.utc_now())
    finished_at = Map.get(result, :finished_at, started_at)
    status = Map.get(result, :status, :ok)
    meta = Map.get(result, :meta, %{})
    error = Map.get(result, :error)
    duration_ms = Map.get(result, :duration_ms, 0)
    attempt_count = Map.get(result, :attempt_count, 1)
    max_attempts = Map.get(result, :max_attempts, attempt_count)

    attempts =
      Map.get(result, :attempts, [
        %{attempt: attempt_count, status: status, meta: meta, error: error}
      ])

    %AssetResult{
      ref: ref,
      stage: Map.get(result, :stage, 0),
      status: status,
      started_at: started_at,
      finished_at: finished_at,
      duration_ms: duration_ms,
      meta: if(is_map(meta), do: meta, else: %{}),
      error: error,
      attempt_count: attempt_count,
      max_attempts: max_attempts,
      attempts: if(is_list(attempts), do: attempts, else: []),
      asset_step_id:
        Map.get(result, :asset_step_id) ||
          AssetStepIdentity.asset_step_id(run_state.id, {ref, nil}, ref)
    }
  end

  defp normalize_node_result(run_state, %NodeResult{ref: ref} = result, node_key) do
    put_node_asset_step_id(result, run_state, node_key, ref)
  end

  defp normalize_node_result(run_state, %{node_key: node_key, ref: ref} = result, _node_key)
       when is_tuple(node_key) and is_tuple(ref) do
    %NodeResult{
      node_key: node_key,
      ref: ref,
      window: Map.get(result, :window),
      stage: Map.get(result, :stage, 0),
      status: Map.get(result, :status, :running),
      started_at: Map.get(result, :started_at),
      finished_at: Map.get(result, :finished_at),
      duration_ms: Map.get(result, :duration_ms),
      reason: Map.get(result, :reason),
      freshness_key: Map.get(result, :freshness_key),
      input_versions: Map.get(result, :input_versions, %{}),
      attempt_count: Map.get(result, :attempt_count, 0),
      max_attempts: Map.get(result, :max_attempts, 1),
      runner_execution_id: Map.get(result, :runner_execution_id),
      meta: Map.get(result, :meta, %{}),
      error: Map.get(result, :error),
      attempts: Map.get(result, :attempts, []),
      asset_step_id:
        Map.get(result, :asset_step_id) ||
          AssetStepIdentity.asset_step_id(run_state.id, node_key, ref)
    }
  end

  defp put_asset_step_id(
         %AssetResult{asset_step_id: asset_step_id} = result,
         _run_state,
         _node_key,
         _ref
       )
       when is_binary(asset_step_id) and asset_step_id != "",
       do: result

  defp put_asset_step_id(%AssetResult{} = result, run_state, node_key, ref) do
    %{result | asset_step_id: AssetStepIdentity.asset_step_id(run_state.id, node_key, ref)}
  end

  defp put_node_asset_step_id(
         %NodeResult{asset_step_id: asset_step_id} = result,
         _run_state,
         _node_key,
         _ref
       )
       when is_binary(asset_step_id) and asset_step_id != "",
       do: result

  defp put_node_asset_step_id(%NodeResult{} = result, run_state, node_key, ref) do
    %{result | asset_step_id: AssetStepIdentity.asset_step_id(run_state.id, node_key, ref)}
  end

  defp public_pipeline(pipeline_context) when is_map(pipeline_context) do
    %{
      id: Map.get(pipeline_context, :id),
      name: Map.get(pipeline_context, :name),
      run_kind: Map.get(pipeline_context, :run_kind),
      resolved_refs: Map.get(pipeline_context, :resolved_refs, []),
      deps: Map.get(pipeline_context, :deps),
      trigger: Map.get(pipeline_context, :trigger),
      schedule: Map.get(pipeline_context, :schedule),
      window: Map.get(pipeline_context, :window),
      max_concurrency: Map.get(pipeline_context, :max_concurrency),
      execution_pool: Map.get(pipeline_context, :execution_pool),
      anchor_window: Map.get(pipeline_context, :anchor_window),
      backfill_range: Map.get(pipeline_context, :backfill_range),
      anchor_ranges: Map.get(pipeline_context, :anchor_ranges, []),
      source: Map.get(pipeline_context, :source),
      outputs: Map.get(pipeline_context, :outputs, [])
    }
  end

  defp maybe_put_submit_ref(pipeline, nil), do: pipeline
  defp maybe_put_submit_ref(pipeline, submit_ref), do: Map.put(pipeline, :submit_ref, submit_ref)

  defp pipeline_origin?(%RunState{submit_kind: :pipeline}), do: true
  defp pipeline_origin?(%RunState{submit_kind: :backfill_pipeline}), do: true

  defp pipeline_origin?(%RunState{metadata: metadata}) when is_map(metadata) do
    Map.get(metadata, :replay_submit_kind) == :pipeline or
      is_map(Map.get(metadata, :pipeline_context)) or
      present_atom?(Map.get(metadata, :pipeline_submit_ref)) or
      (is_list(Map.get(metadata, :pipeline_target_refs)) and
         Map.get(metadata, :pipeline_target_refs) != [])
  end

  defp present_atom?(value) when is_atom(value) and not is_nil(value), do: true
  defp present_atom?(_value), do: false
end
