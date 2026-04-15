defmodule FavnOrchestrator.Projector do
  @moduledoc """
  Projects orchestrator run snapshots into persisted event records.
  """

  alias Favn.Run
  alias Favn.Run.AssetResult
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage

  @spec run_event(RunState.t(), atom(), map()) :: map()
  def run_event(%RunState{} = run_state, event_type, data \\ %{}) when is_atom(event_type) do
    %{
      run_id: run_state.id,
      sequence: run_state.event_seq,
      event_type: event_type,
      occurred_at: DateTime.utc_now(),
      status: run_state.status,
      manifest_version_id: run_state.manifest_version_id,
      manifest_content_hash: run_state.manifest_content_hash,
      asset_ref: run_state.asset_ref,
      data: normalize_data(data)
    }
  end

  @spec persist_snapshot(RunState.t()) :: :ok | {:error, term()}
  def persist_snapshot(%RunState{} = run_state), do: Storage.put_run(run_state)

  @spec persist_snapshot_with_event(RunState.t(), atom(), map()) :: :ok | {:error, term()}
  def persist_snapshot_with_event(%RunState{} = run_state, event_type, data \\ %{})
      when is_atom(event_type) do
    with :ok <- persist_snapshot(run_state) do
      Storage.append_run_event(run_state.id, run_event(run_state, event_type, data))
    end
  end

  @spec project_run(RunState.t()) :: Run.t()
  def project_run(%RunState{} = run_state) do
    terminal? = run_state.status in [:ok, :error, :cancelled, :timed_out]
    asset_results = project_asset_results(run_state)

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
      max_concurrency: project_max_concurrency(run_state.plan),
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
      node_results: project_node_results(asset_results),
      error: run_state.error,
      terminal_reason: project_terminal_reason(run_state)
    }
  end

  @spec project_runs([RunState.t()]) :: [Run.t()]
  def project_runs(runs) when is_list(runs), do: Enum.map(runs, &project_run/1)

  defp normalize_data(data) when is_map(data), do: data
  defp normalize_data(_data), do: %{}

  defp project_asset_results(%RunState{result: %{asset_results: results}})
       when is_list(results) do
    results
    |> Enum.reduce(%{}, fn result, acc ->
      case asset_result_ref(result) do
        {:ok, ref} -> Map.put(acc, ref, normalize_asset_result(result))
        :error -> acc
      end
    end)
  end

  defp project_asset_results(_run_state), do: %{}

  defp project_node_results(asset_results) when is_map(asset_results) do
    Enum.into(asset_results, %{}, fn {ref, %AssetResult{} = result} -> {{ref, nil}, result} end)
  end

  defp project_pipeline(%RunState{submit_kind: :pipeline} = run_state) do
    %{
      target_refs: run_state.target_refs,
      submit_ref: Map.get(run_state.metadata, :pipeline_submit_ref),
      context: Map.get(run_state.metadata, :pipeline_context)
    }
  end

  defp project_pipeline(_run_state), do: nil

  defp project_submit_kind(:manual), do: :asset
  defp project_submit_kind(:pipeline), do: :pipeline
  defp project_submit_kind(:rerun), do: :rerun
  defp project_submit_kind(_other), do: :asset

  defp project_max_concurrency(%Favn.Plan{node_stages: node_stages}) when is_list(node_stages) do
    node_stages
    |> Enum.map(&length/1)
    |> Enum.max(fn -> 1 end)
  end

  defp project_max_concurrency(_plan), do: 1

  defp project_replay_mode(%RunState{submit_kind: :rerun}), do: :exact_replay
  defp project_replay_mode(_run_state), do: :none

  defp project_operator_reason(%RunState{status: :cancelled, error: {:cancelled, reason}}),
    do: reason

  defp project_operator_reason(_run_state), do: nil

  defp project_terminal_reason(%RunState{status: status, error: error})
       when status in [:ok, :error, :cancelled, :timed_out] do
    %{status: status, error: error}
  end

  defp project_terminal_reason(_run_state), do: nil

  defp asset_result_ref(%AssetResult{ref: ref}) when is_tuple(ref), do: {:ok, ref}
  defp asset_result_ref(%{ref: ref}) when is_tuple(ref), do: {:ok, ref}
  defp asset_result_ref(_result), do: :error

  defp normalize_asset_result(%AssetResult{} = result), do: result

  defp normalize_asset_result(%{ref: ref} = result) when is_tuple(ref) do
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
      attempts: if(is_list(attempts), do: attempts, else: [])
    }
  end

  defp normalize_asset_result(_result) do
    %AssetResult{
      ref: {:unknown, :asset},
      stage: 0,
      status: :error,
      started_at: DateTime.utc_now(),
      finished_at: DateTime.utc_now(),
      duration_ms: 0,
      meta: %{},
      error: :invalid_asset_result,
      attempt_count: 1,
      max_attempts: 1,
      attempts: []
    }
  end
end
