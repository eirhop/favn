defmodule FavnOrchestrator.RunServer.Execution.ResultBuilder do
  @moduledoc """
  Builds deterministic orchestrator-owned results from runner output.

  This boundary translates runner asset results into plan-node results and
  keeps pipeline result ordering aligned with the submitted plan.
  """

  alias Favn.Run.NodeResult
  alias FavnOrchestrator.AssetStepIdentity
  alias FavnOrchestrator.RunServer.Snapshots
  alias FavnOrchestrator.RunState

  @doc """
  Adds one completed execution node to a run snapshot.

  Runner result maps may use atom or string keys. Missing timing and attempt
  fields receive deterministic orchestrator defaults.
  """
  @spec record_execution(RunState.t(), map(), non_neg_integer(), pos_integer(), atom(), list()) ::
          RunState.t()
  def record_execution(
        %RunState{} = run_state,
        entry,
        stage,
        attempt,
        status,
        asset_results
      )
      when is_list(asset_results) do
    result = execution_result(run_state, entry, stage, attempt, status, asset_results)
    append_node_result(run_state, result)
  end

  @doc "Builds one completed execution-node result without changing the run snapshot."
  @spec execution_result(
          RunState.t(),
          map(),
          non_neg_integer(),
          pos_integer(),
          atom(),
          list()
        ) :: NodeResult.t()
  def execution_result(
        %RunState{} = run_state,
        %{
          asset_ref: asset_ref,
          node_key: node_key,
          execution_id: execution_id,
          execution_pool: execution_pool,
          freshness_key: freshness_key
        },
        stage,
        attempt,
        status,
        asset_results
      )
      when is_list(asset_results) do
    node = Map.fetch!(run_state.plan.nodes, node_key)
    asset_result = Enum.find(asset_results, &(asset_result_ref(&1) == asset_ref))
    now = DateTime.utc_now()

    NodeResult.new(%{
      node_key: node_key,
      ref: asset_ref,
      window: node.window,
      stage: stage,
      execution_pool: Map.get(node, :execution_pool) || execution_pool,
      status: status,
      started_at: asset_result_field(asset_result, :started_at) || now,
      finished_at: asset_result_field(asset_result, :finished_at) || now,
      duration_ms: asset_result_field(asset_result, :duration_ms) || 0,
      reason: execution_node_reason(status),
      freshness_key: freshness_key,
      input_versions: [],
      attempt_count: asset_result_field(asset_result, :attempt_count) || attempt,
      max_attempts: asset_result_field(asset_result, :max_attempts) || run_state.max_attempts,
      runner_execution_id: execution_id,
      meta: map_field(asset_result, :meta),
      error: asset_result_field(asset_result, :error),
      attempts: list_field(asset_result, :attempts),
      asset_step_id:
        asset_result_field(asset_result, :asset_step_id) ||
          AssetStepIdentity.asset_step_id(run_state.id, node_key, asset_ref)
    })
  end

  @doc "Adds a node result without discarding existing pipeline result data."
  @spec append_node_result(RunState.t(), NodeResult.t()) :: RunState.t()
  def append_node_result(%RunState{} = run_state, %NodeResult{} = result) do
    result_map =
      Map.merge(run_state.result || %{}, %{node_results: [result | node_results(run_state)]})

    Snapshots.snapshot_update(run_state, result: result_map)
  end

  @doc "Builds the persisted aggregate result for a pipeline run."
  @spec pipeline_result(RunState.t(), RunState.status(), list()) :: map()
  def pipeline_result(%RunState{} = run_state, status, asset_results)
      when is_list(asset_results) do
    %{
      status: status,
      asset_results: asset_results,
      node_results: run_state |> node_results() |> Enum.reverse(),
      metadata: run_state.metadata
    }
  end

  @doc "Returns node results already accumulated on a run."
  @spec node_results(RunState.t()) :: [NodeResult.t() | map()]
  def node_results(%RunState{result: %{node_results: results}}) when is_list(results), do: results
  def node_results(%RunState{}), do: []

  @doc "Sorts asset results by plan order while preserving unknown-result order."
  @spec sort_asset_results(RunState.t(), list()) :: list()
  def sort_asset_results(%RunState{} = run_state, results) when is_list(results) do
    ref_order =
      run_state
      |> planned_asset_refs()
      |> Enum.with_index()
      |> Map.new()

    results
    |> Enum.with_index()
    |> Enum.sort_by(fn {result, index} ->
      {Map.get(ref_order, asset_result_ref(result), map_size(ref_order)), index}
    end)
    |> Enum.map(fn {result, _index} -> result end)
  end

  defp execution_node_reason(:ok), do: nil
  defp execution_node_reason(status), do: status

  defp map_field(result, field) do
    case asset_result_field(result, field) do
      value when is_map(value) -> value
      _value -> %{}
    end
  end

  defp list_field(result, field) do
    case asset_result_field(result, field) do
      value when is_list(value) -> value
      _value -> []
    end
  end

  defp asset_result_ref(result), do: asset_result_field(result, :ref)

  defp asset_result_field(result, field) when is_map(result) and is_atom(field),
    do: Map.get(result, field, Map.get(result, Atom.to_string(field)))

  defp asset_result_field(_result, _field), do: nil

  defp planned_asset_refs(%RunState{plan: %Favn.Plan{topo_order: refs}})
       when is_list(refs) and refs != [],
       do: refs

  defp planned_asset_refs(%RunState{target_refs: refs}) when is_list(refs) and refs != [],
    do: refs

  defp planned_asset_refs(%RunState{asset_ref: ref}) when is_tuple(ref), do: [ref]
  defp planned_asset_refs(%RunState{}), do: []
end
