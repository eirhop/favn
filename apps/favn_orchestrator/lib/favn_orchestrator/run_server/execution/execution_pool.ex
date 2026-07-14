defmodule FavnOrchestrator.RunServer.Execution.ExecutionPool do
  @moduledoc """
  Resolves the execution pool for one planned node.

  A node-level pool overrides the pipeline default. Both atom- and string-keyed
  persisted metadata are accepted at this internal boundary.
  """

  alias FavnOrchestrator.RunState

  @type pool :: atom() | String.t() | nil

  @doc "Returns the node pool, pipeline default, or `nil`."
  @spec for_node(RunState.t(), Favn.Plan.node_key()) :: pool()
  def for_node(%RunState{} = run_state, node_key) do
    node_pool(run_state.plan, node_key) || pipeline_default(run_state.metadata)
  end

  defp node_pool(%Favn.Plan{nodes: nodes}, node_key) when is_map(nodes) do
    case Map.get(nodes, node_key) do
      node when is_map(node) -> Map.get(node, :execution_pool) || Map.get(node, "execution_pool")
      _node -> nil
    end
  end

  defp node_pool(_plan, _node_key), do: nil

  defp pipeline_default(metadata) when is_map(metadata) do
    case Map.get(metadata, :pipeline_execution_policy) ||
           Map.get(metadata, "pipeline_execution_policy") do
      policy when is_map(policy) ->
        Map.get(policy, :execution_pool) || Map.get(policy, "execution_pool")

      _policy ->
        nil
    end
  end

  defp pipeline_default(_metadata), do: nil
end
