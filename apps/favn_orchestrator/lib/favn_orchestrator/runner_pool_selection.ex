defmodule FavnOrchestrator.RunnerPoolSelection do
  @moduledoc "Resolves exact asset override, pipeline default, and final default-pool selection."

  alias FavnOrchestrator.RunState

  @spec for_node(RunState.t(), Favn.Plan.node_key()) :: atom()
  def for_node(%RunState{} = run, node_key) do
    node_pool =
      case run.plan do
        %Favn.Plan{nodes: nodes} -> get_in(nodes, [node_key, :runner_pool])
        _other -> nil
      end

    pipeline_pool =
      run.metadata
      |> Map.get(:pipeline_context, Map.get(run.metadata, "pipeline_context", %{}))
      |> context_pool()

    node_pool || pipeline_pool || Favn.RunnerPool.default()
  end

  @spec release_for_node!(RunState.t(), Favn.Plan.node_key()) :: String.t()
  def release_for_node!(%RunState{} = run, node_key) do
    pool = for_node(run, node_key)
    {:ok, pool_name} = Favn.RunnerPool.encode(pool)
    Map.fetch!(run.runner_releases, pool_name)
  end

  defp context_pool(context) when is_map(context),
    do: Map.get(context, :runner_pool, Map.get(context, "runner_pool"))

  defp context_pool(_context), do: nil
end
