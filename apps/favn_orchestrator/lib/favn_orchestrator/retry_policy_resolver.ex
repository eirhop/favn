defmodule FavnOrchestrator.RetryPolicyResolver do
  @moduledoc false

  alias Favn.Manifest.Index
  alias Favn.Plan
  alias Favn.Retry.Policy

  @spec annotate(Plan.t(), Index.t(), Policy.t() | nil, Policy.t() | nil) :: Plan.t()
  def annotate(%Plan{} = plan, %Index{} = index, pipeline_policy, operator_policy) do
    nodes =
      Map.new(plan.nodes, fn {node_key, node} ->
        asset_policy =
          case Index.fetch_asset(index, node.ref) do
            {:ok, asset} -> asset.retry_policy
            {:error, _reason} -> nil
          end

        {policy, source} = effective_policy(operator_policy, asset_policy, pipeline_policy)

        {node_key,
         node
         |> Map.put(:retry_policy, policy)
         |> Map.put(:retry_policy_source, source)}
      end)

    %{plan | nodes: nodes}
  end

  defp effective_policy(%Policy{} = policy, _asset, _pipeline), do: {policy, :operator}
  defp effective_policy(nil, %Policy{} = policy, _pipeline), do: {policy, :asset}
  defp effective_policy(nil, nil, %Policy{} = policy), do: {policy, :pipeline}
  defp effective_policy(nil, nil, nil), do: {Policy.default(), :default}
end
