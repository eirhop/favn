defmodule FavnOrchestrator.RetryPolicyResolverTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest.Asset
  alias Favn.Manifest.Index
  alias Favn.Plan
  alias Favn.Retry.Policy
  alias FavnOrchestrator.RetryPolicyResolver

  @ref {MyApp.RetryAsset, :asset}
  @node_key {@ref, nil}

  test "operator, asset, pipeline, and default precedence is frozen into nodes" do
    pipeline = Policy.new!(max_attempts: 2)
    asset = Policy.new!(max_attempts: 3)
    operator = Policy.new!(max_attempts: 4)

    assert_node_policy(nil, nil, nil, 1, :default)
    assert_node_policy(nil, nil, pipeline, 2, :pipeline)
    assert_node_policy(asset, nil, pipeline, 3, :asset)
    assert_node_policy(asset, operator, pipeline, 4, :operator)
  end

  defp assert_node_policy(asset_policy, operator_policy, pipeline_policy, attempts, source) do
    manifest_asset = %Asset{ref: @ref, retry_policy: asset_policy}
    index = %Index{assets_by_ref: %{@ref => manifest_asset}}

    plan = %Plan{
      target_refs: [@ref],
      target_node_keys: [@node_key],
      nodes: %{
        @node_key => %{
          ref: @ref,
          node_key: @node_key,
          window: nil,
          upstream: [],
          downstream: [],
          stage: 0,
          execution_pool: nil,
          action: :run
        }
      }
    }

    resolved = RetryPolicyResolver.annotate(plan, index, pipeline_policy, operator_policy)
    node = Map.fetch!(resolved.nodes, @node_key)

    assert node.retry_policy.max_attempts == attempts
    assert node.retry_policy_source == source
  end
end
