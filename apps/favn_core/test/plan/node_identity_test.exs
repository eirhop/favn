defmodule Favn.Plan.NodeIdentityTest do
  use ExUnit.Case, async: true

  alias Favn.Plan
  alias Favn.Plan.NodeIdentity

  test "builds identity from manifest version id and plan node" do
    node_key = {{MyApp.Gold, :asset}, nil}

    plan = %Plan{
      target_refs: [{MyApp.Gold, :asset}],
      topo_order: [{MyApp.Raw, :asset}, {MyApp.Gold, :asset}],
      nodes: %{
        node_key => %{
          ref: {MyApp.Gold, :asset},
          node_key: node_key,
          window: nil,
          execution_pool: :warehouse
        }
      }
    }

    assert {:ok, identity} = NodeIdentity.from_plan("manifest-v1", plan, node_key)

    assert identity.manifest_version_id == "manifest-v1"
    assert identity.node_key == node_key
    assert identity.target_refs == [{MyApp.Gold, :asset}]
    assert identity.planned_asset_refs == [{MyApp.Raw, :asset}, {MyApp.Gold, :asset}]
    assert identity.window == nil
    assert identity.execution_pool == :warehouse
  end

  test "returns an explicit error for unknown plan nodes" do
    assert {:error, :plan_node_not_found} =
             NodeIdentity.from_plan("manifest-v1", %Plan{}, {{MyApp.Missing, :asset}, nil})
  end
end
