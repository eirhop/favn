defmodule Favn.Plan.NodeIdentityTest do
  use ExUnit.Case, async: true

  alias Favn.Plan
  alias Favn.Plan.NodeIdentity

  test "validates planned identity shape" do
    assert {:ok, identity} =
             NodeIdentity.new(%{
               manifest_version_id: "mv_1",
               node_key: {{MyApp.Asset, :asset}, nil},
               target_refs: [{MyApp.Asset, :asset}],
               planned_asset_refs: [{MyApp.Dependency, :asset}, {MyApp.Asset, :asset}],
               execution_pool: :default
             })

    assert identity.node_key == {{MyApp.Asset, :asset}, nil}
    assert identity.execution_pool == :default
  end

  test "rejects invalid refs" do
    assert {:error, {:invalid_refs, [:bad]}} =
             NodeIdentity.new(%{
               manifest_version_id: "mv_1",
               node_key: {{MyApp.Asset, :asset}, nil},
               target_refs: [:bad]
             })
  end

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
    assert identity.planned_asset_refs == [{MyApp.Gold, :asset}]
    assert identity.window == nil
    assert identity.execution_pool == :warehouse
  end

  test "keeps each work identity bounded independently of plan width" do
    node_key = {{MyApp.Gold, :asset}, nil}
    wide_refs = List.duplicate({MyApp.Raw, :asset}, 100_000)

    plan = %Plan{
      target_refs: wide_refs,
      topo_order: wide_refs,
      nodes: %{
        node_key => %{
          ref: {MyApp.Gold, :asset},
          node_key: node_key,
          execution_pool: :default
        }
      }
    }

    assert {:ok, identity} = NodeIdentity.from_plan("manifest-v1", plan, node_key)
    assert identity.target_refs == [{MyApp.Gold, :asset}]
    assert identity.planned_asset_refs == [{MyApp.Gold, :asset}]
    assert byte_size(:erlang.term_to_binary(identity)) < 1_000
  end

  test "returns an explicit error for unknown plan nodes" do
    assert {:error, :plan_node_not_found} =
             NodeIdentity.from_plan("manifest-v1", %Plan{}, {{MyApp.Missing, :asset}, nil})
  end
end
