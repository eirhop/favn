defmodule Favn.Plan.NodeIdentityTest do
  use ExUnit.Case, async: true

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
end
