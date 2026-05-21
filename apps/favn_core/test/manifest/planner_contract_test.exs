defmodule Favn.Manifest.PlannerContractTest do
  use ExUnit.Case, async: true

  alias Favn.Assets.Planner
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Graph
  alias Favn.Manifest.PlanningIndex

  test "planner can plan solely from a manifest planning index" do
    assets = [
      %Asset{ref: {MyApp.Raw, :asset}, module: MyApp.Raw, name: :asset, depends_on: []},
      %Asset{
        ref: {MyApp.Gold, :asset},
        module: MyApp.Gold,
        name: :asset,
        depends_on: [{MyApp.Raw, :asset}]
      }
    ]

    assert {:ok, graph} = Graph.build(assets)
    assert {:ok, index} = PlanningIndex.build(%Manifest{assets: assets, graph: graph})

    assert {:ok, plan} =
             Planner.plan({MyApp.Gold, :asset}, planning_index: index, dependencies: :all)

    assert plan.topo_order == [{MyApp.Raw, :asset}, {MyApp.Gold, :asset}]
    assert plan.stages == [[{MyApp.Raw, :asset}], [{MyApp.Gold, :asset}]]
  end

  test "manifest planning does not load authoring modules" do
    module = Module.concat([MyApp, "ManifestOnly#{System.unique_integer([:positive])}"])
    ref = {module, :asset}
    assets = [%Asset{ref: ref, module: module, name: :asset, depends_on: []}]

    refute Code.ensure_loaded?(module)

    assert {:ok, graph} = Graph.build(assets)
    assert {:ok, index} = PlanningIndex.build(%Manifest{assets: assets, graph: graph})
    assert {:ok, plan} = Planner.plan(ref, planning_index: index)

    assert plan.topo_order == [ref]
    refute Code.ensure_loaded?(module)
  end
end
