defmodule Favn.Manifest.PlanningIndexTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Graph
  alias Favn.Manifest.PlanningIndex

  test "builds manifest-derived adjacency, transitive closures, and topo ranks" do
    assert {:ok, graph} = Graph.build(sample_assets())
    manifest = %Manifest{assets: sample_assets(), graph: graph}

    assert {:ok, index} = PlanningIndex.build(manifest)

    assert index.topo_order == [{MyApp.Raw, :asset}, {MyApp.Stage, :asset}, {MyApp.Gold, :asset}]

    assert index.topo_rank == %{
             {MyApp.Raw, :asset} => 0,
             {MyApp.Stage, :asset} => 1,
             {MyApp.Gold, :asset} => 2
           }

    assert index.upstream[{MyApp.Gold, :asset}] == MapSet.new([{MyApp.Stage, :asset}])
    assert index.downstream[{MyApp.Raw, :asset}] == MapSet.new([{MyApp.Stage, :asset}])

    assert index.transitive_upstream[{MyApp.Gold, :asset}] ==
             MapSet.new([{MyApp.Raw, :asset}, {MyApp.Stage, :asset}])

    assert index.transitive_downstream[{MyApp.Raw, :asset}] ==
             MapSet.new([{MyApp.Stage, :asset}, {MyApp.Gold, :asset}])
  end

  test "fails when manifest graph nodes differ from manifest assets" do
    assert {:ok, graph} = Graph.build(sample_assets())
    graph = %{graph | nodes: [{MyApp.Raw, :asset}]}

    assert {:error, {:manifest_graph_mismatch, :nodes}} =
             PlanningIndex.build(%Manifest{assets: sample_assets(), graph: graph})
  end

  test "fails clearly when non-empty assets have no manifest graph" do
    assert {:error, {:missing_manifest_graph, :non_empty_assets}} =
             PlanningIndex.build(%Manifest{assets: sample_assets()})
  end

  test "fails when manifest graph edges differ from manifest asset dependencies" do
    assert {:ok, graph} = Graph.build(sample_assets())
    graph = %{graph | edges: []}

    assert {:error, {:manifest_graph_mismatch, :edges}} =
             PlanningIndex.build(%Manifest{assets: sample_assets(), graph: graph})
  end

  test "fails deterministically for duplicate asset refs" do
    assets = [%Asset{ref: {MyApp.Raw, :asset}}, %Asset{ref: {MyApp.Raw, :asset}}]
    assert {:ok, graph} = Graph.build(assets)

    assert {:error, {:duplicate_asset_ref, {MyApp.Raw, :asset}}} =
             PlanningIndex.build(%Manifest{assets: assets, graph: graph})
  end

  test "projects selected refs through the manifest graph contract" do
    assert {:ok, graph} = Graph.build(sample_assets())
    assert {:ok, index} = PlanningIndex.build(%Manifest{assets: sample_assets(), graph: graph})

    refs = MapSet.new([{MyApp.Stage, :asset}, {MyApp.Gold, :asset}])

    assert {:ok, projected} = PlanningIndex.project(index, refs)

    assert projected.topo_order == [{MyApp.Stage, :asset}, {MyApp.Gold, :asset}]
    assert projected.upstream[{MyApp.Gold, :asset}] == MapSet.new([{MyApp.Stage, :asset}])
    assert projected.upstream[{MyApp.Stage, :asset}] == MapSet.new()
  end

  test "returns deterministic errors for unknown projection refs" do
    assert {:ok, graph} = Graph.build(sample_assets())
    assert {:ok, index} = PlanningIndex.build(%Manifest{assets: sample_assets(), graph: graph})

    assert {:error, {:unknown_projection_ref, {MyApp.Missing, :asset}}} =
             PlanningIndex.project(index, MapSet.new([{MyApp.Missing, :asset}]))
  end

  defp sample_assets do
    [
      %Asset{
        ref: {MyApp.Gold, :asset},
        module: MyApp.Gold,
        name: :asset,
        depends_on: [{MyApp.Stage, :asset}]
      },
      %Asset{ref: {MyApp.Raw, :asset}, module: MyApp.Raw, name: :asset, depends_on: []},
      %Asset{
        ref: {MyApp.Stage, :asset},
        module: MyApp.Stage,
        name: :asset,
        depends_on: [{MyApp.Raw, :asset}]
      }
    ]
  end
end
