defmodule Favn.Manifest.GraphTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest.Graph

  test "builds deterministic topo order and edges" do
    assets = [
      %{ref: {MyApp.B, :asset}, depends_on: [{MyApp.A, :asset}]},
      %{ref: {MyApp.A, :asset}, depends_on: []},
      %{ref: {MyApp.C, :asset}, depends_on: [{MyApp.B, :asset}]}
    ]

    assert {:ok, graph} = Graph.build(assets)

    assert graph.nodes == [{MyApp.A, :asset}, {MyApp.B, :asset}, {MyApp.C, :asset}]
    assert graph.topo_order == [{MyApp.A, :asset}, {MyApp.B, :asset}, {MyApp.C, :asset}]

    assert graph.edges == [
             %{from: {MyApp.A, :asset}, to: {MyApp.B, :asset}},
             %{from: {MyApp.B, :asset}, to: {MyApp.C, :asset}}
           ]
  end

  test "orders destinations when many edges share the same source" do
    source_ref = {MyApp.SharedSource, :asset}

    target_refs =
      for index <- 1..32 do
        {MyApp.SharedTargets, String.to_atom("target_#{String.pad_leading("#{index}", 2, "0")}")}
      end

    assets =
      [%{ref: source_ref, depends_on: []}] ++
        Enum.map(Enum.reverse(target_refs), &%{ref: &1, depends_on: [source_ref]})

    assert {:ok, graph} = Graph.build(assets)

    assert graph.edges == Enum.map(target_refs, &%{from: source_ref, to: &1})
  end

  test "fails when a dependency is missing" do
    assets = [%{ref: {MyApp.B, :asset}, depends_on: [{MyApp.A, :asset}]}]

    assert {:error, {:missing_dependency, {MyApp.B, :asset}, {MyApp.A, :asset}}} =
             Graph.build(assets)
  end

  test "fails deterministically when graph has cycle" do
    assets = [
      %{ref: {MyApp.A, :asset}, depends_on: [{MyApp.B, :asset}]},
      %{ref: {MyApp.B, :asset}, depends_on: [{MyApp.A, :asset}]}
    ]

    assert {:error, {:cycle, [{MyApp.A, :asset}, {MyApp.B, :asset}]}} = Graph.build(assets)
  end

  test "returns tagged invalid input errors" do
    assert {:error, {:invalid_assets_input, :invalid}} = Graph.build(:invalid)
  end
end
