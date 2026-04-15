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
