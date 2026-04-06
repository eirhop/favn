defmodule Favn.Assets.GraphIndexTest do
  use ExUnit.Case

  alias Favn.Test.Fixtures.Assets.Graph.ReportingAssets
  alias Favn.Test.Fixtures.Assets.Graph.SourceAssets
  alias Favn.Test.Fixtures.Assets.Graph.WarehouseAssets

  setup do
    state = Favn.TestSetup.capture_state()

    on_exit(fn ->
      Favn.TestSetup.restore_state(state, reload_graph?: true)
    end)

    :ok
  end

  test "builds a global DAG index with upstream, downstream, transitive closures, and topological order" do
    :ok =
      Favn.TestSetup.setup_asset_modules([SourceAssets, WarehouseAssets, ReportingAssets],
        reload_graph?: true
      )

    assert {:ok, upstream} = Favn.Assets.GraphIndex.upstream_of({WarehouseAssets, :fact_sales})

    assert upstream == [
             {WarehouseAssets, :normalize_customers},
             {WarehouseAssets, :normalize_orders}
           ]

    assert {:ok, downstream} =
             Favn.Assets.GraphIndex.downstream_of({WarehouseAssets, :normalize_orders})

    assert downstream == [
             {WarehouseAssets, :fact_sales},
             {ReportingAssets, :dashboard}
           ]

    assert {:ok, transitive_upstream} =
             Favn.Assets.GraphIndex.transitive_upstream_of({ReportingAssets, :dashboard})

    assert transitive_upstream == [
             {SourceAssets, :raw_customers},
             {SourceAssets, :raw_orders},
             {WarehouseAssets, :normalize_customers},
             {WarehouseAssets, :normalize_orders},
             {WarehouseAssets, :fact_sales}
           ]

    assert {:ok, transitive_downstream} =
             Favn.Assets.GraphIndex.transitive_downstream_of({SourceAssets, :raw_orders})

    assert transitive_downstream == [
             {WarehouseAssets, :normalize_orders},
             {WarehouseAssets, :fact_sales},
             {ReportingAssets, :dashboard}
           ]

    assert {:ok, topo_order} = Favn.Assets.GraphIndex.topological_order()

    assert Enum.find_index(topo_order, &(&1 == {SourceAssets, :raw_orders})) <
             Enum.find_index(topo_order, &(&1 == {WarehouseAssets, :normalize_orders}))

    assert Enum.find_index(topo_order, &(&1 == {WarehouseAssets, :fact_sales})) <
             Enum.find_index(topo_order, &(&1 == {ReportingAssets, :dashboard}))
  end

  test "selects related assets with filters and direct traversal options" do
    :ok =
      Favn.TestSetup.setup_asset_modules([SourceAssets, WarehouseAssets, ReportingAssets],
        reload_graph?: true
      )

    assert {:ok, assets} =
             Favn.Assets.GraphIndex.related_assets({ReportingAssets, :dashboard},
               direction: :upstream,
               tags: [:warehouse]
             )

    assert Enum.map(assets, & &1.ref) == [
             {WarehouseAssets, :normalize_customers},
             {WarehouseAssets, :normalize_orders}
           ]

    assert {:ok, both_neighbors} =
             Favn.Assets.GraphIndex.related_assets({WarehouseAssets, :normalize_orders},
               direction: :both,
               transitive: false,
               include_target: false
             )

    assert Enum.map(both_neighbors, & &1.ref) == [
             {SourceAssets, :raw_orders},
             {WarehouseAssets, :fact_sales},
             {ReportingAssets, :dashboard}
           ]
  end

  test "builds filtered subgraphs rooted at a target reference" do
    :ok =
      Favn.TestSetup.setup_asset_modules([SourceAssets, WarehouseAssets, ReportingAssets],
        reload_graph?: true
      )

    assert {:ok, subgraph} =
             Favn.Assets.GraphIndex.subgraph({ReportingAssets, :dashboard},
               direction: :upstream,
               tags: [:warehouse]
             )

    assert Map.keys(subgraph.assets_by_ref) |> Enum.sort() == [
             {ReportingAssets, :dashboard},
             {WarehouseAssets, :normalize_customers},
             {WarehouseAssets, :normalize_orders}
           ]

    assert Map.fetch!(subgraph.upstream, {ReportingAssets, :dashboard}) ==
             MapSet.new([{WarehouseAssets, :normalize_orders}])

    assert Map.fetch!(subgraph.downstream, {WarehouseAssets, :normalize_orders}) ==
             MapSet.new([{ReportingAssets, :dashboard}])
  end

  test "returns invalid_opts for invalid graph query options" do
    :ok =
      Favn.TestSetup.setup_asset_modules([SourceAssets, WarehouseAssets, ReportingAssets],
        reload_graph?: true
      )

    assert {:error, :invalid_opts} =
             Favn.Assets.GraphIndex.related_assets({ReportingAssets, :dashboard},
               direction: :sideways
             )

    assert {:error, :invalid_opts} =
             Favn.Assets.GraphIndex.related_assets({ReportingAssets, :dashboard},
               transitive: :yes
             )

    assert {:error, :invalid_opts} =
             Favn.Assets.GraphIndex.related_assets({ReportingAssets, :dashboard},
               include_target: 1
             )

    assert {:error, :invalid_opts} =
             Favn.Assets.GraphIndex.related_assets({ReportingAssets, :dashboard},
               tags: :warehouse
             )

    assert {:error, :invalid_opts} =
             Favn.Assets.GraphIndex.related_assets({ReportingAssets, :dashboard},
               modules: ReportingAssets
             )

    assert {:error, :invalid_opts} =
             Favn.Assets.GraphIndex.subgraph({ReportingAssets, :dashboard}, names: :dashboard)
  end

  test "reports missing dependencies during graph construction" do
    missing = %Favn.Asset{
      module: __MODULE__,
      name: :missing_dependency,
      ref: {__MODULE__, :missing_dependency},
      arity: 0,
      file: "test/graph_index_test.exs",
      line: 1,
      depends_on: [{__MODULE__, :does_not_exist}]
    }

    assert {:error,
            {:missing_dependency, {__MODULE__, :missing_dependency},
             {__MODULE__, :does_not_exist}}} =
             Favn.Assets.GraphIndex.build_index([missing])
  end

  test "rejects cycles during graph construction" do
    a = %Favn.Asset{
      module: __MODULE__,
      name: :a,
      ref: {__MODULE__, :a},
      arity: 0,
      file: "test/graph_index_test.exs",
      line: 1,
      depends_on: [{__MODULE__, :b}]
    }

    b = %Favn.Asset{
      module: __MODULE__,
      name: :b,
      ref: {__MODULE__, :b},
      arity: 0,
      file: "test/graph_index_test.exs",
      line: 2,
      depends_on: [{__MODULE__, :a}]
    }

    assert {:error, {:cycle, cycle}} = Favn.Assets.GraphIndex.build_index([a, b])
    assert cycle == [{__MODULE__, :a}, {__MODULE__, :b}, {__MODULE__, :a}]
  end
end
