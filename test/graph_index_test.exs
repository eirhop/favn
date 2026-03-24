defmodule Flux.GraphIndexTest do
  use ExUnit.Case

  defmodule SourceAssets do
    use Flux.Assets

    @doc "Raw orders"
    @asset true
    def raw_orders, do: [%{id: 1}]

    @doc "Raw customers"
    @asset true
    def raw_customers, do: [%{id: 1}]
  end

  defmodule WarehouseAssets do
    use Flux.Assets

    alias Flux.GraphIndexTest.SourceAssets

    @doc "Normalize orders"
    @asset depends_on: [{SourceAssets, :raw_orders}], tags: [:warehouse]
    def normalize_orders(rows), do: rows

    @doc "Normalize customers"
    @asset depends_on: [{SourceAssets, :raw_customers}], tags: [:warehouse]
    def normalize_customers(rows), do: rows

    @doc "Build sales fact"
    @asset depends_on: [:normalize_orders, :normalize_customers], tags: [:finance]
    def fact_sales(orders, customers), do: {orders, customers}
  end

  defmodule ReportingAssets do
    use Flux.Assets

    alias Flux.GraphIndexTest.WarehouseAssets

    @doc "Build dashboard"
    @asset depends_on: [{WarehouseAssets, :fact_sales}, {WarehouseAssets, :normalize_orders}]
    def dashboard(fact_sales, normalize_orders), do: {fact_sales, normalize_orders}
  end

  setup do
    previous_modules = Application.get_env(:flux, :asset_modules)
    previous_catalog = Flux.Registry.build_catalog(previous_modules || [])

    on_exit(fn ->
      if is_nil(previous_modules) do
        Application.delete_env(:flux, :asset_modules)
      else
        Application.put_env(:flux, :asset_modules, previous_modules)
      end

      restore_registry(previous_catalog)
    end)

    :ok
  end

  test "builds a global DAG index with upstream, downstream, transitive closures, and topological order" do
    Application.put_env(:flux, :asset_modules, [SourceAssets, WarehouseAssets, ReportingAssets])

    assert :ok = Flux.Registry.reload()
    assert :ok = Flux.GraphIndex.reload()

    assert {:ok, upstream} = Flux.GraphIndex.upstream_of({WarehouseAssets, :fact_sales})

    assert upstream == [
             {WarehouseAssets, :normalize_customers},
             {WarehouseAssets, :normalize_orders}
           ]

    assert {:ok, downstream} = Flux.GraphIndex.downstream_of({WarehouseAssets, :normalize_orders})

    assert downstream == [
             {WarehouseAssets, :fact_sales},
             {ReportingAssets, :dashboard}
           ]

    assert {:ok, transitive_upstream} =
             Flux.GraphIndex.transitive_upstream_of({ReportingAssets, :dashboard})

    assert transitive_upstream == [
             {SourceAssets, :raw_customers},
             {SourceAssets, :raw_orders},
             {WarehouseAssets, :normalize_customers},
             {WarehouseAssets, :normalize_orders},
             {WarehouseAssets, :fact_sales}
           ]

    assert {:ok, transitive_downstream} =
             Flux.GraphIndex.transitive_downstream_of({SourceAssets, :raw_orders})

    assert transitive_downstream == [
             {WarehouseAssets, :normalize_orders},
             {WarehouseAssets, :fact_sales},
             {ReportingAssets, :dashboard}
           ]

    assert {:ok, topo_order} = Flux.GraphIndex.topological_order()

    assert Enum.find_index(topo_order, &(&1 == {SourceAssets, :raw_orders})) <
             Enum.find_index(topo_order, &(&1 == {WarehouseAssets, :normalize_orders}))

    assert Enum.find_index(topo_order, &(&1 == {WarehouseAssets, :fact_sales})) <
             Enum.find_index(topo_order, &(&1 == {ReportingAssets, :dashboard}))
  end

  test "selects related assets with filters and direct traversal options" do
    Application.put_env(:flux, :asset_modules, [SourceAssets, WarehouseAssets, ReportingAssets])

    assert :ok = Flux.Registry.reload()
    assert :ok = Flux.GraphIndex.reload()

    assert {:ok, assets} =
             Flux.GraphIndex.related_assets({ReportingAssets, :dashboard},
               direction: :upstream,
               tags: [:warehouse]
             )

    assert Enum.map(assets, & &1.ref) == [
             {WarehouseAssets, :normalize_customers},
             {WarehouseAssets, :normalize_orders}
           ]

    assert {:ok, both_neighbors} =
             Flux.GraphIndex.related_assets({WarehouseAssets, :normalize_orders},
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
    Application.put_env(:flux, :asset_modules, [SourceAssets, WarehouseAssets, ReportingAssets])

    assert :ok = Flux.Registry.reload()
    assert :ok = Flux.GraphIndex.reload()

    assert {:ok, subgraph} =
             Flux.GraphIndex.subgraph({ReportingAssets, :dashboard},
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

  test "reports missing dependencies during graph construction" do
    missing = %Flux.Asset{
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
             Flux.GraphIndex.build_index([missing])
  end

  test "rejects cycles during graph construction" do
    a = %Flux.Asset{
      module: __MODULE__,
      name: :a,
      ref: {__MODULE__, :a},
      arity: 0,
      file: "test/graph_index_test.exs",
      line: 1,
      depends_on: [{__MODULE__, :b}]
    }

    b = %Flux.Asset{
      module: __MODULE__,
      name: :b,
      ref: {__MODULE__, :b},
      arity: 0,
      file: "test/graph_index_test.exs",
      line: 2,
      depends_on: [{__MODULE__, :a}]
    }

    assert {:error, {:cycle, cycle}} = Flux.GraphIndex.build_index([a, b])
    assert cycle == [{__MODULE__, :a}, {__MODULE__, :b}, {__MODULE__, :a}]
  end

  defp restore_registry({:ok, _catalog}) do
    :ok = Flux.Registry.reload()
    :ok = Flux.GraphIndex.reload()
  end

  defp restore_registry({:error, _reason}), do: :ok
end
