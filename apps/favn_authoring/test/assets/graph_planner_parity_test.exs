defmodule FavnAuthoring.Assets.GraphPlannerParityTest do
  use ExUnit.Case, async: false

  alias Favn.Assets.GraphIndex
  alias Favn.Assets.Planner
  alias Favn.Test.Fixtures.Assets.Graph.ReportingAssets
  alias Favn.Test.Fixtures.Assets.Graph.SourceAssets
  alias Favn.Test.Fixtures.Assets.Graph.WarehouseAssets
  alias Favn.Window.Anchor
  alias Favn.Window.Key
  alias FavnTestSupport.Fixtures

  defmodule RelationOrders do
    use Favn.Namespace, relation: [connection: :warehouse, catalog: "raw", schema: "commerce"]
    use Favn.Asset

    @relation [name: "orders"]
    def asset(_ctx), do: :ok
  end

  defmodule RelationCustomers do
    use Favn.Namespace, relation: [connection: :warehouse, catalog: "raw", schema: "commerce"]
    use Favn.Asset

    @relation [name: "customers"]
    def asset(_ctx), do: :ok
  end

  defmodule RelationCustomer360 do
    use Favn.Namespace, relation: [connection: :warehouse, catalog: "gold", schema: "commerce"]
    use Favn.SQLAsset

    @materialized :view
    query do
      ~SQL"""
      select o.id, c.id as customer_id
      from raw.commerce.orders o
      join raw.commerce.customers c on c.id = o.customer_id
      """
    end
  end

  setup_all do
    Fixtures.compile_fixture!(:graph_assets)
    :ok
  end

  test "index_for_modules/1 builds deterministic topology and transitive closures" do
    assert {:ok, index} =
             GraphIndex.index_for_modules([SourceAssets, WarehouseAssets, ReportingAssets])

    assert index.topo_order == [
             {SourceAssets, :raw_customers},
             {SourceAssets, :raw_orders},
             {WarehouseAssets, :normalize_customers},
             {WarehouseAssets, :normalize_orders},
             {WarehouseAssets, :fact_sales},
             {ReportingAssets, :dashboard}
           ]

    assert index.upstream[{WarehouseAssets, :fact_sales}] ==
             MapSet.new([
               {WarehouseAssets, :normalize_customers},
               {WarehouseAssets, :normalize_orders}
             ])

    assert index.transitive_upstream[{ReportingAssets, :dashboard}] ==
             MapSet.new([
               {WarehouseAssets, :fact_sales},
               {WarehouseAssets, :normalize_customers},
               {WarehouseAssets, :normalize_orders},
               {SourceAssets, :raw_customers},
               {SourceAssets, :raw_orders}
             ])
  end

  test "related_assets/2 and subgraph/2 preserve graph semantics with filters" do
    assert :ok = GraphIndex.load([SourceAssets, WarehouseAssets, ReportingAssets])

    assert {:ok, upstream_assets} =
             GraphIndex.related_assets({ReportingAssets, :dashboard},
               direction: :upstream,
               transitive: true,
               tags: [:warehouse]
             )

    assert Enum.map(upstream_assets, & &1.ref) == [
             {WarehouseAssets, :normalize_customers},
             {WarehouseAssets, :normalize_orders}
           ]

    assert {:ok, subgraph} =
             GraphIndex.subgraph({ReportingAssets, :dashboard}, direction: :upstream)

    assert subgraph.topo_order == [
             {SourceAssets, :raw_customers},
             {SourceAssets, :raw_orders},
             {WarehouseAssets, :normalize_customers},
             {WarehouseAssets, :normalize_orders},
             {WarehouseAssets, :fact_sales},
             {ReportingAssets, :dashboard}
           ]
  end

  test "planner keeps dependencies mode semantics" do
    assert {:ok, index} =
             GraphIndex.index_for_modules([SourceAssets, WarehouseAssets, ReportingAssets])

    assert {:ok, with_dependencies} =
             Planner.plan({ReportingAssets, :dashboard}, graph_index: index, dependencies: :all)

    assert with_dependencies.topo_order == [
             {SourceAssets, :raw_customers},
             {SourceAssets, :raw_orders},
             {WarehouseAssets, :normalize_customers},
             {WarehouseAssets, :normalize_orders},
             {WarehouseAssets, :fact_sales},
             {ReportingAssets, :dashboard}
           ]

    assert {:ok, target_only} =
             Planner.plan({ReportingAssets, :dashboard}, graph_index: index, dependencies: :none)

    assert target_only.topo_order == [{ReportingAssets, :dashboard}]
    assert target_only.stages == [[{ReportingAssets, :dashboard}]]
  end

  test "planner expands anchor ranges across windowed assets" do
    module_name = Module.concat(__MODULE__, "Windowed#{System.unique_integer([:positive])}")

    Code.compile_string(
      """
      defmodule #{inspect(module_name)} do
        use Favn.Assets

        @asset true
        @window Favn.Window.daily()
        def source(_ctx), do: :ok

        @asset true
        @window Favn.Window.daily()
        @depends :source
        def target(_ctx), do: :ok
      end
      """,
      "test/assets/graph_planner_parity_test.exs"
    )

    assert {:ok, index} = GraphIndex.index_for_modules([module_name])

    range = %{
      kind: :day,
      start_at: DateTime.from_naive!(~N[2025-01-10 00:00:00], "Etc/UTC"),
      end_at: DateTime.from_naive!(~N[2025-01-13 00:00:00], "Etc/UTC"),
      timezone: "Etc/UTC"
    }

    assert {:ok, plan} =
             Planner.plan({module_name, :target},
               graph_index: index,
               dependencies: :all,
               anchor_ranges: [range]
             )

    assert length(plan.target_node_keys) == 3
    assert map_size(plan.nodes) == 6

    assert Enum.all?(plan.target_node_keys, fn {_ref, key} ->
             is_map(key) and Key.validate(key) == :ok
           end)
  end

  test "planner counts civil daily windows across DST ranges" do
    module_name = Module.concat(__MODULE__, "DailyDSTRoot#{System.unique_integer([:positive])}")

    Code.compile_string(
      """
      defmodule #{inspect(module_name)} do
        use Favn.Assets

        @asset true
        @window Favn.Window.daily(timezone: "Europe/Oslo")
        def target(_ctx), do: :ok
      end
      """,
      "test/assets/graph_planner_parity_test.exs"
    )

    assert {:ok, index} = GraphIndex.index_for_modules([module_name])

    range = %{
      kind: :day,
      start_at:
        DateTime.from_naive!(~N[2026-03-28 00:00:00], "Europe/Oslo", Favn.Timezone.database!()),
      end_at:
        DateTime.from_naive!(~N[2026-03-30 00:00:00], "Europe/Oslo", Favn.Timezone.database!()),
      timezone: "Europe/Oslo"
    }

    assert DateTime.diff(range.end_at, range.start_at, :hour) == 47

    assert {:ok, plan} =
             Planner.plan({module_name, :target},
               graph_index: index,
               dependencies: :all,
               anchor_ranges: [range]
             )

    assert length(plan.target_node_keys) == 2
  end

  test "graph index and planner include relation-inferred SQL dependencies" do
    assert {:ok, index} =
             GraphIndex.index_for_modules([
               RelationOrders,
               RelationCustomers,
               RelationCustomer360
             ])

    assert index.upstream[{RelationCustomer360, :asset}] ==
             MapSet.new([{RelationCustomers, :asset}, {RelationOrders, :asset}])

    assert {:ok, plan} =
             Planner.plan({RelationCustomer360, :asset}, graph_index: index, dependencies: :all)

    assert plan.topo_order == [
             {RelationCustomers, :asset},
             {RelationOrders, :asset},
             {RelationCustomer360, :asset}
           ]
  end

  test "planner anchor_window validation returns precise errors" do
    assert {:error, {:invalid_anchor_window, :bad}} =
             Planner.plan({SourceAssets, :raw_orders},
               asset_modules: [SourceAssets, WarehouseAssets],
               anchor_window: :bad
             )

    invalid_anchor =
      %Anchor{
        kind: :day,
        start_at: ~U[2026-04-02 00:00:00Z],
        end_at: ~U[2026-04-01 00:00:00Z],
        timezone: "Etc/UTC",
        key: %{kind: :day, start_at_us: 1, timezone: "Etc/UTC"}
      }

    assert {:error, :invalid_window_bounds} =
             Planner.plan({SourceAssets, :raw_orders},
               asset_modules: [SourceAssets, WarehouseAssets],
               anchor_window: invalid_anchor
             )
  end
end
