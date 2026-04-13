defmodule Favn.SQLDependencyInferenceTest do
  use ExUnit.Case

  alias Favn.Assets.GraphIndex
  alias Favn.Assets.Registry

  setup do
    state = Favn.TestSetup.capture_state()

    on_exit(fn ->
      Favn.TestSetup.restore_state(state, reload_graph?: true)
    end)

    :ok
  end

  test "infers dependency from SQL plain relation reference" do
    root = Module.concat(__MODULE__, "Infer#{System.unique_integer([:positive])}")
    raw_orders = Module.concat(root, RawOrders)
    fct_orders = Module.concat(root, FctOrders)

    compile_elixir_asset!(raw_orders, "raw_orders")
    compile_sql_asset!(fct_orders, "select * from silver.sales.raw_orders")

    :ok = Favn.TestSetup.setup_asset_modules([raw_orders, fct_orders], reload_graph?: true)

    assert {:ok, asset} = Favn.get_asset(fct_orders)
    assert asset.depends_on == [{raw_orders, :asset}]

    assert [%Favn.Asset.Dependency{asset_ref: {^raw_orders, :asset}, provenance: provenance}] =
             asset.dependencies

    assert Enum.sort(provenance) == [:inferred_sql_relation]
  end

  test "dedupes explicit and inferred dependency and retains provenance" do
    root = Module.concat(__MODULE__, "Dedupe#{System.unique_integer([:positive])}")
    raw_orders = Module.concat(root, RawOrders)
    fct_orders = Module.concat(root, FctOrders)

    compile_elixir_asset!(raw_orders, "raw_orders")

    Code.compile_string(
      """
      defmodule #{inspect(fct_orders)} do
        use Favn.Namespace, relation: [connection: :warehouse, catalog: :gold, schema: :sales]
        use Favn.SQLAsset

        @depends #{inspect(raw_orders)}
        @materialized :view

        query do
          ~SQL[select * from silver.sales.raw_orders]
        end
      end
      """,
      "test/dynamic_sql_dependency_inference_test.exs"
    )

    :ok = Favn.TestSetup.setup_asset_modules([raw_orders, fct_orders], reload_graph?: true)

    assert {:ok, asset} = Favn.get_asset(fct_orders)
    assert asset.depends_on == [{raw_orders, :asset}]

    assert [%Favn.Asset.Dependency{asset_ref: {^raw_orders, :asset}, provenance: provenance}] =
             asset.dependencies

    assert Enum.sort(provenance) == [:explicit, :inferred_sql_relation]
  end

  test "emits warning diagnostic for unmanaged SQL relation" do
    root = Module.concat(__MODULE__, "Unmanaged#{System.unique_integer([:positive])}")
    fct_orders = Module.concat(root, FctOrders)

    compile_sql_asset!(fct_orders, "select * from ext_orders")

    assert {:ok, catalog} = Registry.build_catalog([fct_orders])
    [asset] = catalog.assets

    assert Enum.any?(asset.diagnostics, fn diagnostic ->
             diagnostic.code == :unmanaged_relation_reference and diagnostic.severity == :warning
           end)
  end

  test "fails catalog build for ambiguous relation ownership" do
    root = Module.concat(__MODULE__, "Ambiguous#{System.unique_integer([:positive])}")
    owner_a = Module.concat(root, OwnerA)
    owner_b = Module.concat(root, OwnerB)
    consumer = Module.concat(root, Consumer)

    compile_elixir_asset_with_schema!(owner_a, "orders", :sales)
    compile_elixir_asset_with_schema!(owner_b, "orders", :marketing)

    Code.compile_string(
      """
      defmodule #{inspect(consumer)} do
        use Favn.Namespace, relation: [connection: :warehouse]
        use Favn.SQLAsset

        @materialized :view

        query do
          ~SQL[select * from orders]
        end
      end
      """,
      "test/dynamic_sql_dependency_inference_test.exs"
    )

    assert {:error, {:dependency_inference_error, {^consumer, :asset}, diagnostic}} =
             Registry.build_catalog([owner_a, owner_b, consumer])

    assert diagnostic.code == :ambiguous_relation_owner
  end

  test "cycle detection includes inferred edges" do
    root = Module.concat(__MODULE__, "Cycle#{System.unique_integer([:positive])}")
    a = Module.concat(root, A)
    b = Module.concat(root, B)

    compile_sql_asset!(a, "select * from b")
    compile_sql_asset!(b, "select * from a")

    assert {:ok, catalog} = Registry.build_catalog([a, b])
    assert {:error, {:cycle, _cycle}} = GraphIndex.build_index(catalog.assets)
  end

  test "infers dependency from relation ref inside defsql body" do
    root = Module.concat(__MODULE__, "Defsql#{System.unique_integer([:positive])}")
    sql_module = Module.concat(root, SQL)
    raw_orders = Module.concat(root, RawOrders)
    consumer = Module.concat(root, Consumer)

    compile_elixir_asset!(raw_orders, "raw_orders")

    Code.compile_string(
      """
      defmodule #{inspect(sql_module)} do
        use Favn.SQL

        defsql orders_in_window(start_at, end_at) do
          ~SQL[
          select *
          from silver.sales.raw_orders
          where inserted_at >= @start_at and inserted_at < @end_at
          ]
        end
      end
      """,
      "test/dynamic_sql_dependency_inference_test.exs"
    )

    Code.compile_string(
      """
      defmodule #{inspect(consumer)} do
        use Favn.Namespace, relation: [connection: :warehouse, catalog: :gold, schema: :sales]
        use Favn.SQLAsset
        use #{inspect(sql_module)}

        @materialized :view

        query do
          ~SQL[select * from orders_in_window(@window_start, @window_end)]
        end
      end
      """,
      "test/dynamic_sql_dependency_inference_test.exs"
    )

    :ok = Favn.TestSetup.setup_asset_modules([raw_orders, consumer], reload_graph?: true)

    assert {:ok, asset} = Favn.get_asset(consumer)
    assert asset.depends_on == [{raw_orders, :asset}]
  end

  test "infers dependency from nested defsql relation chain" do
    root = Module.concat(__MODULE__, "NestedDefsql#{System.unique_integer([:positive])}")
    sql_module = Module.concat(root, SQL)
    raw_orders = Module.concat(root, RawOrders)
    consumer = Module.concat(root, Consumer)

    compile_elixir_asset!(raw_orders, "raw_orders")

    Code.compile_string(
      """
      defmodule #{inspect(sql_module)} do
        use Favn.SQL

        defsql raw_orders_relation(start_at) do
          ~SQL[select * from silver.sales.raw_orders where inserted_at >= @start_at]
        end

        defsql wrapped_orders(start_at) do
          ~SQL[select * from raw_orders_relation(@start_at)]
        end
      end
      """,
      "test/dynamic_sql_dependency_inference_test.exs"
    )

    Code.compile_string(
      """
      defmodule #{inspect(consumer)} do
        use Favn.Namespace, relation: [connection: :warehouse, catalog: :gold, schema: :sales]
        use Favn.SQLAsset
        use #{inspect(sql_module)}

        @materialized :view

        query do
          ~SQL[select * from wrapped_orders(@window_start)]
        end
      end
      """,
      "test/dynamic_sql_dependency_inference_test.exs"
    )

    :ok = Favn.TestSetup.setup_asset_modules([raw_orders, consumer], reload_graph?: true)

    assert {:ok, asset} = Favn.get_asset(consumer)
    assert asset.depends_on == [{raw_orders, :asset}]
  end

  test "infers direct asset refs used inside defsql body" do
    root = Module.concat(__MODULE__, "DefsqlAssetRef#{System.unique_integer([:positive])}")
    sql_module = Module.concat(root, SQL)
    raw_orders = Module.concat(root, RawOrders)
    consumer = Module.concat(root, Consumer)

    compile_elixir_asset!(raw_orders, "raw_orders")

    Code.compile_string(
      """
      defmodule #{inspect(sql_module)} do
        use Favn.SQL

        defsql raw_orders_relation(start_at) do
          ~SQL[select * from #{inspect(raw_orders)} where inserted_at >= @start_at]
        end
      end
      """,
      "test/dynamic_sql_dependency_inference_test.exs"
    )

    Code.compile_string(
      """
      defmodule #{inspect(consumer)} do
        use Favn.Namespace, relation: [connection: :warehouse, catalog: :gold, schema: :sales]
        use Favn.SQLAsset
        use #{inspect(sql_module)}

        @materialized :view

        query do
          ~SQL[select * from raw_orders_relation(@window_start)]
        end
      end
      """,
      "test/dynamic_sql_dependency_inference_test.exs"
    )

    :ok = Favn.TestSetup.setup_asset_modules([raw_orders, consumer], reload_graph?: true)

    assert {:ok, asset} = Favn.get_asset(consumer)
    assert asset.depends_on == [{raw_orders, :asset}]
  end

  test "fails catalog build for cross-connection direct asset ref" do
    root = Module.concat(__MODULE__, "CrossConn#{System.unique_integer([:positive])}")
    upstream = Module.concat(root, Upstream)
    consumer = Module.concat(root, Consumer)

    Code.compile_string(
      """
      defmodule #{inspect(upstream)} do
        use Favn.Namespace, relation: [connection: :other, catalog: :silver, schema: :sales]
        use Favn.Asset

        @relation true
        def asset(_ctx), do: :ok
      end
      """,
      "test/dynamic_sql_dependency_inference_test.exs"
    )

    Code.compile_string(
      """
      defmodule #{inspect(consumer)} do
        use Favn.Namespace, relation: [connection: :warehouse, catalog: :gold, schema: :sales]
        use Favn.SQLAsset

        @materialized :view

        query do
          ~SQL[select * from #{inspect(upstream)}]
        end
      end
      """,
      "test/dynamic_sql_dependency_inference_test.exs"
    )

    assert {:error, {:dependency_inference_error, {^consumer, :asset}, diagnostic}} =
             Registry.build_catalog([upstream, consumer])

    assert diagnostic.code == :cross_connection_direct_asset_ref
  end

  test "ignores nested CTE aliases while inferring relation dependencies" do
    root = Module.concat(__MODULE__, "NestedCTE#{System.unique_integer([:positive])}")
    raw_orders = Module.concat(root, RawOrders)
    customers = Module.concat(root, Customers)
    consumer = Module.concat(root, Consumer)

    compile_elixir_asset!(raw_orders, "raw_orders")
    compile_elixir_asset!(customers, "customers")

    compile_sql_asset!(
      consumer,
      """
      with raw_orders as (
        select * from silver.sales.raw_orders
      )
      select *
      from raw_orders
      join (
        with customers as (
          select * from silver.sales.customers
        )
        select * from customers
      ) nested_customers on true
      """
    )

    :ok =
      Favn.TestSetup.setup_asset_modules([raw_orders, customers, consumer], reload_graph?: true)

    assert {:ok, asset} = Favn.get_asset(consumer)

    assert asset.depends_on == [{customers, :asset}, {raw_orders, :asset}]

    refute Enum.any?(asset.diagnostics, fn diagnostic ->
             diagnostic.code == :unmanaged_relation_reference
           end)
  end

  test "ignores CTE aliases inside nested defsql relation chain" do
    root = Module.concat(__MODULE__, "NestedDefsqlCTE#{System.unique_integer([:positive])}")
    sql_module = Module.concat(root, SQL)
    raw_orders = Module.concat(root, RawOrders)
    customers = Module.concat(root, Customers)
    consumer = Module.concat(root, Consumer)

    compile_elixir_asset!(raw_orders, "raw_orders")
    compile_elixir_asset!(customers, "customers")

    Code.compile_string(
      """
      defmodule #{inspect(sql_module)} do
        use Favn.SQL

        defsql filtered_orders(start_at) do
          ~SQL[
          with scoped_orders as (
            select * from silver.sales.raw_orders
          )
          select *
          from scoped_orders
          where inserted_at >= @start_at
            and exists (
            with scoped_customers as (
              select customer_id from silver.sales.customers
            )
            select 1 from scoped_customers
          )
          ]
        end

        defsql wrapped_orders(start_at) do
          ~SQL[select * from filtered_orders(@start_at)]
        end
      end
      """,
      "test/dynamic_sql_dependency_inference_test.exs"
    )

    Code.compile_string(
      """
      defmodule #{inspect(consumer)} do
        use Favn.Namespace, relation: [connection: :warehouse, catalog: :gold, schema: :sales]
        use Favn.SQLAsset
        use #{inspect(sql_module)}

        @materialized :view

        query do
          ~SQL[select * from wrapped_orders(@window_start)]
        end
      end
      """,
      "test/dynamic_sql_dependency_inference_test.exs"
    )

    :ok =
      Favn.TestSetup.setup_asset_modules([raw_orders, customers, consumer], reload_graph?: true)

    assert {:ok, asset} = Favn.get_asset(consumer)
    assert asset.depends_on == [{customers, :asset}, {raw_orders, :asset}]

    refute Enum.any?(asset.diagnostics, fn diagnostic ->
             diagnostic.code == :unmanaged_relation_reference
           end)
  end

  defp compile_elixir_asset!(module, table_name) do
    Code.compile_string(
      """
      defmodule #{inspect(module)} do
        use Favn.Namespace, relation: [connection: :warehouse, catalog: :silver, schema: :sales]
        use Favn.Asset

        @relation name: #{inspect(table_name)}
        def asset(_ctx), do: :ok
      end
      """,
      "test/dynamic_sql_dependency_inference_test.exs"
    )
  end

  defp compile_elixir_asset_with_schema!(module, table_name, schema_name) do
    Code.compile_string(
      """
      defmodule #{inspect(module)} do
        use Favn.Namespace, relation: [connection: :warehouse, catalog: :silver, schema: #{inspect(schema_name)}]
        use Favn.Asset

        @relation name: #{inspect(table_name)}
        def asset(_ctx), do: :ok
      end
      """,
      "test/dynamic_sql_dependency_inference_test.exs"
    )
  end

  defp compile_sql_asset!(module, sql) do
    Code.compile_string(
      """
      defmodule #{inspect(module)} do
        use Favn.Namespace, relation: [connection: :warehouse, catalog: :gold, schema: :sales]
        use Favn.SQLAsset

        @materialized :view

        query do
          ~SQL[#{sql}]
        end
      end
      """,
      "test/dynamic_sql_dependency_inference_test.exs"
    )
  end
end
