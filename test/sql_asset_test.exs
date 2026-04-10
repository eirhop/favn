defmodule Favn.SQLAssetTest do
  use ExUnit.Case

  alias Favn.Asset
  alias Favn.Assets.Compiler
  alias Favn.SQLAsset.Compiler, as: SQLAssetCompiler
  alias Favn.SQLAsset.Definition

  setup do
    state = Favn.TestSetup.capture_state()

    on_exit(fn ->
      Favn.TestSetup.restore_state(state, reload_graph?: true)
    end)

    :ok
  end

  test "compiles one SQL asset module into one canonical asset" do
    root = Module.concat(__MODULE__, "Warehouse#{System.unique_integer([:positive])}")
    gold = Module.concat(root, Gold)
    sales = Module.concat(gold, Sales)
    asset_module = Module.concat(sales, FctOrders)

    Code.compile_string(
      "defmodule #{inspect(root)} do\n  use Favn.Namespace, connection: :warehouse\nend",
      "test/dynamic_sql_asset_test.exs"
    )

    Code.compile_string(
      "defmodule #{inspect(gold)} do\n  use Favn.Namespace, catalog: :gold\nend",
      "test/dynamic_sql_asset_test.exs"
    )

    Code.compile_string(
      "defmodule #{inspect(sales)} do\n  use Favn.Namespace, schema: :sales\nend",
      "test/dynamic_sql_asset_test.exs"
    )

    Code.compile_string(
      """
      defmodule #{inspect(asset_module)} do
        use Favn.SQLAsset

        @doc "Gold fact table for orders"
        @meta owner: "analytics", category: :sales, tags: [:gold]
        @window Favn.Window.daily(lookback: 2)
        @materialized {:incremental, strategy: :delete_insert, unique_key: [:order_id]}

        query do
          ~SQL[select order_id from silver.sales.stg_orders]
        end
      end
      """,
      "test/dynamic_sql_asset_test.exs"
    )

    assert {:ok, [%Asset{} = asset]} = Compiler.compile_module_assets(asset_module)
    assert {:ok, %Definition{} = definition} = SQLAssetCompiler.fetch_definition(asset_module)

    assert asset.ref == {asset_module, :asset}
    assert asset.type == :sql
    assert asset.doc == "Gold fact table for orders"
    assert asset.meta == %{owner: "analytics", category: :sales, tags: [:gold]}

    assert asset.materialization ==
             {:incremental, strategy: :delete_insert, unique_key: [:order_id]}

    assert asset.window_spec == %Favn.Window.Spec{kind: :day, lookback: 2, timezone: "Etc/UTC"}

    assert asset.produces == %Favn.RelationRef{
             connection: :warehouse,
             catalog: "gold",
             schema: "sales",
             name: "fct_orders"
           }

    assert definition.asset.ref == asset.ref
    assert definition.sql =~ "select order_id"
    assert definition.materialization == asset.materialization
  end

  test "supports explicit depends and produces overrides for SQL assets" do
    upstream = Module.concat(__MODULE__, "Upstream#{System.unique_integer([:positive])}")
    asset_module = Module.concat(__MODULE__, "SqlDepends#{System.unique_integer([:positive])}")

    Code.compile_string(
      """
      defmodule #{inspect(upstream)} do
        use Favn.Asset

        def asset(_ctx), do: :ok
      end
      """,
      "test/dynamic_sql_asset_test.exs"
    )

    Code.compile_string(
      """
      defmodule #{inspect(asset_module)} do
        use Favn.Namespace, connection: :warehouse, catalog: :silver, schema: :sales
        use Favn.SQLAsset

        @depends #{inspect(upstream)}
        @materialized :table
        @produces schema: :mart, table: :fact_orders
        query do
          ~SQL[select * from silver.sales.stg_orders]
        end
      end
      """,
      "test/dynamic_sql_asset_test.exs"
    )

    assert {:ok, [%Asset{} = asset]} = Compiler.compile_module_assets(asset_module)

    assert asset.depends_on == [{upstream, :asset}]

    assert asset.produces == %Favn.RelationRef{
             connection: :warehouse,
             catalog: "silver",
             schema: "mart",
             name: "fact_orders"
           }
  end

  test "public facade treats SQLAsset modules as single-asset modules" do
    asset_module = Module.concat(__MODULE__, "Facade#{System.unique_integer([:positive])}")

    Code.compile_string(
      """
      defmodule #{inspect(asset_module)} do
        use Favn.Namespace, connection: :warehouse, catalog: :gold, schema: :sales
        use Favn.SQLAsset

        @doc "Facade SQL asset"
        @materialized :view

        query do
          ~SQL[select 1 as id]
        end
      end
      """,
      "test/dynamic_sql_asset_test.exs"
    )

    :ok = Favn.TestSetup.setup_asset_modules([asset_module])

    assert Favn.asset_module?(asset_module)
    assert {:ok, asset} = Favn.get_asset(asset_module)
    assert asset.type == :sql
    assert asset.ref == {asset_module, :asset}
  end

  test "runtime execution returns an explicit not-implemented error for SQL assets" do
    asset_module = Module.concat(__MODULE__, "Runtime#{System.unique_integer([:positive])}")

    Code.compile_string(
      """
      defmodule #{inspect(asset_module)} do
        use Favn.Namespace, connection: :warehouse, catalog: :gold, schema: :sales
        use Favn.SQLAsset

        @materialized :table

        query do
          ~SQL[select 1 as id]
        end
      end
      """,
      "test/dynamic_sql_asset_test.exs"
    )

    :ok = Favn.TestSetup.setup_asset_modules([asset_module], reload_graph?: true)

    assert {:ok, run_id} = Favn.run_asset({asset_module, :asset}, dependencies: :none)
    assert {:error, run} = Favn.await_run(run_id, timeout: 5_000)

    assert run.status == :error

    assert run.asset_results[{asset_module, :asset}].error.reason ==
             :sql_asset_runtime_not_implemented
  end

  test "rejects missing materialized attribute" do
    assert_raise CompileError, ~r/Favn\.SQLAsset requires one @materialized attribute/, fn ->
      compile_sql_asset_module("""
      use Favn.Namespace, connection: :warehouse
      use Favn.SQLAsset

      query do
        ~SQL[select 1]
      end
      """)
    end
  end

  test "rejects missing connection for produced SQL relation" do
    assert_raise CompileError,
                 ~r/SQL assets require a connection through Favn\.Namespace or @produces/,
                 fn ->
                   compile_sql_asset_module("""
                   use Favn.SQLAsset

                   @materialized :view
                   query do
                     ~SQL[select 1]
                   end
                   """)
                 end
  end

  test "rejects SQL sigil modifiers" do
    assert_raise CompileError, ~r/~SQL sigil does not support modifiers/, fn ->
      module_name =
        Module.concat(__MODULE__, "SigilModifiers#{System.unique_integer([:positive])}")

      Code.compile_string(
        "defmodule #{inspect(module_name)} do\n" <>
          "  use Favn.Namespace, connection: :warehouse\n" <>
          "  use Favn.SQLAsset\n\n" <>
          "  @materialized :view\n" <>
          "  query do\n" <>
          "    ~SQL[select 1]x\n" <>
          "  end\n" <>
          "end\n",
        "test/dynamic_sql_asset_test.exs"
      )
    end
  end

  test "rejects module shorthand depends for multi-asset modules" do
    asset_module = Module.concat(__MODULE__, "BadDepends#{System.unique_integer([:positive])}")

    Code.compile_string(
      """
      defmodule #{inspect(asset_module)} do
        use Favn.Namespace, connection: :warehouse
        use Favn.SQLAsset

        @depends #{inspect(Favn.AssetsTest.Upstream)}
        @materialized :view

        query do
          ~SQL[select 1]
        end
      end
      """,
      "test/dynamic_sql_asset_test.exs"
    )

    assert {:error, {:invalid_compiled_assets, message}} =
             Compiler.compile_module_assets(asset_module)

    assert message =~ "invalid @depends entry #{inspect(Favn.AssetsTest.Upstream)}"
    assert message =~ "single-asset module"
  end

  test "rejects multiple produces attributes with a controlled compile error" do
    assert_raise CompileError, ~r/multiple @produces attributes are not allowed/, fn ->
      compile_sql_asset_module("""
      use Favn.Namespace, connection: :warehouse
      use Favn.SQLAsset

      @materialized :view
      @produces true
      @produces name: :orders

      query do
        ~SQL[select 1]
      end
      """)
    end
  end

  test "single-asset module shorthand depends is compile-order independent for SQL assets" do
    upstream = Module.concat(__MODULE__, "LateUpstream#{System.unique_integer([:positive])}")
    asset_module = Module.concat(__MODULE__, "LateDepends#{System.unique_integer([:positive])}")

    Code.compile_string(
      """
      defmodule #{inspect(asset_module)} do
        use Favn.Namespace, connection: :warehouse, catalog: :gold, schema: :sales
        use Favn.SQLAsset

        @depends #{inspect(upstream)}
        @materialized :view

        query do
          ~SQL[select 1]
        end
      end
      """,
      "test/dynamic_sql_asset_test.exs"
    )

    Code.compile_string(
      """
      defmodule #{inspect(upstream)} do
        use Favn.Asset

        def asset(_ctx), do: :ok
      end
      """,
      "test/dynamic_sql_asset_test.exs"
    )

    assert {:ok, [%Asset{} = asset]} = Compiler.compile_module_assets(asset_module)
    assert asset.depends_on == [{upstream, :asset}]
  end

  defp compile_sql_asset_module(body) do
    module_name = Module.concat(__MODULE__, "Dynamic#{System.unique_integer([:positive])}")

    Code.compile_string(
      """
      defmodule #{inspect(module_name)} do
      #{indent(body, 2)}
      end
      """,
      "test/dynamic_sql_asset_test.exs"
    )
  end

  defp indent(string, spaces) do
    padding = String.duplicate(" ", spaces)

    string
    |> String.trim_trailing()
    |> String.split("\n")
    |> Enum.map_join("\n", &(padding <> &1))
  end
end
