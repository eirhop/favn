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
      "defmodule #{inspect(root)} do\n  use Favn.Namespace, relation: [connection: :warehouse]\nend",
      "test/dynamic_sql_asset_test.exs"
    )

    Code.compile_string(
      "defmodule #{inspect(gold)} do\n  use Favn.Namespace, relation: [catalog: :gold]\nend",
      "test/dynamic_sql_asset_test.exs"
    )

    Code.compile_string(
      "defmodule #{inspect(sales)} do\n  use Favn.Namespace, relation: [schema: :sales]\nend",
      "test/dynamic_sql_asset_test.exs"
    )

    Code.compile_string(
      """
      defmodule #{inspect(asset_module)} do
        use Favn.SQLAsset

        @doc "Gold fact table for orders"
        @meta owner: "analytics", category: :sales, tags: [:gold]
        @window Favn.Window.daily(lookback: 2)
        @materialized {:incremental, strategy: :delete_insert, window_column: :order_date}

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
             {:incremental, strategy: :delete_insert, window_column: :order_date}

    assert asset.window_spec == %Favn.Window.Spec{kind: :day, lookback: 2, timezone: "Etc/UTC"}

    assert asset.relation == %Favn.RelationRef{
             connection: :warehouse,
             catalog: "gold",
             schema: "sales",
             name: "fct_orders"
           }

    assert definition.asset.ref == asset.ref
    assert definition.sql =~ "select order_id"
    assert definition.materialization == asset.materialization
  end

  test "supports explicit depends and relation overrides for SQL assets" do
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
        use Favn.Namespace, relation: [connection: :warehouse, catalog: :silver, schema: :sales]
        use Favn.SQLAsset

        @depends #{inspect(upstream)}
        @materialized :table
        @relation schema: :mart, table: :fact_orders
        query do
          ~SQL[select * from silver.sales.stg_orders]
        end
      end
      """,
      "test/dynamic_sql_asset_test.exs"
    )

    assert {:ok, [%Asset{} = asset]} = Compiler.compile_module_assets(asset_module)

    assert asset.depends_on == [{upstream, :asset}]

    assert asset.relation == %Favn.RelationRef{
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
        use Favn.Namespace, relation: [connection: :warehouse, catalog: :gold, schema: :sales]
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

  test "supports file-backed query bodies" do
    root = Module.concat(__MODULE__, "FileBacked#{System.unique_integer([:positive])}")
    asset_module = Module.concat(root, Asset)
    fixture = write_sql_fixture!("sql_asset_query.sql", "select @country as country")

    Code.compile_string(
      """
      defmodule #{inspect(asset_module)} do
        use Favn.Namespace, relation: [connection: :warehouse, catalog: :gold, schema: :sales]
        use Favn.SQLAsset

        @materialized :view

        query file: #{inspect(fixture.relative)}
      end
      """,
      fixture.owner_file
    )

    assert {:ok, %Definition{} = definition} = SQLAssetCompiler.fetch_definition(asset_module)
    assert definition.sql == "select @country as country"
    assert String.ends_with?(definition.raw_asset.sql_file, "sql_asset_query.sql")
    assert definition.raw_asset.sql_line == 1
    assert definition.template.span.start_line == 1
  end

  test "reports SQL file diagnostics for file-backed query bodies" do
    root = Module.concat(__MODULE__, "BadFileBacked#{System.unique_integer([:positive])}")
    asset_module = Module.concat(root, Asset)
    fixture = write_sql_fixture!("sql_asset_bad_query.sql", "select @1bad")

    error =
      assert_raise CompileError, fn ->
        Code.compile_string(
          """
          defmodule #{inspect(asset_module)} do
            use Favn.Namespace, relation: [connection: :warehouse, catalog: :gold, schema: :sales]
            use Favn.SQLAsset

            @materialized :view

            query file: #{inspect(fixture.relative)}
          end
          """,
          fixture.owner_file
        )
      end

    assert String.ends_with?(to_string(error.file), "sql_asset_bad_query.sql")
    assert error.line == 1
  end

  test "rejects deferred incremental strategy :merge at compile time" do
    assert_raise CompileError, ~r/incremental strategy :merge is not supported in Phase 4b/, fn ->
      compile_sql_asset_module("""
      use Favn.Namespace, relation: [connection: :warehouse]
      use Favn.SQLAsset

      @window Favn.Window.daily()
      @materialized {:incremental, strategy: :merge}

      query do
        ~SQL[select 1 as id]
      end
      """)
    end
  end

  test "rejects incremental unique_key for Phase 4b strategies" do
    assert_raise CompileError,
                 ~r/incremental materialization unique_key is reserved for future :merge semantics/,
                 fn ->
                   compile_sql_asset_module("""
                   use Favn.Namespace, relation: [connection: :warehouse]
                   use Favn.SQLAsset

                   @window Favn.Window.daily()
                   @materialized {:incremental, strategy: :append, unique_key: [:id]}

                   query do
                     ~SQL[select 1 as id]
                   end
                   """)
                 end
  end

  test "rejects delete_insert without window_column" do
    assert_raise CompileError, ~r/incremental :delete_insert requires :window_column/, fn ->
      compile_sql_asset_module("""
      use Favn.Namespace, relation: [connection: :warehouse]
      use Favn.SQLAsset

      @window Favn.Window.daily()
      @materialized {:incremental, strategy: :delete_insert}

      query do
        ~SQL[select 1 as id]
      end
      """)
    end
  end

  test "rejects incremental materialization without @window" do
    assert_raise CompileError, ~r/incremental SQL materialization requires @window/, fn ->
      compile_sql_asset_module("""
      use Favn.Namespace, relation: [connection: :warehouse]
      use Favn.SQLAsset

      @materialized {:incremental, strategy: :append}

      query do
        ~SQL[select 1 as id]
      end
      """)
    end
  end

  test "rejects missing materialized attribute" do
    assert_raise CompileError, ~r/Favn\.SQLAsset requires one @materialized attribute/, fn ->
      compile_sql_asset_module("""
      use Favn.Namespace, relation: [connection: :warehouse]
      use Favn.SQLAsset

      query do
        ~SQL[select 1]
      end
      """)
    end
  end

  test "rejects missing connection for produced SQL relation" do
    assert_raise CompileError,
                 ~r/SQL assets require a connection through Favn\.Namespace or @relation/,
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
          "  use Favn.Namespace, relation: [connection: :warehouse]\n" <>
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

  test "rejects plain string query bodies" do
    assert_raise CompileError, ~r/query body must contain a ~SQL literal/, fn ->
      compile_sql_asset_module("""
      use Favn.Namespace, relation: [connection: :warehouse]
      use Favn.SQLAsset

      @materialized :view

      query do
        "select 1"
      end
      """)
    end
  end

  test "rejects module shorthand depends for multi-asset modules" do
    asset_module = Module.concat(__MODULE__, "BadDepends#{System.unique_integer([:positive])}")

    Code.compile_string(
      """
      defmodule #{inspect(asset_module)} do
        use Favn.Namespace, relation: [connection: :warehouse]
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

  test "rejects multiple relation attributes with a controlled compile error" do
    assert_raise CompileError, ~r/multiple @relation attributes are not allowed/, fn ->
      compile_sql_asset_module("""
      use Favn.Namespace, relation: [connection: :warehouse]
      use Favn.SQLAsset

      @materialized :view
      @relation true
      @relation name: :orders

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
        use Favn.Namespace, relation: [connection: :warehouse, catalog: :gold, schema: :sales]
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

  defp write_sql_fixture!(file_name, body) do
    uniq = "sql_asset_#{System.unique_integer([:positive])}"
    base = Path.join(File.cwd!(), "tmp/favn_test_#{uniq}")
    owner_dir = Path.join(base, "lib/my_app")
    sql_dir = Path.join(base, "sql")

    File.mkdir_p!(owner_dir)
    File.mkdir_p!(sql_dir)

    owner_file = Path.join(owner_dir, "asset.ex")
    sql_file = Path.join(sql_dir, file_name)
    File.write!(sql_file, body)

    %{owner_file: owner_file, relative: "../../sql/#{file_name}"}
  end
end
