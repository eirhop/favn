defmodule Favn.AssetsTest.Upstream do
  use Favn.Assets

  @doc "Load source rows"
  @asset true
  def source_rows(_ctx), do: :ok
end

defmodule Favn.AssetsTest.Sample do
  use Favn.Assets

  alias Favn.AssetsTest.Upstream
  alias Favn.Window

  @doc "Extract raw orders"
  @asset "Extract Orders"
  def extract_orders(_ctx), do: :ok

  @doc "Normalize extracted orders"
  @asset true
  @depends :extract_orders
  @meta tags: [:sales, "warehouse"], owner: "data"
  @window Window.daily(lookback: 1)
  def normalize_orders(_ctx), do: :ok

  @doc false
  @asset true
  @depends {Upstream, :source_rows}
  @meta owner: "analytics", category: :sales, tags: [:view]
  def fact_sales(_ctx), do: :ok
end

defmodule Favn.AssetsTest do
  use ExUnit.Case, async: true

  alias Favn.Asset
  alias Favn.Assets.Compiler
  alias Favn.AssetsTest.Sample
  alias Favn.RelationRef

  defmodule SQLLikeCompiler do
    @behaviour Favn.Assets.Compiler

    @impl true
    def compile_assets(module) do
      {:ok,
       [
         %Favn.Asset{
           module: module,
           name: :compiled_sql_asset,
           ref: {module, :compiled_sql_asset},
           arity: 1,
           doc: "compiled sql asset",
           file: "lib/sql_assets.ex",
           line: 1,
           title: "Compiled SQL Asset",
           meta: %{category: :sql},
           depends_on: [],
           window_spec: Favn.Window.daily(),
           relation: %Favn.RelationRef{name: "compiled_sql_asset"}
         }
       ]}
    end
  end

  defmodule InvalidCompiledProducesCompiler do
    @behaviour Favn.Assets.Compiler

    @impl true
    def compile_assets(module) do
      {:ok,
       [
         %Favn.Asset{
           module: module,
           name: :bad_compiled_asset,
           ref: {module, :bad_compiled_asset},
           arity: 1,
           file: "lib/sql_assets.ex",
           line: 1,
           relation: %{name: "bad"}
         }
       ]}
    end
  end

  test "captures canonical asset metadata in source order" do
    assets = Sample.__favn_assets__()

    assert Enum.map(assets, & &1.name) == [:extract_orders, :normalize_orders, :fact_sales]
    assert [%Asset{} = extract, %Asset{} = normalize, %Asset{} = fact] = assets

    assert extract.ref == {Favn.AssetsTest.Sample, :extract_orders}
    assert extract.arity == 1
    assert extract.title == "Extract Orders"
    assert extract.doc == "Extract raw orders"
    assert extract.meta == %{}
    assert extract.depends_on == []
    assert extract.relation == nil

    assert normalize.depends_on == [{Favn.AssetsTest.Sample, :extract_orders}]
    assert normalize.meta == %{owner: "data", tags: [:sales, "warehouse"]}

    assert normalize.window_spec == %Favn.Window.Spec{
             kind: :day,
             lookback: 1,
             timezone: "Etc/UTC"
           }

    assert fact.doc == nil
    assert fact.meta == %{owner: "analytics", category: :sales, tags: [:view]}
    assert fact.depends_on == [{Favn.AssetsTest.Upstream, :source_rows}]
    assert fact.window_spec == nil
    assert fact.relation == nil
  end

  test "captures @relation with namespace defaults and runtime name inference" do
    module_name = Module.concat(__MODULE__, "Produces#{System.unique_integer([:positive])}")

    source = """
    defmodule #{inspect(module_name)} do
      use Favn.Namespace, connection: :warehouse, catalog: :raw, schema: :sales
      use Favn.Assets

      @asset true
      @relation true
      def orders(_ctx), do: :ok

      @asset true
      @relation database: :silver, table: :daily_orders
      def stage_orders(_ctx), do: :ok
    end
    """

    assert Enum.any?(Code.compile_string(source, "test/dynamic_assets_test.exs"), fn {mod, _} ->
             mod == module_name
           end)

    assert {:ok, [orders, stage_orders]} = Compiler.compile_module_assets(module_name)

    assert orders.relation == %RelationRef{
             connection: :warehouse,
             catalog: "raw",
             schema: "sales",
             name: "orders"
           }

    assert stage_orders.relation == %RelationRef{
             connection: :warehouse,
             catalog: "silver",
             schema: "sales",
             name: "daily_orders"
           }
  end

  test "supports single-asset modules with use Favn.Asset" do
    module_name = Module.concat(__MODULE__, "SingleAsset#{System.unique_integer([:positive])}")

    source = """
    defmodule #{inspect(module_name)} do
      use Favn.Namespace, connection: :warehouse, catalog: :raw, schema: :sales
      use Favn.Asset

      @doc "Extract raw orders"
      @meta owner: "data-platform", category: :sales, tags: [:raw]
      @depends {Favn.AssetsTest.Upstream, :source_rows}
      @window Favn.Window.daily(lookback: 2)
      @relation true
      def asset(ctx) do
        _target = ctx.asset.relation
        :ok
      end
    end
    """

    assert Enum.any?(Code.compile_string(source, "test/dynamic_assets_test.exs"), fn {mod, _} ->
             mod == module_name
           end)

    assert {:ok, [%Asset{} = asset]} = Compiler.compile_module_assets(module_name)
    expected_name = module_name |> Module.split() |> List.last() |> Macro.underscore()

    assert asset.ref == {module_name, :asset}
    assert asset.depends_on == [{Favn.AssetsTest.Upstream, :source_rows}]

    assert asset.window_spec == %Favn.Window.Spec{kind: :day, lookback: 2, timezone: "Etc/UTC"}

    assert asset.meta == %{owner: "data-platform", category: :sales, tags: [:raw]}

    assert asset.relation == %RelationRef{
             connection: :warehouse,
             catalog: "raw",
             schema: "sales",
             name: expected_name
           }
  end

  test "single-asset module infers relation name from module leaf" do
    module_name = Module.concat(__MODULE__, "FctOrders#{System.unique_integer([:positive])}")

    source = """
    defmodule #{inspect(module_name)} do
      use Favn.Asset

      @relation true
      def asset(_ctx), do: :ok
    end
    """

    assert Enum.any?(Code.compile_string(source, "test/dynamic_assets_test.exs"), fn {mod, _} ->
             mod == module_name
           end)

    assert {:ok, [%Asset{relation: %RelationRef{name: name}}]} =
             Compiler.compile_module_assets(module_name)

    assert name =~ ~r/^fct_orders\d+$/
  end

  test "single-asset module supports tuple and module depends refs" do
    upstream = Module.concat(__MODULE__, "Upstream#{System.unique_integer([:positive])}")
    module_name = Module.concat(__MODULE__, "Depends#{System.unique_integer([:positive])}")

    Code.compile_string(
      """
      defmodule #{inspect(upstream)} do
        use Favn.Asset

        def asset(_ctx), do: :ok
      end
      """,
      "test/dynamic_assets_test.exs"
    )

    Code.compile_string(
      """
      defmodule #{inspect(module_name)} do
        use Favn.Asset

        @depends #{inspect(upstream)}
        @depends {#{inspect(Favn.AssetsTest.Upstream)}, :source_rows}
        def asset(_ctx), do: :ok
      end
      """,
      "test/dynamic_assets_test.exs"
    )

    assert {:ok, [%Asset{depends_on: depends_on}]} = Compiler.compile_module_assets(module_name)

    assert depends_on == [
             {upstream, :asset},
             {Favn.AssetsTest.Upstream, :source_rows}
           ]
  end

  test "single-asset module rejects module shorthand for multi-asset modules" do
    module_name = Module.concat(__MODULE__, "BadDepends#{System.unique_integer([:positive])}")

    Code.compile_string(
      """
      defmodule #{inspect(module_name)} do
        use Favn.Asset

        @depends #{inspect(Favn.AssetsTest.Upstream)}
        def asset(_ctx), do: :ok
      end
      """,
      "test/dynamic_assets_test.exs"
    )

    assert {:error, {:invalid_compiled_assets, message}} =
             Compiler.compile_module_assets(module_name)

    assert message =~ "module shorthand requires a single-asset module"
  end

  test "resolves namespace inheritance from separately compiled ancestor modules" do
    root = Module.concat(__MODULE__, "Root#{System.unique_integer([:positive])}")
    raw = Module.concat(root, Raw)
    sales = Module.concat(raw, Sales)
    assets = Module.concat(sales, Assets)

    Code.compile_string(
      "defmodule #{inspect(root)} do\n  use Favn.Namespace, connection: :warehouse\nend",
      "test/dynamic_assets_test.exs"
    )

    Code.compile_string(
      "defmodule #{inspect(raw)} do\n  use Favn.Namespace, catalog: :raw\nend",
      "test/dynamic_assets_test.exs"
    )

    Code.compile_string(
      "defmodule #{inspect(sales)} do\n  use Favn.Namespace, schema: :sales\nend",
      "test/dynamic_assets_test.exs"
    )

    Code.compile_string(
      """
      defmodule #{inspect(assets)} do
        use Favn.Assets

        @asset true
        @relation true
        def orders(_ctx), do: :ok
      end
      """,
      "test/dynamic_assets_test.exs"
    )

    assert {:ok, [%Asset{relation: %RelationRef{} = produces}]} =
             Compiler.compile_module_assets(assets)

    assert produces == %RelationRef{
             connection: :warehouse,
             catalog: "raw",
             schema: "sales",
             name: "orders"
           }
  end

  test "produced relation inheritance is compile-order independent" do
    root = Module.concat(__MODULE__, "OrderRoot#{System.unique_integer([:positive])}")
    raw = Module.concat(root, Raw)
    sales = Module.concat(raw, Sales)
    assets = Module.concat(sales, Assets)

    Code.compile_string(
      """
      defmodule #{inspect(assets)} do
        use Favn.Assets

        @asset true
        @relation true
        def orders(_ctx), do: :ok
      end
      """,
      "test/dynamic_assets_test.exs"
    )

    Code.compile_string(
      "defmodule #{inspect(root)} do\n  use Favn.Namespace, connection: :warehouse\nend",
      "test/dynamic_assets_test.exs"
    )

    Code.compile_string(
      "defmodule #{inspect(raw)} do\n  use Favn.Namespace, catalog: :raw\nend",
      "test/dynamic_assets_test.exs"
    )

    Code.compile_string(
      "defmodule #{inspect(sales)} do\n  use Favn.Namespace, schema: :sales\nend",
      "test/dynamic_assets_test.exs"
    )

    assert {:ok, [%Asset{relation: %RelationRef{} = produces}]} =
             Compiler.compile_module_assets(assets)

    assert produces == %RelationRef{
             connection: :warehouse,
             catalog: "raw",
             schema: "sales",
             name: "orders"
           }
  end

  test "rejects invalid asset declarations at compile time" do
    assert_raise CompileError, ~r/@asset must be true or a display-name string/, fn ->
      compile_test_module("""
      use Favn.Assets

      @asset depends_on: [:legacy]
      def bad_asset(_ctx), do: :ok
      """)
    end

    assert_raise CompileError, ~r/invalid @depends entry/, fn ->
      compile_test_module("""
      use Favn.Assets

      @asset true
      @depends [:bad]
      def bad_depends(_ctx), do: :ok
      """)
    end

    assert_raise CompileError, ~r/asset meta must be a keyword list or map/, fn ->
      compile_test_module("""
      use Favn.Assets

      @asset true
      @meta :bad
      def bad_meta(_ctx), do: :ok
      """)
    end

    assert_raise CompileError, ~r/asset meta contains unsupported key/, fn ->
      compile_test_module("""
      use Favn.Assets

      @asset true
      @meta kind: :legacy
      def bad_meta_key(_ctx), do: :ok
      """)
    end

    assert_raise CompileError, ~r/asset meta owner must be a string/, fn ->
      compile_test_module("""
      use Favn.Assets

      @asset true
      @meta owner: :team
      def bad_owner(_ctx), do: :ok
      """)
    end

    assert_raise CompileError, ~r/asset meta category must be an atom/, fn ->
      compile_test_module("""
      use Favn.Assets

      @asset true
      @meta category: \"sales\"
      def bad_category(_ctx), do: :ok
      """)
    end

    assert_raise CompileError, ~r/asset meta tags entries must be atoms or strings/, fn ->
      compile_test_module("""
      use Favn.Assets

      @asset true
      @meta tags: [:ok, 123]
      def bad_tag_entry(_ctx), do: :ok
      """)
    end

    assert_raise CompileError, ~r/duplicate asset name/, fn ->
      compile_test_module("""
      use Favn.Assets

      @asset true
      def duplicate(_ctx), do: :ok

      @asset true
      def duplicate(_ctx), do: :ok
      """)
    end

    assert_raise CompileError, ~r/@asset functions must have arity 1/, fn ->
      compile_test_module("""
      use Favn.Assets

      @asset true
      def wrong_arity(_ctx, _deps), do: :ok
      """)
    end

    assert_raise CompileError, ~r/requires @asset immediately above/, fn ->
      compile_test_module("""
      use Favn.Assets

      @window Favn.Window.daily()
      @depends :upstream
      def helper(_ctx), do: :ok
      """)
    end

    assert_raise CompileError, ~r/requires @asset immediately above/, fn ->
      compile_test_module("""
      use Favn.Assets

      @window Favn.Window.daily()
      @meta owner: \"data\"
      def helper(_ctx), do: :ok
      """)
    end

    assert_raise CompileError,
                 ~r/must be attached to an immediately following @asset function/,
                 fn ->
                   compile_test_module("""
                   use Favn.Assets

                   @depends :upstream
                   @meta owner: \"data\"
                   @window Favn.Window.daily()
                   """)
                 end

    assert_raise CompileError, ~r/invalid @window value/, fn ->
      compile_test_module("""
      use Favn.Assets

      @asset true
      @window :day
      def bad_window(_ctx), do: :ok
      """)
    end

    assert_raise CompileError, ~r/multiple @window attributes are not allowed/, fn ->
      compile_test_module("""
      use Favn.Assets

      @asset true
      @window Favn.Window.daily()
      @window Favn.Window.hourly()
      def too_many_windows(_ctx), do: :ok
      """)
    end

    assert_raise CompileError, ~r/invalid @relation value/, fn ->
      compile_test_module("""
      use Favn.Assets

      @asset true
      @relation :bad
      def bad_produces(_ctx), do: :ok
      """)
    end

    assert_raise CompileError, ~r/multiple @relation attributes are not allowed/, fn ->
      compile_test_module("""
      use Favn.Assets

      @asset true
      @relation true
      @relation name: :orders
      def duplicate_produces(_ctx), do: :ok
      """)
    end

    duplicate_module =
      Module.concat(__MODULE__, "DuplicateProduces#{System.unique_integer([:positive])}")

    assert Enum.any?(
             Code.compile_string(
               """
               defmodule #{inspect(duplicate_module)} do
                 use Favn.Namespace, connection: :warehouse, catalog: :raw, schema: :sales
                 use Favn.Assets

                 @asset true
                 @relation true
                 def orders(_ctx), do: :ok

                 @asset true
                 @relation name: :orders
                 def duplicate_orders(_ctx), do: :ok
               end
               """,
               "test/dynamic_assets_test.exs"
             ),
             fn {mod, _} -> mod == duplicate_module end
           )

    assert {:error, {:invalid_compiled_assets, message}} =
             Compiler.compile_module_assets(duplicate_module)

    assert message =~ "duplicate relation"

    assert_raise CompileError, ~r/requires @asset immediately above/, fn ->
      compile_test_module("""
      use Favn.Assets

      @relation true
      def helper(_ctx), do: :ok
      """)
    end

    duplicate_catalog_module =
      Module.concat(__MODULE__, "DuplicateCatalog#{System.unique_integer([:positive])}")

    assert Enum.any?(
             Code.compile_string(
               """
               defmodule #{inspect(duplicate_catalog_module)} do
                 use Favn.Assets

                 @asset true
                 @relation %{"database" => "raw", :catalog => "silver", "name" => "orders"}
                 def duplicate_catalog_keys(_ctx), do: :ok
               end
               """,
               "test/dynamic_assets_test.exs"
             ),
             fn {mod, _} -> mod == duplicate_catalog_module end
           )

    assert {:error, {:invalid_compiled_assets, message}} =
             Compiler.compile_module_assets(duplicate_catalog_module)

    assert message =~ "duplicate values for canonical key :catalog"

    duplicate_name_module =
      Module.concat(__MODULE__, "DuplicateName#{System.unique_integer([:positive])}")

    assert Enum.any?(
             Code.compile_string(
               """
               defmodule #{inspect(duplicate_name_module)} do
                 use Favn.Assets

                 @asset true
                 @relation %{"table" => "orders", :name => "other_orders"}
                 def duplicate_name_keys(_ctx), do: :ok
               end
               """,
               "test/dynamic_assets_test.exs"
             ),
             fn {mod, _} -> mod == duplicate_name_module end
           )

    assert {:error, {:invalid_compiled_assets, message}} =
             Compiler.compile_module_assets(duplicate_name_module)

    assert message =~ "duplicate values for canonical key :name"

    assert_raise CompileError, ~r/must define exactly one public asset\/1 function/, fn ->
      compile_single_asset_module("""
      use Favn.Asset
      """)
    end

    assert_raise CompileError, ~r/requires exactly one public asset\/1 function/, fn ->
      compile_single_asset_module("""
      use Favn.Asset

      def asset(_ctx, _opts), do: :ok
      """)
    end

    assert_raise CompileError, ~r/can define only one asset\/1 function/, fn ->
      compile_single_asset_module("""
      use Favn.Asset

      def asset(_ctx), do: :ok
      def asset(_ctx), do: :ok
      """)
    end

    assert_raise CompileError, ~r/requires a public def asset\(ctx\)/, fn ->
      compile_single_asset_module("""
      use Favn.Asset

      defp asset(_ctx), do: :ok
      """)
    end

    assert_raise CompileError, ~r/requires def asset\(ctx\) immediately below/, fn ->
      compile_single_asset_module("""
      use Favn.Asset

      @meta owner: "data"
      def helper(_ctx), do: :ok

      def asset(_ctx), do: :ok
      """)
    end
  end

  test "consumes @depends and @meta only for the intended asset" do
    module_name = Module.concat(__MODULE__, "Consume#{System.unique_integer([:positive])}")

    source = """
    defmodule #{inspect(module_name)} do
      use Favn.Assets

      @asset true
      @depends :a
      @meta owner: "one", category: :core, tags: [:a]
      def first(_ctx), do: :ok

      @asset true
      def a(_ctx), do: :ok

      @asset true
      def second(_ctx), do: :ok
    end
    """

    assert Enum.any?(Code.compile_string(source, "test/dynamic_assets_test.exs"), fn {mod, _} ->
             mod == module_name
           end)

    [first, a, second] = module_name.__favn_assets__()

    assert first.depends_on == [{module_name, :a}]
    assert first.meta == %{owner: "one", category: :core, tags: [:a]}
    assert a.depends_on == []
    assert a.meta == %{}
    assert second.depends_on == []
    assert second.meta == %{}
  end

  test "asset compiler seam supports non-Elixir frontends compiling into canonical assets" do
    module_name = Module.concat(__MODULE__, "SQLLike#{System.unique_integer([:positive])}")

    source = """
    defmodule #{inspect(module_name)} do
      def __favn_asset_compiler__, do: #{inspect(SQLLikeCompiler)}
    end
    """

    assert Enum.any?(Code.compile_string(source, "test/dynamic_assets_test.exs"), fn {mod, _} ->
             mod == module_name
           end)

    assert Favn.asset_module?(module_name)

    assert {:ok, [%Favn.Asset{name: :compiled_sql_asset, module: ^module_name}]} =
             Favn.list_assets(module_name)

    assert {:ok, catalog} = Favn.Assets.Registry.build_catalog([module_name])
    assert [%Favn.Asset{name: :compiled_sql_asset, module: ^module_name}] = catalog.assets
  end

  test "asset compiler seam rejects invalid canonical produced relation shapes" do
    module_name = Module.concat(__MODULE__, "BadCompiler#{System.unique_integer([:positive])}")

    source = """
    defmodule #{inspect(module_name)} do
      def __favn_asset_compiler__, do: #{inspect(InvalidCompiledProducesCompiler)}
    end
    """

    assert Enum.any?(Code.compile_string(source, "test/dynamic_assets_test.exs"), fn {mod, _} ->
             mod == module_name
           end)

    assert {:error, {:invalid_compiled_assets, _message}} =
             Compiler.compile_module_assets(module_name)
  end

  test "supports string-key produces maps without name inference collisions" do
    module_name = Module.concat(__MODULE__, "StringKeys#{System.unique_integer([:positive])}")

    Code.compile_string(
      """
      defmodule #{inspect(module_name)} do
        use Favn.Assets

        @asset true
        @relation %{"name" => "orders"}
        def ignored_name_inference(_ctx), do: :ok
      end
      """,
      "test/dynamic_assets_test.exs"
    )

    assert {:ok, [%Asset{relation: %RelationRef{name: "orders"} = produces}]} =
             Compiler.compile_module_assets(module_name)

    assert produces.catalog == nil
    assert produces.schema == nil
  end

  defp compile_test_module(body) do
    module_name = Module.concat(__MODULE__, "Dynamic#{System.unique_integer([:positive])}")

    source = """
    defmodule #{inspect(module_name)} do
    #{indent(body, 2)}
    end
    """

    Code.compile_string(source, "test/dynamic_assets_test.exs")
  end

  defp compile_single_asset_module(body) do
    module_name = Module.concat(__MODULE__, "SingleDynamic#{System.unique_integer([:positive])}")

    source = """
    defmodule #{inspect(module_name)} do
    #{indent(body, 2)}
    end
    """

    Code.compile_string(source, "test/dynamic_assets_test.exs")
  end

  defp indent(string, spaces) do
    padding = String.duplicate(" ", spaces)

    string
    |> String.trim_trailing()
    |> String.split("\n")
    |> Enum.map_join("\n", &(padding <> &1))
  end
end
