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
           window_spec: Favn.Window.daily()
         }
       ]}
    end
  end

  test "captures canonical asset metadata in source order" do
    assets = Favn.AssetsTest.Sample.__favn_assets__()

    assert Enum.map(assets, & &1.name) == [:extract_orders, :normalize_orders, :fact_sales]
    assert [%Asset{} = extract, %Asset{} = normalize, %Asset{} = fact] = assets

    assert extract.ref == {Favn.AssetsTest.Sample, :extract_orders}
    assert extract.arity == 1
    assert extract.title == "Extract Orders"
    assert extract.doc == "Extract raw orders"
    assert extract.meta == %{}
    assert extract.depends_on == []

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

    [{^module_name, _}] = Code.compile_string(source, "test/dynamic_assets_test.exs")
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

    [{^module_name, _}] = Code.compile_string(source, "test/dynamic_assets_test.exs")

    assert Favn.asset_module?(module_name)

    assert {:ok, [%Favn.Asset{name: :compiled_sql_asset, module: ^module_name}]} =
             Favn.list_assets(module_name)

    assert {:ok, catalog} = Favn.Assets.Registry.build_catalog([module_name])
    assert [%Favn.Asset{name: :compiled_sql_asset, module: ^module_name}] = catalog.assets
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

  defp indent(string, spaces) do
    padding = String.duplicate(" ", spaces)

    string
    |> String.trim_trailing()
    |> String.split("\n")
    |> Enum.map_join("\n", &(padding <> &1))
  end
end
