defmodule Favn.AssetsTest.Upstream do
  use Favn.Assets

  @doc "Load source rows"
  @asset true
  def source_rows(_ctx), do: :ok
end

defmodule Favn.AssetsTest.Sample do
  use Favn.Assets

  alias Favn.AssetsTest.Upstream

  @doc "Extract raw orders"
  @asset "Extract Orders"
  def extract_orders(_ctx), do: :ok

  @doc "Normalize extracted orders"
  @asset true
  @depends :extract_orders
  @meta tags: [:sales, "warehouse"], owner: "data"
  def normalize_orders(_ctx), do: :ok

  @doc false
  @asset true
  @depends {Upstream, :source_rows}
  @meta kind: :view
  def fact_sales(_ctx), do: :ok
end

defmodule Favn.AssetsTest do
  use ExUnit.Case, async: true

  alias Favn.Asset

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

    assert fact.doc == nil
    assert fact.meta == %{kind: :view}
    assert fact.depends_on == [{Favn.AssetsTest.Upstream, :source_rows}]
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

    assert_raise CompileError, ~r/@meta must be a keyword list/, fn ->
      compile_test_module("""
      use Favn.Assets

      @asset true
      @meta :bad
      def bad_meta(_ctx), do: :ok
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
