defmodule Flux.AssetsTest.Upstream do
  use Flux.Assets

  @doc "Load source rows"
  @asset true
  def source_rows, do: [%{id: 1, total: 100}]
end

defmodule Flux.AssetsTest.Sample do
  use Flux.Assets

  alias Flux.AssetsTest.Upstream

  @doc "Extract raw orders"
  @asset true
  def extract_orders, do: [%{id: 1, total: 100}]

  @doc "Normalize extracted orders"
  @asset depends_on: [:extract_orders], tags: [:sales, "warehouse"]
  def normalize_orders(orders), do: Enum.map(orders, &Map.put(&1, :normalized, true))

  @doc false
  @asset depends_on: [{Upstream, :source_rows}], kind: :view
  def fact_sales(rows), do: %{rows: rows}
end

defmodule Flux.AssetsTest do
  use ExUnit.Case, async: true

  alias Flux.Asset

  require Logger

  test "captures canonical asset metadata in source order" do
    assets = Flux.AssetsTest.Sample.__flux_assets__()

    Logger.debug("module assets: #{inspect(assets, pretty: true)}")

    assert Enum.map(assets, & &1.name) == [:extract_orders, :normalize_orders, :fact_sales]

    assert [%Asset{} = extract, %Asset{} = normalize, %Asset{} = fact] = assets

    assert extract.ref == {Flux.AssetsTest.Sample, :extract_orders}
    assert extract.arity == 0
    assert extract.doc == "Extract raw orders"
    assert extract.file == "test/assets_test.exs"
    assert is_integer(extract.line)
    assert extract.kind == :materialized
    assert extract.tags == []
    assert extract.depends_on == []

    assert normalize.ref == {Flux.AssetsTest.Sample, :normalize_orders}
    assert normalize.doc == "Normalize extracted orders"
    assert normalize.tags == [:sales, "warehouse"]
    assert normalize.depends_on == [{Flux.AssetsTest.Sample, :extract_orders}]

    assert fact.kind == :view
    assert fact.doc == nil
    assert fact.depends_on == [{Flux.AssetsTest.Upstream, :source_rows}]
  end

  test "rejects invalid asset declarations at compile time" do
    Logger.info("compiling invalid asset declarations to verify compile-time validation")

    assert_raise CompileError, ~r/invalid asset kind/, fn ->
      compile_test_module("""
      use Flux.Assets

      @asset kind: :invalid
      def bad_kind, do: :ok
      """)
    end

    assert_raise CompileError, ~r/asset tags must be a list/, fn ->
      compile_test_module("""
      use Flux.Assets

      @asset tags: :sales
      def bad_tags, do: :ok
      """)
    end

    assert_raise CompileError, ~r/asset tags must be atoms or strings/, fn ->
      compile_test_module("""
      use Flux.Assets

      @asset tags: [:sales, 1]
      def bad_tag_entry, do: :ok
      """)
    end

    assert_raise CompileError, ~r/asset depends_on must be a list/, fn ->
      compile_test_module("""
      use Flux.Assets

      @asset depends_on: :extract_orders
      def bad_depends_on_shape, do: :ok
      """)
    end

    assert_raise CompileError, ~r/invalid depends_on entry/, fn ->
      compile_test_module("""
      use Flux.Assets

      @asset depends_on: [:ok, "bad"]
      def bad_depends_on, do: :ok
      """)
    end

    assert_raise CompileError, ~r/duplicate asset name/, fn ->
      compile_test_module("""
      use Flux.Assets

      @asset true
      def duplicate, do: :ok

      @asset true
      def duplicate(value), do: value
      """)
    end

    assert_raise CompileError, ~r/@asset can only be used on public functions/, fn ->
      compile_test_module("""
      use Flux.Assets

      @asset true
      defp private_asset, do: :ok
      """)
    end

    assert_raise CompileError, ~r/@asset must be followed by a public function definition/, fn ->
      compile_test_module("""
      use Flux.Assets

      @asset true
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
