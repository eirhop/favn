defmodule FavnAuthoring.SemanticDSLTest do
  use ExUnit.Case, async: false

  defmodule Sales do
    use Favn.SQLAsset
    relation(connection: :warehouse, schema: "mart", name: "sales")
    materialized(:table)

    contract do
      grain(by: [:id])
      column(:id, :integer, null: false)
      column(:sale_date, :date, null: false)
      column(:gross, :decimal, null: false)
      column(:discount, :decimal, null: false)
      column(:units, :integer, null: false)
    end

    semantic :sales do
      time(:sale_date, grain: :day, timezone: "Europe/Oslo")

      metric revenue(gross, discount),
        unit: {:currency, "NOK"},
        time_aggregate: :aggregate,
        description: "Revenue after discounts" do
        ~SQL"SUM(@gross - @discount)"
      end

      metric average_price(gross, discount, units),
        unit: {:custom, "NOK/unit"},
        description: "Revenue per unit" do
        ~SQL"revenue(@gross, @discount) / NULLIF(unit_count(@units), 0)"
      end

      metric unit_count(units),
        unit: :count,
        time_aggregate: :aggregate,
        description: "Units sold" do
        ~SQL"SUM(@units)"
      end
    end

    query do
      ~SQL"select 1 as id, DATE '2026-01-01' as sale_date, 100::DECIMAL as gross, 10::DECIMAL as discount, 3 as units"
    end

    def ordinary_helper, do: :retained
  end

  test "captures same-file semantics separately from execution declarations" do
    assert %{name: :sales, metrics: [revenue, _, _]} = Sales.__favn_semantic__()
    assert revenue.args == [:gross, :discount]
    refute Map.has_key?(hd(Sales.__favn_assets_raw__()), :semantic)
    refute Map.has_key?(hd(Sales.__favn_assets_raw__()), :metrics)
    assert Sales.__favn_sql_asset_definition__().contract.columns != []
    assert Sales.ordinary_helper() == :retained
  end

  test "declaration timing and malformed options are rejected" do
    for {body, message} <- [
          {"semantic :sales do end\n" <> contract(), "preceding output contract"},
          {contract() <> "query do ~SQL\"select 1 as id\" end\nsemantic :sales do end",
           "before query"},
          {contract() <> "semantic :sales do end\nsemantic :other do end", "only one"},
          {contract() <> "semantic :sales do source Other end", "accepts only"},
          {contract() <>
             "semantic :sales do metric bad(id), unit: :count, nonsense: true do ~SQL\"SUM(@id)\" end end",
           "unknown or duplicate"}
        ] do
      assert_raise CompileError, ~r/#{message}/, fn -> compile_source(body) end
    end
  end

  test "file formulas retain external-resource tracking and recapture file-only changes" do
    directory =
      Path.join(System.tmp_dir!(), "favn-semantic-#{System.unique_integer([:positive])}")

    File.mkdir_p!(directory)
    on_exit(fn -> File.rm_rf!(directory) end)
    file = Path.join(directory, "metric.sql")
    source_file = Path.join(directory, "asset.ex")

    body =
      contract() <>
        """
        semantic :file_sales do
          metric count(id), file: "metric.sql", unit: :count,
            time_aggregate: :aggregate, description: "Count"
        end
        query do ~SQL"select 1 as id" end
        """

    File.write!(file, "COUNT(@id)")
    module = compile_source(body, source_file)
    assert hd(module.__favn_semantic__().metrics).sql == "COUNT(@id)"
    assert file in module.__info__(:attributes)[:external_resource]
    File.write!(file, "SUM(@id)")
    other = compile_source(body, source_file)
    assert hd(other.__favn_semantic__().metrics).sql == "SUM(@id)"
    File.rm!(file)

    assert_raise CompileError, ~r/cannot read metric SQL file/, fn ->
      compile_source(body, source_file)
    end
  end

  defp contract, do: "contract do\n column :id, :integer, null: false\nend\n"

  defp compile_source(body, file \\ "semantic_test.ex") do
    module = Module.concat(__MODULE__, "Fixture#{System.unique_integer([:positive])}")

    source = """
    defmodule #{inspect(module)} do
      use Favn.SQLAsset
      relation connection: :warehouse, schema: "mart", name: "test"
      materialized :table
      #{body}
    end
    """

    Code.compile_string(source, file)

    on_exit(fn ->
      :code.purge(module)
      :code.delete(module)
    end)

    module
  end
end
