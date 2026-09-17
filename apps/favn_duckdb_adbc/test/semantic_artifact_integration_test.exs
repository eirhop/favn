defmodule FavnDuckdbADBC.SemanticArtifactIntegrationTest do
  use ExUnit.Case, async: false
  @moduletag :adbc_integration

  alias Favn.Connection.Resolved
  alias Favn.SQL.Adapter.DuckDB.ADBC
  alias Favn.Semantic.{Artifact, Catalog}
  alias FavnAuthoring.Semantic.Builder
  alias FavnDuckdbADBC.SemanticCompiler

  defmodule Sales do
    use Favn.SQLAsset
    relation(connection: :analytics, schema: "mart", name: "fct_sales")
    materialized(:table)

    contract do
      grain(by: [:id])
      column(:id, :integer, null: false)
      column(:sale_date, :date, null: false)
      column(:gross_value, :decimal, null: false)
      column(:discount_value, :decimal, null: false)
      column(:units_sold, :integer, null: false)
    end

    semantic :sales do
      time(:sale_date, grain: :day, timezone: "Europe/Oslo")

      metric net_revenue(gross_value, discount_value),
        unit: {:currency, "NOK"},
        time_aggregate: :aggregate,
        description: "Revenue after discounts" do
        ~SQL"SUM(@gross_value - @discount_value)"
      end

      metric units_sold(units_sold),
        unit: :count,
        time_aggregate: :aggregate,
        description: "Units sold" do
        ~SQL"SUM(@units_sold)"
      end

      metric average_unit_price(gross_value, discount_value, units_sold),
        unit: {:custom, "NOK/unit"},
        description: "Revenue per unit" do
        ~SQL"net_revenue(@gross_value, @discount_value) / NULLIF(units_sold(@units_sold), 0)"
      end
    end

    query do
      ~SQL"select 1 as id, DATE '2026-01-01' as sale_date, 100::DECIMAL as gross_value, 10::DECIMAL as discount_value, 3 as units_sold"
    end
  end

  test "same-file authoring builds and round trips a native-validated independent artifact" do
    assert {:ok, assets} = FavnAuthoring.list_assets([Sales])
    execution = Sales.__favn_assets_raw__()
    assert {:ok, artifact} = Builder.compile(assets, SemanticCompiler)
    assert artifact.semantic_version =~ "sm_"
    assert Sales.__favn_assets_raw__() == execution
    refute Map.has_key?(hd(execution), :semantic)
    refute Map.has_key?(hd(execution), :metrics)

    directory =
      Path.join(System.tmp_dir!(), "favn-semantic-artifact-#{System.unique_integer([:positive])}")

    on_exit(fn -> File.rm_rf!(directory) end)
    assert {:ok, result} = Artifact.write(artifact, directory)
    assert {:ok, decoded} = Artifact.read(result.path)
    assert decoded == artifact
    assert {:ok, ^result} = Artifact.write(artifact, directory)
    inspected = Catalog.inspect(decoded, metric: "sales.net_revenue")
    assert inspect(inspected) =~ "gross_value"
    assert inspect(inspected) =~ "discount_value"
    assert inspect(inspected) =~ "DECIMAL(38,2)"

    :code.purge(Sales)
    :code.delete(Sales)
    refute Code.ensure_loaded?(Sales)
    assert_generated_dashboard(decoded)
  end

  defp assert_generated_dashboard(artifact) do
    resolved = %Resolved{
      name: :analytics,
      adapter: ADBC,
      module: __MODULE__,
      config: %{open: [database: ":memory:"]}
    }

    opts =
      case System.get_env("DUCKDB_ADBC_DRIVER") do
        value when value in [nil, ""] -> []
        driver -> [duckdb_adbc: [driver: driver, entrypoint: "duckdb_adbc_init"]]
      end

    assert {:ok, conn} = ADBC.connect(resolved, opts)

    try do
      namespace = Catalog.namespace(artifact)
      assert {:ok, _} = ADBC.execute(conn, ~s|CREATE SCHEMA "#{namespace}"|, [])

      for sql <- Catalog.macros(artifact) do
        assert {:ok, _} = ADBC.execute(conn, sql, [])
      end

      assert {:ok, _} = ADBC.execute(conn, "CREATE SCHEMA mart", [])

      assert {:ok, _} =
               ADBC.execute(
                 conn,
                 """
                 CREATE TABLE mart.fct_sales AS
                 SELECT * FROM (VALUES
                   (1, DATE '2026-01-01', 100::DECIMAL(18,2), 10::DECIMAL(18,2), 3),
                   (2, DATE '2026-01-02', 140::DECIMAL(18,2), 20::DECIMAL(18,2), 2)
                 ) AS rows(id, sale_date, gross_value, discount_value, units_sold)
                 """,
                 []
               )

      [model] = artifact.models
      revenue = Enum.find(model["metrics"], &(&1["ref"] == "sales.net_revenue"))
      average = Enum.find(model["metrics"], &(&1["ref"] == "sales.average_unit_price"))
      assert Enum.map(revenue["inputs"], & &1["column"]) == ["gross_value", "discount_value"]

      assert Enum.map(average["inputs"], & &1["column"]) == [
               "gross_value",
               "discount_value",
               "units_sold"
             ]

      revenue_call = Catalog.invocation(artifact, revenue, "sales")
      average_call = Catalog.invocation(artifact, average, "sales")
      assert revenue_call =~ ~s|("sales"."gross_value", "sales"."discount_value")|

      assert average_call =~
               ~s|("sales"."gross_value", "sales"."discount_value", "sales"."units_sold")|

      sql = """
      SELECT #{revenue_call} AS net_revenue, #{average_call} AS average_unit_price
      FROM mart.fct_sales AS sales
      WHERE sales.sale_date >= DATE '2026-01-01' AND sales.sale_date < DATE '2026-02-01'
      """

      assert {:ok, %{rows: [row]}} = ADBC.query(conn, sql, [])
      assert Decimal.equal?(row["net_revenue"], Decimal.new("210.00"))
      assert row["average_unit_price"] == 42
    after
      ADBC.disconnect(conn, [])
    end
  end
end
