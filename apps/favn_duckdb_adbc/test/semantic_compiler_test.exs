defmodule FavnDuckdbADBC.SemanticCompilerTest do
  use ExUnit.Case, async: false

  alias Favn.Connection.Resolved
  alias Favn.SQL.Adapter.DuckDB.ADBC
  alias FavnDuckdbADBC.SemanticCompiler
  alias FavnDuckdbADBC.SemanticCompiler.Grammar

  @moduletag :adbc_integration
  @inputs [
    %{name: "gross", type: :decimal, nullable: false},
    %{name: "discount", type: :decimal, nullable: false},
    %{name: "units", type: :integer, nullable: false},
    %{name: "category", type: :string, nullable: true}
  ]

  test "consumer macros preserve native join/filter plans and weighted ratios" do
    resolved = %Resolved{
      name: :analytics,
      adapter: ADBC,
      module: __MODULE__,
      config: %{open: [database: ":memory:"]}
    }

    driver = System.fetch_env!("DUCKDB_ADBC_DRIVER")

    assert {:ok, conn} =
             ADBC.connect(resolved, duckdb_adbc: [driver: driver, entrypoint: "duckdb_adbc_init"])

    try do
      for sql <- [
            "CREATE SCHEMA metrics_v1",
            "CREATE MACRO metrics_v1.sales_net_revenue(gross, discount) AS SUM(gross - discount)",
            "CREATE MACRO metrics_v1.sales_units_sold(units) AS SUM(units)",
            "CREATE MACRO metrics_v1.sales_average_unit_price(gross, discount, units) AS " <>
              "metrics_v1.sales_net_revenue(gross, discount) / NULLIF(metrics_v1.sales_units_sold(units), 0)"
          ] do
        assert {:ok, _} = ADBC.execute(conn, sql, [])
      end

      sql = """
      SELECT store.region,
        metrics_v1.sales_net_revenue(sales.gross, sales.discount) AS revenue,
        metrics_v1.sales_average_unit_price(sales.gross, sales.discount, sales.units) AS price
      FROM (VALUES (1, 100, 10, 3, DATE '2026-01-03'),
                   (1, 140, 20, 2, DATE '2026-01-04'),
                   (1, 999, 0, 1, DATE '2025-12-31')) AS sales(store_id, gross, discount, units, sale_date)
      JOIN (VALUES (1, 'North')) AS store(store_id, region) USING (store_id)
      WHERE sales.sale_date >= DATE '2026-01-01' AND sales.sale_date < DATE '2026-02-01'
      GROUP BY store.region
      """

      assert {:ok, %{rows: [%{"region" => "North", "revenue" => revenue, "price" => price}]}} =
               ADBC.query(conn, sql, [])

      assert to_string(revenue) in ["210", "210.00"]
      assert to_string(price) in ["42", "42.0", "42.00"]

      plan = explain(conn, sql)

      assert plan =~ "HASH_JOIN"
      assert plan =~ "HASH_GROUP_BY"
      assert plan =~ "FILTER"
      refute plan =~ "sales_net_revenue"
      refute plan =~ "sales_average_unit_price"

      assert {:ok, _} =
               ADBC.execute(
                 conn,
                 "CREATE TABLE sales_physical(store_id INTEGER, gross DECIMAL(18,2), " <>
                   "discount DECIMAL(18,2), units INTEGER, sale_date DATE, " <>
                   "unused_note VARCHAR, unused_payload BLOB)",
                 []
               )

      assert {:ok, _} =
               ADBC.execute(
                 conn,
                 "INSERT INTO sales_physical VALUES " <>
                   "(1, 100, 10, 3, DATE '2026-01-03', 'unused', 'payload'), " <>
                   "(1, 140, 20, 2, DATE '2026-01-04', 'unused', 'payload'), " <>
                   "(1, 999, 0, 1, DATE '2025-12-31', 'unused', 'payload')",
                 []
               )

      macro_sql =
        "SELECT metrics_v1.sales_net_revenue(gross, discount) AS revenue, " <>
          "metrics_v1.sales_average_unit_price(gross, discount, units) AS price " <>
          "FROM sales_physical WHERE sale_date >= DATE '2026-01-01' " <>
          "AND sale_date < DATE '2026-02-01'"

      inline_sql =
        macro_sql
        |> String.replace(
          "metrics_v1.sales_net_revenue(gross, discount)",
          "SUM(gross - discount)"
        )
        |> String.replace(
          "metrics_v1.sales_average_unit_price(gross, discount, units)",
          "SUM(gross - discount) / NULLIF(SUM(units), 0)"
        )

      macro_plan = explain(conn, macro_sql)
      inline_plan = explain(conn, inline_sql)

      assert macro_plan == inline_plan
      assert macro_plan =~ "SEQ_SCAN"
      assert macro_plan =~ "Projections:"
      assert macro_plan =~ "Filters:"
      refute macro_plan =~ "unused_note"
      refute macro_plan =~ "unused_payload"
      refute macro_plan =~ "store_id"

      assert {:ok, %{rows: [%{"revenue" => physical_revenue, "price" => physical_price}]}} =
               ADBC.query(conn, macro_sql, [])

      assert Decimal.equal?(physical_revenue, Decimal.new("210"))
      assert physical_price == 42.0

      assert {:ok,
              %{
                rows: [
                  %{"normal" => normal, "reversed" => reversed, "zero_price" => zero_price}
                ]
              }} =
               ADBC.query(
                 conn,
                 """
                 SELECT
                   metrics_v1.sales_net_revenue(gross, discount) AS normal,
                   metrics_v1.sales_net_revenue(discount, gross) AS reversed,
                   metrics_v1.sales_average_unit_price(gross, discount, 0) AS zero_price
                 FROM sales_physical
                 WHERE sale_date >= DATE '2026-01-01' AND sale_date < DATE '2026-02-01'
                 """,
                 []
               )

      assert Decimal.equal?(normal, Decimal.new("210"))
      assert Decimal.equal?(reversed, Decimal.new("-210"))
      assert is_nil(zero_price)

      for sql <- [
            "SELECT metrics_v1.sales_net_revenue(gross, discount) AS result FROM sales_physical WHERE FALSE",
            "SELECT metrics_v1.sales_net_revenue(gross, discount) AS result FROM (VALUES (NULL::DECIMAL, 10::DECIMAL)) AS t(gross, discount)"
          ] do
        assert {:ok, %{rows: [%{"result" => nil}]}} = ADBC.query(conn, sql, [])
      end

      for {cast, expected} <- [
            {"DECIMAL(18,2)", "DECIMAL(38,2)"},
            {"DECIMAL(38,10)", "DECIMAL(38,10)"}
          ] do
        assert {:ok, %{rows: [%{"column_type" => ^expected}]}} =
                 ADBC.query(conn, "DESCRIBE SELECT SUM(x) FROM (SELECT NULL::#{cast} AS x)", [])
      end

      assert {:ok, _} =
               ADBC.execute(
                 conn,
                 "CREATE MACRO metrics_v1.inventory_units(units) AS SUM(units)",
                 []
               )

      assert {:ok, _} =
               ADBC.execute(
                 conn,
                 "CREATE TABLE inventory(entity VARCHAR, observed_date DATE, units INTEGER)",
                 []
               )

      assert {:ok, _} =
               ADBC.execute(
                 conn,
                 """
                 INSERT INTO inventory VALUES
                   ('B', DATE '2025-12-31', 999),
                   ('A', DATE '2026-01-30', 10), ('A', DATE '2026-01-31', 12),
                   ('B', DATE '2026-01-30', 20),
                   ('A', DATE '2026-02-01', 14), ('A', DATE '2026-02-28', 16),
                   ('C', DATE '2026-02-10', 7), ('A', DATE '2026-03-01', 900)
                 """,
                 []
               )

      assert observed_totals(conn, "DESC", "2026-02-01") == "2026-01:32"
      assert observed_totals(conn, "ASC", "2026-02-01") == "2026-01:30"
      assert observed_totals(conn, "DESC", "2026-03-01") == "2026-01:32|2026-02:23"
      assert observed_totals(conn, "ASC", "2026-03-01") == "2026-01:30|2026-02:21"

      assert {:ok, %{rows: [%{"total" => selected_total}]}} =
               ADBC.query(
                 conn,
                 "SELECT metrics_v1.inventory_units(units) AS total FROM inventory WHERE observed_date = DATE '2026-01-31'",
                 []
               )

      assert Decimal.equal?(selected_total, Decimal.new("12"))

      assert {:ok, %{rows: [%{"total" => nil}]}} =
               ADBC.query(
                 conn,
                 "SELECT metrics_v1.inventory_units(units) AS total FROM inventory WHERE observed_date = DATE '2026-02-02'",
                 []
               )

      assert {:ok, _} =
               ADBC.execute(
                 conn,
                 "INSERT INTO inventory VALUES ('A', DATE '2026-01-31', 50)",
                 []
               )

      assert {:ok, %{rows: [%{"unique_grain" => false}]}} =
               ADBC.query(
                 conn,
                 "SELECT count(*) = 0 AS unique_grain FROM " <>
                   "(SELECT entity, observed_date FROM inventory " <>
                   "GROUP BY entity, observed_date HAVING count(*) > 1)",
                 []
               )

      assert {:ok, %{rows: [%{"ast" => ast}]}} =
               ADBC.query(
                 conn,
                 "SELECT json_serialize_sql('SELECT SUM(1)') AS ast",
                 []
               )

      assert {:ok, [0]} = Grammar.validate(Jason.decode!(ast), [], nil)

      invalid =
        Regex.replace(~r/"type_info"\s*:\s*null/, ast, ~s|"type_info":false|, global: false)

      refute invalid == ast

      assert {:error, :invalid_semantic_expression} =
               Grammar.validate(Jason.decode!(invalid), [], nil)
    after
      ADBC.disconnect(conn, [])
    end
  end

  defp explain(conn, sql) do
    assert {:ok, result} = Adbc.Connection.query(conn.conn_ref, "EXPLAIN " <> sql)
    result |> Adbc.Result.to_map() |> Map.fetch!("explain_value") |> hd()
  end

  defp observed_totals(conn, direction, until) when direction in ["ASC", "DESC"] do
    sql = """
    WITH selected AS (
      SELECT entity, observed_date, units,
        date_trunc('month', observed_date) AS bucket
      FROM inventory
      WHERE observed_date >= DATE '2026-01-01' AND observed_date < DATE '#{until}'
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY entity, date_trunc('month', observed_date)
        ORDER BY observed_date #{direction}
      ) = 1
    ), totals AS (
      SELECT bucket, metrics_v1.inventory_units(units) AS total
      FROM selected GROUP BY bucket
    )
    SELECT string_agg(strftime(bucket, '%Y-%m') || ':' || total::VARCHAR, '|' ORDER BY bucket) AS totals
    FROM totals
    """

    assert {:ok, %{rows: [%{"totals" => totals}]}} = ADBC.query(conn, sql, [])
    totals
  end

  test "validates aggregate expressions with explicit profile provenance" do
    assert {:ok, result} = SemanticCompiler.validate(~s|SUM("gross" - "discount")|, @inputs)
    assert result.native_type == "DECIMAL(38,2)"
    assert result.nullable == :unknown
    assert result.compiler_version == "duckdb-semantic-v1"
    assert result.validation_profile["gross"] == "DECIMAL(18,2)"
    assert result.runtime_version in ["v1.5.2", "v1.5.5"]
  end

  test "accepts SQL null handling, zero denominators, distinct strings, CASE, casts and FILTER" do
    for sql <- [
          ~s|SUM("gross" - "discount") / NULLIF(SUM("units"), 0)|,
          ~s|COUNT(DISTINCT "category")|,
          ~s|COALESCE(SUM("gross"), 0)|,
          ~s|CAST(SUM("gross") AS DECIMAL(38,10))|,
          ~s|SUM(CASE WHEN "units" > 0 AND "category" IS NOT NULL THEN "gross" ELSE 0 END)|,
          ~s|SUM("gross") FILTER (WHERE "units" > 0)|,
          ~s|ROUND(ABS(SUM("gross")), 2)|
        ] do
      assert {:ok, _} = SemanticCompiler.validate(sql, @inputs), sql
    end
  end

  test "native binding cannot establish business provenance for same-type arguments" do
    assert {:ok, _} = SemanticCompiler.validate(~s|SUM("discount" - "gross")|, @inputs)
  end

  test "aggregate origins use exact native byte offsets across SQL lexical forms" do
    inputs = [%{name: "gross", type: :decimal, nullable: false}]

    assert {:ok, %{aggregate_locations: [0]}} =
             SemanticCompiler.validate(~s|SUM("gross")|, inputs)

    assert {:ok, %{aggregate_locations: [1]}} =
             SemanticCompiler.validate(~s|(SUM("gross")) + 1|, inputs,
               allowed_aggregate_locations: [1]
             )

    assert {:error, :invalid_semantic_expression} =
             SemanticCompiler.validate(~s|(SUM("gross")) + 1|, inputs,
               allowed_aggregate_locations: [1, 20]
             )

    assert {:error, :invalid_semantic_expression} =
             SemanticCompiler.validate(~s|SUM("gross")|, inputs, allowed_aggregate_locations: [])

    for suffix <- [
          "sum(1)",
          ~s|"sum"/* comment */(1)|,
          "sum\u00A0(1)",
          "sum\u200B(1)",
          "CASE WHEN E''\n'\\'' = '' THEN 1 ELSE 0 END + sum(1)"
        ] do
      sql = ~s|(SUM("gross")) + | <> suffix
      assert {:ok, %{aggregate_locations: [1, extra]}} = SemanticCompiler.validate(sql, inputs)
      assert extra > 1

      assert {:error, :invalid_semantic_expression} =
               SemanticCompiler.validate(sql, inputs, allowed_aggregate_locations: [1])
    end

    assert {:error, :aggregate_limit} =
             SemanticCompiler.validate(
               ~s|SUM("gross")|,
               inputs,
               allowed_aggregate_locations: Enum.to_list(0..1024)
             )
  end

  test "compiler offsets distinguish generated inputs from otherwise-binding raw references" do
    inputs = [%{name: "gross", type: :decimal, nullable: false, locations: [4]}]
    assert {:ok, _} = SemanticCompiler.validate(~s|SUM("gross")|, inputs)

    assert {:error, :invalid_semantic_expression} =
             SemanticCompiler.validate(~s|SUM("gross" + gross)|, inputs)

    assert {:error, :invalid_semantic_expression} =
             SemanticCompiler.validate(~s|SUM("gross" + "gross")|, inputs)

    sql = ~s|SUM(CASE WHEN 'ø' = 'ø' THEN "gross" ELSE 0 END)|
    {offset, _length} = :binary.match(sql, ~s|"gross"|)
    assert {:ok, _} = SemanticCompiler.validate(sql, [%{hd(inputs) | locations: [offset]}])

    assert {:error, :invalid_semantic_expression} =
             SemanticCompiler.validate(sql, [%{hd(inputs) | locations: [offset - 2]}])
  end

  test "native aggregate evidence has a bounded 1024-location budget" do
    inputs = [%{name: "gross", type: :decimal, nullable: false}]

    assert {:ok, %{aggregate_locations: locations}} =
             SemanticCompiler.validate(balanced_aggregates(1024), inputs)

    assert length(locations) == 1024

    assert {:error, :aggregate_limit} =
             SemanticCompiler.validate(balanced_aggregates(1025), inputs)
  end

  test "maximum escaped input profiles and aggregate evidence fit one bounded receipt" do
    inputs =
      for index <- 0..63 do
        name = <<1 + div(index, 8), 1 + rem(index, 8)>> <> String.duplicate(<<1>>, 126)
        %{name: name, type: :decimal, nullable: false}
      end

    sql = "/*" <> String.duplicate("p", 40_000) <> "*/" <> balanced_constant_aggregates(1024)
    assert byte_size(sql) < 65_536
    assert {:ok, result} = SemanticCompiler.validate(sql, inputs)
    assert length(result.aggregate_locations) == 1024
    assert map_size(result.validation_profile) == 64

    assert Map.keys(result.validation_profile) |> Enum.sort() ==
             Enum.map(inputs, & &1.name) |> Enum.sort()
  end

  defp balanced_constant_aggregates(1), do: "SUM(1)"

  defp balanced_constant_aggregates(count) do
    left = div(count, 2)

    "(" <>
      balanced_constant_aggregates(left) <>
      "+" <>
      balanced_constant_aggregates(count - left) <> ")"
  end

  defp balanced_aggregates(1), do: ~s|SUM("gross")|

  defp balanced_aggregates(count) do
    left = div(count, 2)
    "(" <> balanced_aggregates(left) <> "+" <> balanced_aggregates(count - left) <> ")"
  end

  test "count accepts every supported logical input family" do
    types = [
      :integer,
      :float,
      :decimal,
      :string,
      :boolean,
      :date,
      :datetime,
      :binary,
      :time,
      :json,
      :uuid
    ]

    inputs = Enum.map(types, &%{name: Atom.to_string(&1), type: &1, nullable: true})
    sql = Enum.map_join(inputs, " + ", &~s|COUNT("#{&1.name}")|)
    assert {:ok, result} = SemanticCompiler.validate(sql, inputs)
    assert map_size(result.validation_profile) == length(types)
  end

  test "rejects independent reads and unsupported grammar before binding" do
    for sql <- [
          ~s|SUM("gross") + (SELECT 1)|,
          ~s|SUM("gross") FROM read_csv('/tmp/no-access')|,
          ~s|SUM("gross") OVER ()|,
          ~s|SUM("gross" ORDER BY "units")|,
          ~s|COUNT(*)|,
          ~s|SUM("missing")|,
          ~s|SUM(inputs."gross")|,
          ~s|SUM("gross") + random()|,
          ~s|SUM("gross") + current_setting('threads')|,
          ~s|SUM("gross"); SELECT 1|,
          ~s|SUM("gross"), SUM("discount")|,
          ~s|SUM(SUM("gross"))|,
          ~s|"gross" - "discount"|,
          ~s|SUM("gross") AS renamed|,
          ~s|SUM("gross") + $1|,
          ~s|SUM("gross") + unknown_helper(1)|
        ] do
      assert {:error, _} = SemanticCompiler.validate(sql, @inputs), sql
    end
  end

  test "binding rejects ungrouped columns and incompatible input types" do
    assert {:error, :semantic_bind_failed} =
             SemanticCompiler.validate(~s|SUM("gross") + "discount"|, @inputs)

    assert {:error, :semantic_bind_failed} =
             SemanticCompiler.validate(~s|SUM("category")|, @inputs)
  end

  test "bounds input before spawning and reports unavailable driver explicitly" do
    assert {:error, :invalid_semantic_input} =
             SemanticCompiler.validate(String.duplicate("x", 65_537), @inputs)

    for opts <- [[{:allowed_aggregate_locations}], [{:allowed_aggregate_locations, [], :extra}]] do
      assert {:error, :invalid_semantic_input} =
               SemanticCompiler.validate(~s|SUM("gross")|, @inputs, opts)
    end

    config = Application.get_env(:favn, :duckdb_adbc)

    on_exit(fn ->
      if config,
        do: Application.put_env(:favn, :duckdb_adbc, config),
        else: Application.delete_env(:favn, :duckdb_adbc)
    end)

    Application.put_env(:favn, :duckdb_adbc, driver: "/missing/semantic-driver.so")
    assert {:error, :semantic_driver_unavailable} = SemanticCompiler.validate("SUM(1)", @inputs)
  end
end
