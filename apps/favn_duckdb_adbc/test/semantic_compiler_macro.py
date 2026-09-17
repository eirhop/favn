"""Native consumer fixture: aggregate scalar macros expand into a normal plan."""
import ctypes
import importlib.util
from pathlib import Path
import sys

sys.dont_write_bytecode = True

source = Path(__file__).parents[1] / "lib/favn_duckdb_adbc/semantic_compiler/worker.py"
spec = importlib.util.spec_from_file_location("worker", source)
worker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(worker)
db = worker.DuckDB(sys.argv[1])


def execute(sql):
    result = worker.Result()
    try:
        assert db.lib.duckdb_query(db.conn, sql.encode(), ctypes.byref(result)) == 0
    finally:
        db.lib.duckdb_destroy_result(ctypes.byref(result))


try:
    execute("CREATE SCHEMA metrics_v1")
    execute("CREATE MACRO metrics_v1.sales_net_revenue(gross, discount) AS SUM(gross - discount)")
    execute("CREATE MACRO metrics_v1.sales_units_sold(units) AS SUM(units)")
    execute("CREATE MACRO metrics_v1.sales_average_unit_price(gross, discount, units) AS "
            "metrics_v1.sales_net_revenue(gross, discount) / NULLIF(metrics_v1.sales_units_sold(units), 0)")
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
    assert db.query(sql, 0) == "North"
    assert db.query(sql, 1) == "210"
    assert float(db.query(sql, 2)) == 42.0
    plan = db.query("EXPLAIN " + sql, 1)
    assert "HASH_JOIN" in plan and "HASH_GROUP_BY" in plan and "FILTER" in plan
    assert "sales_net_revenue" not in plan and "sales_average_unit_price" not in plan
    # A physical wide table proves normal projection/filter pruning, not only
    # constant-folding of the VALUES fixture above.
    execute("CREATE TABLE sales_physical(store_id INTEGER, gross DECIMAL(18,2), "
            "discount DECIMAL(18,2), units INTEGER, sale_date DATE, "
            "unused_note VARCHAR, unused_payload BLOB)")
    execute("INSERT INTO sales_physical VALUES "
            "(1, 100, 10, 3, DATE '2026-01-03', 'not projected', 'payload'), "
            "(1, 140, 20, 2, DATE '2026-01-04', 'not projected', 'payload'), "
            "(1, 999, 0, 1, DATE '2025-12-31', 'not projected', 'payload')")
    macro_sql = """
    SELECT metrics_v1.sales_net_revenue(gross, discount) AS revenue,
      metrics_v1.sales_average_unit_price(gross, discount, units) AS price
    FROM sales_physical
    WHERE sale_date >= DATE '2026-01-01' AND sale_date < DATE '2026-02-01'
    """
    inline_sql = macro_sql.replace(
        "metrics_v1.sales_net_revenue(gross, discount)", "SUM(gross - discount)"
    ).replace(
        "metrics_v1.sales_average_unit_price(gross, discount, units)",
        "SUM(gross - discount) / NULLIF(SUM(units), 0)"
    )
    macro_plan = db.query("EXPLAIN " + macro_sql, 1)
    assert macro_plan == db.query("EXPLAIN " + inline_sql, 1)
    assert "SEQ_SCAN" in macro_plan and "Projections:" in macro_plan
    assert "Filters:" in macro_plan
    assert "unused_note" not in macro_plan and "unused_payload" not in macro_plan
    assert "store_id" not in macro_plan
    assert float(db.query(macro_sql)) == 210.0
    assert float(db.query(macro_sql, 1)) == 42.0
    assert float(db.query(macro_sql.replace(
        "sales_net_revenue(gross, discount)", "sales_net_revenue(discount, gross)"
    ))) == -210.0  # Same SQL types do not establish business provenance.
    assert db.query("SELECT metrics_v1.sales_net_revenue(gross, discount) IS NULL "
                    "FROM sales_physical WHERE FALSE") == "true"
    assert db.query("SELECT metrics_v1.sales_net_revenue(gross, discount) IS NULL "
                    "FROM (VALUES (NULL::DECIMAL, 10::DECIMAL)) AS t(gross, discount)") == "true"
    assert db.query("SELECT metrics_v1.sales_average_unit_price(gross, discount, units) IS NULL "
                    "FROM (VALUES (100, 10, 0)) AS t(gross, discount, units)") == "true"

    # Consumers select observed rows per entity within the requested interval
    # and bucket before invoking an aggregate-expression macro.
    execute("CREATE MACRO metrics_v1.inventory_units(units) AS SUM(units)")
    execute("CREATE TABLE inventory(entity VARCHAR, observed_date DATE, units INTEGER)")
    execute("INSERT INTO inventory VALUES "
            "('B', DATE '2025-12-31', 999), "
            "('A', DATE '2026-01-30', 10), ('A', DATE '2026-01-31', 12), "
            "('B', DATE '2026-01-30', 20), "
            "('A', DATE '2026-02-01', 14), ('A', DATE '2026-02-28', 16), "
            "('C', DATE '2026-02-10', 7), ('A', DATE '2026-03-01', 900)")

    def observed_totals(direction, until):
        assert direction in ("ASC", "DESC")
        return db.query(f"""
        WITH selected AS (
          SELECT entity, observed_date, units,
            date_trunc('month', observed_date) AS bucket
          FROM inventory
          WHERE observed_date >= DATE '2026-01-01' AND observed_date < DATE '{until}'
          QUALIFY ROW_NUMBER() OVER (
            PARTITION BY entity, date_trunc('month', observed_date)
            ORDER BY observed_date {direction}
          ) = 1
        ), totals AS (
          SELECT bucket, metrics_v1.inventory_units(units) AS total
          FROM selected GROUP BY bucket
        )
        SELECT string_agg(strftime(bucket, '%Y-%m') || ':' || total::VARCHAR, '|' ORDER BY bucket)
        FROM totals
        """)

    assert observed_totals("DESC", "2026-02-01") == "2026-01:32"
    assert observed_totals("ASC", "2026-02-01") == "2026-01:30"
    assert observed_totals("DESC", "2026-03-01") == "2026-01:32|2026-02:23"
    assert observed_totals("ASC", "2026-03-01") == "2026-01:30|2026-02:21"
    assert db.query("SELECT metrics_v1.inventory_units(units) FROM inventory "
                    "WHERE observed_date = DATE '2026-01-31'") == "12"
    assert db.query("SELECT metrics_v1.inventory_units(units) IS NULL FROM inventory "
                    "WHERE observed_date = DATE '2026-02-02'") == "true"
    execute("INSERT INTO inventory VALUES ('A', DATE '2026-01-31', 50)")
    assert db.query("SELECT count(*) = 0 FROM (SELECT entity, observed_date "
                    "FROM inventory GROUP BY entity, observed_date HAVING count(*) > 1)") == "false"

    # Both physical decimals satisfy one logical decimal contract; result precision differs.
    assert db.query("DESCRIBE SELECT SUM(x) FROM (SELECT NULL::DECIMAL(18,2) AS x)", 1) == "DECIMAL(38,2)"
    assert db.query("DESCRIBE SELECT SUM(x) FROM (SELECT NULL::DECIMAL(38,10) AS x)", 1) == "DECIMAL(38,10)"
finally:
    db.close()
print("ok")
