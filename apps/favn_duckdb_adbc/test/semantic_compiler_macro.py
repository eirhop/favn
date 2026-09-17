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
    # Both physical decimals satisfy one logical decimal contract; result precision differs.
    assert db.query("DESCRIBE SELECT SUM(x) FROM (SELECT NULL::DECIMAL(18,2) AS x)", 1) == "DECIMAL(38,2)"
    assert db.query("DESCRIBE SELECT SUM(x) FROM (SELECT NULL::DECIMAL(38,10) AS x)", 1) == "DECIMAL(38,10)"
finally:
    db.close()
print("ok")
