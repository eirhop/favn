# favn_duckdb

Purpose: DuckDB SQL adapter and runner plugin runtime, including in-process and
separate-process execution modes, DuckDB/DuckLake bootstrap behavior, and
adapter-owned safe relation inspection queries for row counts, samples, and table
metadata.

Code:
- `apps/favn_duckdb/lib/favn/sql/adapter/duckdb*.ex`
- `apps/favn_duckdb/lib/favn_duckdb/`

Tests:
- `apps/favn_duckdb/test/`

Use when changing DuckDB adapter queries/materialization, safe inspection query
generation, bootstrap diagnostics, runtime placement, worker lifecycle, or DuckDB
plugin child specs.
