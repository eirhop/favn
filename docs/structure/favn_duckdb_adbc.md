# favn_duckdb_adbc

Purpose: DuckDB SQL adapter and runner plugin runtime backed by Arrow Database
Connectivity for deployments that need explicit DuckDB shared-library/driver
control, including bounded query result conversion, DuckDB/DuckLake bootstrap
behavior, production local-file validation, ADBC bulk-insert materialization, and
adapter-owned safe relation inspection queries.

Code:
- `apps/favn_duckdb_adbc/lib/favn/sql/adapter/duckdb/adbc*.ex`
- `apps/favn_duckdb_adbc/lib/favn_duckdb_adbc/`

Tests:
- `apps/favn_duckdb_adbc/test/`
- `apps/favn_duckdb_adbc/test/support/` for app-local ADBC fake clients,
  process-local event recording, and test-only client modes

Use when changing the DuckDB ADBC adapter, result bounds, bootstrap
diagnostics, ADBC client integration, or plugin child specs. Update test support
when the ADBC client boundary or shared adapter test instrumentation changes.
