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

Use when changing the DuckDB ADBC adapter, result bounds, bootstrap diagnostics,
scoped catalog attach behavior through `required_catalogs`, ADBC client
integration, or plugin child specs. Update test support when the ADBC client
boundary or shared adapter test instrumentation changes.

The ADBC adapter owns DuckDB config parsing, bootstrap SQL generation, driver
preflight diagnostics, and default-on runner-local session pooling for ADBC-backed
sessions. Disable pooling with `pool: [enabled: false]`; tune it with
`pool: [enabled: true, max_idle_per_key: 1, idle_timeout_ms: 300_000]`. Pooling
reuses only warm sessions whose connection/config hash, required catalog set, and
adapter fingerprint match. Checked-out sessions are exclusive to one asset
execution, and pooling must not increase catalog/write concurrency or retry
unknown-outcome writes. Safe bounded retries are limited to session
creation/bootstrap and read-only inspection/query paths.
DuckLake catalogs backed by PostgreSQL metadata can use multiple PostgreSQL
backend connections per concurrent DuckLake writer; observed deployments used
about three. Size DuckLake `write_concurrency` with that multiplier and leave
headroom for admin tools, migrations, monitoring, and other application traffic.
