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
Raw write admission is SQL-runtime owned: callers provide explicit
`admission: [...]` operation catalog targets or rely on the session
`required_catalogs` scope; the adapter does not parse arbitrary SQL text to infer
catalogs.
Local DuckLake attachments may use
`metadata: "ducklake:sqlite:/absolute/path/catalog.sqlite"` without
`meta_secret`. PostgreSQL metadata attachments still require a known PostgreSQL
secret, and both metadata and data paths are validated before bootstrap.
Catalog aliases and named connections that resolve to the same SQLite metadata
file share one write-admission scope.
DuckLake catalogs backed by PostgreSQL metadata can use multiple PostgreSQL
backend connections per concurrent DuckLake writer; observed deployments used
about three. Size DuckLake `write_concurrency` with that multiplier and leave
headroom for admin tools, migrations, monitoring, and other application traffic.
DuckDB ADBC bootstrap validates and emits `duckdb.settings` before secrets and
`ATTACH`, including `threads` and DuckDB Postgres extension pool settings such as
`pg_pool_max_connections`, `pg_pool_acquire_mode`,
`pg_pool_enable_thread_local_cache`, timeout settings, and the reaper-thread flag.
For DuckLake-on-Postgres deployments, prefer `pg_pool_acquire_mode: :wait`, a
finite per-attached-database `pg_pool_max_connections`, and
`pg_pool_enable_thread_local_cache: false`. The deprecated `pg_connection_limit`
setting is not supported. Capacity planning must account for Favn execution
concurrency, DuckLake catalog `write_concurrency`, DuckDB `threads`, number of
Postgres-backed attaches, per-catalog DuckDB Postgres pool limits, and the
metadata database's usable connection slots.
