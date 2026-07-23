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

Use when changing the DuckDB ADBC adapter, result bounds, native session-script
diagnostics, scoped catalog/resource behavior, ADBC client integration, or
plugin child specs. Update test support when the ADBC client
boundary or shared adapter test instrumentation changes.

The ADBC adapter explicitly implements `Favn.SQL.GenerationAdapter` with the
same candidate, atomic-swap, marker-reconciliation, and safe-discard contract as
the in-process DuckDB adapter. Generation operations use the checked-out ADBC
connection and surface commit uncertainty instead of retrying a possible write.

The ADBC adapter owns DuckDB config parsing, native script execution, driver
preflight diagnostics, and default-on runner-local session pooling for ADBC-backed
sessions. Disable pooling with `pool: [enabled: false]`; tune it with
`pool: [enabled: true, max_idle_per_key: 1, idle_timeout_ms: 300_000]`. Pooling
reuses only warm sessions whose connection/config hash, required catalog and
resource sets, script/parameter fingerprints, and adapter fingerprint match.
Checked-out sessions are exclusive to one asset
execution, and pooling must not increase catalog/write concurrency or retry
unknown-outcome writes. Safe bounded retries are limited to session
creation/bootstrap and read-only inspection/query paths.
Raw write admission is SQL-runtime owned: callers provide explicit
`admission: [...]` operation catalog targets or rely on the session
`required_catalogs` scope; the adapter does not parse arbitrary SQL text to infer
catalogs.
The adapter does not generate or allowlist DuckDB setup statements.
`duckdb.startup` and selected named resources are trusted SQL files; catalogs
contain only resource mapping, `write_concurrency`, and optional `write_scope`.
Scripts must be idempotent and retry-safe because partial execution can be
followed by fresh-session retry.
Supported deferred `Favn.RuntimeValue` parameters resolve during session
planning. Secret values are redacted and their hashes participate in pool
identity; provider packages remain outside this adapter. The Azure PostgreSQL
regression path proves a cached Entra token is reused with a warm physical
session, then refreshes and bootstraps a replacement ADBC session after expiry.
The adapter converts Favn `DateTime` SQL parameters into UTC microsecond Arrow
timestamp columns before invoking the ADBC client, preserving the represented
instant for query and execute paths.
DuckLake catalogs backed by PostgreSQL metadata can use multiple PostgreSQL
backend connections per concurrent DuckLake writer; observed deployments used
about three. Size DuckLake `write_concurrency` with that multiplier and leave
headroom for admin tools, migrations, monitoring, and other application traffic.
DuckDB Postgres extension settings are written as native SQL in the scripts.
Capacity planning must still account for Favn execution concurrency, catalog
`write_concurrency`, DuckDB threads, attached Postgres-backed catalogs,
per-catalog DuckDB Postgres pool limits, and usable metadata database slots.
