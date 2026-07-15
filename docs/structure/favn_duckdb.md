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
- `apps/favn_duckdb/test/support/` for app-local DuckDB fake clients,
  process-local event recording, and test-only client modes

Use when changing DuckDB adapter queries/materialization, safe inspection query
generation, native session-script execution, scoped catalog/resource behavior,
runtime placement, worker lifecycle, or DuckDB plugin child specs. Update test
support when the DuckDB client boundary or shared adapter test instrumentation
changes.

DuckDB connection config is adapter-owned. The public runtime shape uses
`open: [...]` for the session database, default-on connection-level pooling for
runner-local warm session reuse, and native SQL files through
`duckdb.startup/resources/catalogs`. The SQL owns extension, setting, secret,
attach, and catalog-selection syntax; catalog entries retain only resource and
write-admission metadata. Disable pooling with
`pool: [enabled: false]`; tune it with `pool: [enabled: true,
max_idle_per_key: 1, idle_timeout_ms: 300_000]`. Pool reuse is safe only for
matching connection/config hash, required catalog/resource sets, script content
and parameter fingerprints, and adapter fingerprint; checked-out sessions are
exclusive to one asset execution. Pooling must not bypass catalog/write
admission or retry unknown-outcome writes.
Raw write admission is SQL-runtime owned: callers provide explicit
`admission: [...]` operation catalog targets or rely on the session
`required_catalogs` scope; the adapter does not parse arbitrary SQL text to infer
catalogs.
Scripts are trusted deployment code. They must be bounded, idempotent,
retry-safe session preparation without durable business writes or external side
effects. The runtime validates file locators and exact value parameters but does
not interpret arbitrary DuckDB SQL. Operators set an explicit catalog
`write_scope` when aliases or named connections share backend capacity.
DuckLake catalogs backed by PostgreSQL metadata can use multiple PostgreSQL
backend connections per concurrent DuckLake writer; observed deployments used
about three. Size DuckLake `write_concurrency` with that multiplier and leave
headroom for admin tools, migrations, monitoring, and other application traffic.
