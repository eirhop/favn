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
generation, bootstrap diagnostics, scoped catalog attach behavior through
`required_catalogs`, runtime placement, worker lifecycle, or DuckDB plugin child
specs. Update test support when the DuckDB client boundary or shared adapter test
instrumentation changes.

DuckDB connection config is adapter-owned. The public runtime shape uses
`open: [...]` for the session database, default-on connection-level pooling for
runner-local warm session reuse, and `duckdb: [...]` for extension load, settings,
secrets, keyed attaches, and optional catalog selection. Disable pooling with
`pool: [enabled: false]`; tune it with `pool: [enabled: true,
max_idle_per_key: 1, idle_timeout_ms: 300_000]`. Pool reuse is safe only for
matching connection/config hash, required catalog set, and adapter fingerprint;
checked-out sessions are exclusive to one asset execution. Pooling must not
bypass catalog/write admission or retry unknown-outcome writes.
