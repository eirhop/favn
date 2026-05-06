# `apps/favn_duckdb_adbc`

DuckDB SQL adapter and runner plugin backed by Arrow Database Connectivity.

## Purpose

- own the recommended production DuckDB SQL adapter for Favn
- keep ADBC/DuckDB driver concerns outside `favn`, `favn_core`, and `favn_sql_runtime`
- preserve the existing `Favn.SQLClient` and SQL asset adapter contracts

## Dependency boundary

Allowed umbrella dependency direction:

- `favn_duckdb_adbc -> favn_runner`
- `favn_duckdb_adbc -> favn_sql_runtime`

External dependency:

- `adbc` for Arrow Database Connectivity

Must not depend on:

- `favn`
- `favn_orchestrator`
- storage adapters

## Usage

Use `Favn.SQL.Adapter.DuckDB.ADBC` in connection definitions. The adapter exposes
the same DuckDB bootstrap schema helpers as the legacy `duckdbex` adapter.

```elixir
config :favn, :runner_plugins, [FavnDuckdbADBC]
```

By default the adapter uses the bundled DuckDB ADBC driver. Configure an explicit
driver path when production deployments need a pinned DuckDB build:

```elixir
config :favn, :duckdb_adbc,
  driver: "/opt/duckdb/lib/libduckdb.so",
  entrypoint: "duckdb_adbc_init"
```

Large result sets should be written by DuckDB itself with explicit SQL such as
`COPY (...) TO '/explicit/path/data.parquet' (FORMAT parquet)`. Normal query
results returned to Elixir are bounded.

## Tests

Normal tests use fake ADBC clients and do not require a DuckDB ADBC driver:

```sh
MIX_ENV=test mix test
```

Run the real in-memory DuckDB ADBC smoke test only in environments where the
DuckDB ADBC driver is installed or configured:

```sh
FAVN_DUCKDB_ADBC_INTEGRATION=1 MIX_ENV=test mix test test/sql/adapter/duckdb_adbc_integration_test.exs
```

The smoke test honors `DUCKDB_ADBC_DRIVER` when set, using
`duckdb_adbc_init` as the entrypoint.
