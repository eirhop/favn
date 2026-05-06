# `apps/favn_duckdb_adbc`

DuckDB SQL adapter and runner plugin backed by Arrow Database Connectivity.
This is a supported DuckDB plugin alongside `favn_duckdb`, which remains the
supported `duckdbex`-backed plugin for bundled local/in-memory DuckDB execution.

## Purpose

- own the ADBC-backed DuckDB SQL adapter for Favn deployments that need explicit
  DuckDB shared-library/driver control
- keep ADBC/DuckDB driver concerns outside `favn`, `favn_core`, and `favn_sql_runtime`
- preserve the existing `Favn.SQLClient` and SQL asset adapter contracts

## Dependency boundary

Allowed umbrella dependency direction:

- `favn_duckdb_adbc -> favn_runner`
- `favn_duckdb_adbc -> favn_sql_runtime`

External dependency:

- `adbc` for Arrow Database Connectivity
- a DuckDB ADBC-capable `libduckdb` installed on the machine or downloaded by
  the `:adbc` package driver mechanism

Must not depend on:

- `favn`
- `favn_orchestrator`
- storage adapters

## Usage

Use `Favn.SQL.Adapter.DuckDB.ADBC` in connection definitions. The adapter exposes
the same DuckDB bootstrap schema helpers as the `favn_duckdb` adapter.

```elixir
config :favn, :runner_plugins, [FavnDuckdbADBC]
```

## DuckDB ADBC Installation

This plugin requires a DuckDB ADBC driver to be available on the machine. See
the DuckDB ADBC client documentation for installation and driver setup details:
https://duckdb.org/docs/stable/clients/adbc.html

By default the adapter asks `:adbc` for its configured DuckDB driver. Configure
an explicit driver path when production deployments need a pinned DuckDB build:

```elixir
config :favn, :duckdb_adbc,
  driver: "/opt/duckdb/1.5.2/libduckdb.so",
  entrypoint: "duckdb_adbc_init"
```

In shell-based smoke tests, the same path can be provided with
`DUCKDB_ADBC_DRIVER=/opt/duckdb/1.5.2/libduckdb.so`.

## Result Bounds

Normal `Favn.SQLClient.query` results are for bounded read-style result sets such
as `SELECT`, `WITH`, and `VALUES` queries that return small control-flow data.
The adapter wraps query SQL with `LIMIT default_row_limit + 1`, rejects overflow,
and checks the converted result against `default_result_byte_limit` before
returning rows to Elixir:

```elixir
config :favn, :runner_plugins,
  [{FavnDuckdbADBC,
    default_row_limit: 10_000,
    default_result_byte_limit: 20_000_000}]
```

Use `Favn.SQLClient.execute` for command statements such as `COPY`, DDL, DML,
`PRAGMA`, `DESCRIBE`, and `SHOW`. Large result sets should be written by DuckDB
itself with explicit SQL:

```elixir
Favn.SQLClient.execute(session, """
COPY (
  SELECT *
  FROM large_table
) TO '/explicit/path/data.parquet' (FORMAT parquet)
""")
```

Normal query results returned to Elixir are bounded; large data belongs in
explicit DuckDB files or materializations.

For JSON/API landing paths, prefer DuckDB-native file ingestion over large row
batches in Elixir:

```sql
CREATE TABLE raw_orders AS
SELECT *
FROM read_ndjson('/explicit/staging/orders/*.ndjson');
```

or:

```sql
COPY raw_orders
FROM '/explicit/staging/orders.ndjson'
(FORMAT json);
```

Use adapter-owned ADBC `bulk_insert` materialization only for small, already
bounded internal row batches.

## Diagnostics

`Favn.SQL.Adapter.DuckDB.ADBC.diagnostics/2` performs a real preflight: it opens
the configured driver, connects, runs bootstrap, pings with `SELECT 1`, and
reports `SELECT version()` as `duckdb_version` with driver paths redacted.

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
