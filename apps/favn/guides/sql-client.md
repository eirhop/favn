# SQL Client

`Favn.SQLClient` lets Elixir code run SQL through named Favn connections.

Use it in tools, maintenance code, and Elixir assets that need to write raw data
before SQL assets transform it.

## Basic Use

```elixir
{:ok, session} = Favn.SQLClient.connect(:warehouse)
{:ok, result} = Favn.SQLClient.query(session, "select 1 as ok")
:ok = Favn.SQLClient.disconnect(session)
```

Prefer `with_connection/3` when doing more than one SQL call:

```elixir
Favn.SQLClient.with_connection(:warehouse, [], fn session ->
  with {:ok, _} <- Favn.SQLClient.execute(session, "create schema if not exists raw"),
       {:ok, result} <- Favn.SQLClient.query(session, "select 1 as ok") do
    {:ok, result}
  end
end)
```

Sessions are process-owned. Do not share one session across concurrent tasks.

## Functions

| Function | Use it for |
| --- | --- |
| `connect/2` | Open a SQL session for a named connection. |
| `disconnect/1` | Close or release a session. |
| `with_connection/3` | Open a session, run a callback, and clean up automatically. |
| `query/3` | Run SQL that returns rows. |
| `execute/3` | Run DDL, DML, or command SQL. |
| `transaction/3` | Run work in a transaction when the adapter supports it. |
| `capabilities/1` | Inspect backend capabilities. |
| `relation/2` | Inspect table/view metadata. |
| `columns/2` | Inspect relation columns. |
| `with_required_catalogs/2` | Set catalog scope from a relation for a callback. |
| `with_required_catalogs/3` | Set catalog scope for one connection and callback. |

## Connect Options

Basic:

```elixir
Favn.SQLClient.connect(:warehouse)
```

DuckDB/DuckLake catalog scope:

```elixir
Favn.SQLClient.connect(:warehouse, required_catalogs: ["raw"])
```

Native DuckDB session resources without any configured catalog:

```elixir
Favn.SQLClient.connect(:warehouse,
  required_catalogs: [],
  required_resources: [:azure_extension]
)
```

Rules:

- Connection name must be an atom.
- Options must be a keyword list.
- Public callers cannot pass internal routing options such as `:registry_name`.
- Omitting `required_catalogs` on a DuckDB SQLClient call prepares all configured
  catalogs for inspection and maintenance compatibility. Pass an explicit list,
  including `[]`, for least-privilege setup.
- `required_resources` selects exact trusted native SQL files. SQL assets should
  use `@resources` so the requirement is visible in the manifest.

DuckDB startup and selected resources run only when a physical session is
created, before client SQL. A compatible pooled session can be reused later
without rerunning setup. Read
[DuckDB Session Scripts And Resources](duckdb-session-scripts.html) for the full
lifecycle, retry warning, and safe authoring rules.

## Query And Execute Options

Read-style query:

```elixir
Favn.SQLClient.query(session, "select * from raw.sales.orders limit 10")
```

Command:

```elixir
Favn.SQLClient.execute(session, "create schema if not exists raw")
```

Common options:

| Option | Meaning |
| --- | --- |
| `params: [...]` | Adapter-supported query parameters. |
| `timeout_ms: N` | Operation timeout. |
| `admission: [catalog: "raw"]` | Explicit write target. |
| `admission: [required_catalogs: ["raw", "mart"]]` | Multi-catalog write target. |

Favn does not parse arbitrary SQL to infer which catalog a write targets. Pass
`admission` for raw writes that need catalog-aware limits.

## Transactions

```elixir
Favn.SQLClient.transaction(session, fn tx ->
  with {:ok, _} <- Favn.SQLClient.execute(tx, "insert into raw.events values (1)"),
       {:ok, result} <- Favn.SQLClient.query(tx, "select count(*) from raw.events") do
    {:ok, result}
  end
end)
```

If the adapter does not support transactions, the call returns an unsupported
capability error.

## Child Tasks And Catalog Scope

Runner-executed assets may have a default catalog scope in the current process.
Child tasks do not inherit that process-local scope. Pass it explicitly:

```elixir
Task.async(fn ->
  Favn.SQLClient.with_required_catalogs(ctx.asset.relation, fn ->
    Favn.SQLClient.with_connection(ctx.asset.relation.connection, [], fn session ->
      Favn.SQLClient.execute(session, landing_sql())
    end)
  end)
end)
```

Or set scope by connection:

```elixir
Favn.SQLClient.with_required_catalogs(:warehouse, ["raw"], fn ->
  Favn.SQLClient.with_connection(:warehouse, [], fn session ->
    Favn.SQLClient.execute(session, landing_sql())
  end)
end)
```

## Raw Landing Pattern

Use this pattern for an Elixir asset that fetches from a source system and writes
raw rows to SQL storage:

```elixir
defmodule MyApp.Lakehouse.Raw.Sales.Orders do
  use Favn.Asset

  runtime_config :source,
    token: secret_env!("SOURCE_TOKEN")

  @relation true
  def asset(ctx) do
    rows = MyApp.SourceClient.fetch_orders(ctx.config.source.token)

    Favn.SQLClient.with_required_catalogs(ctx.asset.relation, fn ->
      Favn.SQLClient.with_connection(ctx.asset.relation.connection, [], fn session ->
        MyApp.SQLLanding.replace_rows(session, ctx.asset.relation, rows)
      end)
    end)
  end
end
```

Return counts, relation names, and redacted source metadata. Do not return source
tokens or secrets.

## Failure Modes

| Problem | What to do |
| --- | --- |
| Invalid connection name | Use an atom such as `:warehouse`. |
| Connection not found | Check `Favn.Connection` modules and `config :favn, connections: [...]`. |
| Missing env config | Provide the named env value before starting runtime work. |
| Session ownership error | Open one session per process. Do not share sessions across tasks. |
| Admission timeout | Lower concurrency or increase the external system capacity before raising limits. |
| SQL timeout after a write | Treat the write result as unknown. Inspect before retrying. |
| Unsupported transaction | Use an adapter that supports transactions or remove transaction use. |
