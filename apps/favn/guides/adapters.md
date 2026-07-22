# Adapters And Plugins

Adapters connect Favn to external systems.

Most users see adapters through configuration. Application code should use
`Favn.SQLAsset`, `Favn.SQLClient`, `Favn.Connection`, and `mix favn.*` commands,
not call adapter modules directly.

SQL adapters may separately implement Favn's target-generation contract used by
manual rebuilds. Ordinary transaction support is not enough: generation support
must explicitly provide isolated candidates, physical inspection, atomic swap,
transactional initialization of the first active marker, exact marker
compare-and-swap, reconciliation, and safe idempotent discard.
Favn checks these capabilities before rebuild work; application code does not
invoke the generation callbacks directly.

Discard is used for both abandoned candidate relations and retired relations
after the new active binding is durable. Adapters must compare the sidecar marker
before removal and return an unknown outcome when they cannot prove that the
requested generation is inactive.

## Two Adapter Jobs

| Adapter kind | Used for | User-facing path |
| --- | --- | --- |
| SQL execution adapter | Run SQL, inspect relations, and materialize SQL assets. | Configure a Favn connection and use `Favn.SQLAsset` or `Favn.SQLClient`. |
| Runtime storage adapter | Store Favn runtime records such as runs, schedules, logs, and diagnostics. | Configure local/runtime tooling. Do not use it for SQL asset tables. |

SQL asset tables and warehouse data are separate from Favn's runtime records.

## DuckDB Plugin

Use `:favn_duckdb` for the standard bundled DuckDB path:

```elixir
def deps do
  [
    {:favn, path: "../favn/apps/favn"},
    {:favn_duckdb, path: "../favn/apps/favn_duckdb"}
  ]
end
```

Use `:favn_duckdb_adbc` when a deployment needs explicit DuckDB shared-library or
driver control:

```elixir
def deps do
  [
    {:favn, path: "../favn/apps/favn"},
    {:favn_duckdb_adbc, path: "../favn/apps/favn_duckdb_adbc"}
  ]
end
```

Do not add runtime implementation apps directly for DuckDB support.

## Connection Example

```elixir
defmodule MyApp.Connections.Warehouse do
  @behaviour Favn.Connection

  @impl true
  def definition do
    %Favn.Connection.Definition{
      name: :warehouse,
      adapter: Favn.SQL.Adapter.DuckDB,
      config_schema: Favn.SQL.Adapter.DuckDB.config_schema_fields()
    }
  end
end
```

```elixir
config :favn,
  discovery: [apps: [:my_app], connections: :all],
  connections: [
    warehouse: [
      open: [database: ":memory:"],
      duckdb: [
        startup: [file: {:priv, :my_app, "duckdb/startup.sql"}],
        resources: [
          landing_storage: [file: {:priv, :my_app, "duckdb/landing_storage.sql"}]
        ]
      ]
    ]
  ]
```

DuckDB capabilities are configured in the SQL files with native `INSTALL`,
`LOAD`, `SET`, `CREATE SECRET`, `ATTACH`, and `USE` syntax. See
[DuckDB Session Scripts And Resources](duckdb-session-scripts.md) for the full
lifecycle and safety contract, then [Configuration](configuration.md) for the
option reference.

## Common Problems

| Problem | What to do |
| --- | --- |
| DuckDB dependency is missing | Add `:favn_duckdb` or `:favn_duckdb_adbc`. |
| Connection cannot load | Check the `Favn.Connection` module and discovery config. |
| Missing `open.database` | Add `open: [database: ...]`. |
| DuckDB session script fails | Read the reported startup/resource id, then run the redacted SQL directly against the pinned DuckDB version. |
| Session ownership error | Open a session per process. |
| SQL timeout after a write | Treat the result as unknown and inspect before retrying. |
