# `apps/favn_storage_sqlite`

Purpose:

- internal SQLite storage adapter for orchestrator persistence

Visibility:

- internal

Allowed dependencies:

- `favn_orchestrator`
- `ecto_sql`
- `ecto_sqlite3`

Must not depend on:

- `favn_core`, `favn_runner`

Current status:

- implemented SQLite storage adapter for orchestrator persistence
- module entrypoint: `Favn.Storage.Adapter.SQLite`
- managed repo bootstrap with startup migrations
- persists manifest versions, active manifest pointer, run snapshots, run events, and scheduler cursors
- persisted run/event/scheduler payloads now use canonical inspectable `json-v1` payload storage through shared orchestrator codecs

## Runtime configuration

SQLite is the persistent local-dev adapter path.

```elixir
config :favn_orchestrator,
  storage_adapter: Favn.Storage.Adapter.SQLite,
  storage_adapter_opts: [
    database: ".favn/dev.sqlite3",
    migration_mode: :auto,
    pool_size: 1,
    busy_timeout: 5_000
  ]
```

Use `migration_mode: :manual` only when schema lifecycle is controlled externally.

Read safe readiness diagnostics without starting the managed adapter supervisor:

```elixir
FavnStorageSqlite.Diagnostics.readiness(
  database: ".favn/dev.sqlite3",
  migration_mode: :manual
)
```

The readiness result includes the migration mode and schema status, but redacts
the configured database path.

## Payload compatibility

- pre-closeout persisted run/event/scheduler rows encoded as BEAM term blobs are intentionally unsupported
- run `mix favn.reset` (or clear the SQLite runtime tables manually) before running with the closeout adapter

## Instance model

- current managed mode supports one SQLite adapter instance per BEAM node
- the adapter repo is globally named inside the adapter app, so `supervisor_name` should be treated as supervisor identity only, not as a multi-instance isolation mechanism
