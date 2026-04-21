# `apps/favn_storage_sqlite`

Purpose:

- internal SQLite storage adapter for orchestrator persistence

Visibility:

- internal

Allowed dependencies in Phase 1:

- `favn_orchestrator`
- `ecto_sql`
- `ecto_sqlite3`

Must not depend on:

- `favn_core`, `favn_runner`

Current status:

- implemented initial Phase 6 SQLite adapter foundation
- module entrypoint: `FavnStorageSqlite.Adapter`
- managed repo bootstrap with startup migrations
- persists manifest versions, active manifest pointer, run snapshots, run events, and scheduler cursors
- current persisted run/event/scheduler payloads use BEAM term blobs as a temporary foundation choice; this should be replaced with more inspectable canonical payload storage before final cutover

## Runtime configuration

SQLite is the persistent local-dev adapter path.

```elixir
config :favn_orchestrator,
  storage_adapter: FavnStorageSqlite.Adapter,
  storage_adapter_opts: [
    database: ".favn/dev.sqlite3",
    migration_mode: :auto,
    pool_size: 1,
    busy_timeout: 5_000
  ]
```

Use `migration_mode: :manual` only when schema lifecycle is controlled externally.

## Instance model

- current managed mode supports one SQLite adapter instance per BEAM node
- the adapter repo is globally named inside the adapter app, so `supervisor_name` should be treated as supervisor identity only, not as a multi-instance isolation mechanism
