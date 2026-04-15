# `apps/favn_storage_sqlite`

Purpose:

- internal SQLite storage adapter for orchestrator persistence

Visibility:

- internal

Allowed dependencies in Phase 1:

- `favn_orchestrator`
- `ecto_sql`
- `ecto_sqlite3`

Must not depend on in Phase 1:

- `favn_core`, `favn_runner`, `favn_view`
- `favn_legacy`

Current status:

- implemented initial Phase 6 SQLite adapter foundation
- module entrypoint: `FavnStorageSqlite.Adapter`
- managed repo bootstrap with startup migrations
- persists manifest versions, active manifest pointer, run snapshots, run events, and scheduler cursors

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
