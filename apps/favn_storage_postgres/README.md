# `apps/favn_storage_postgres`

Purpose:

- internal Postgres storage adapter for orchestrator persistence

Visibility:

- internal

Allowed dependencies in Phase 1:

- `favn_orchestrator`
- `ecto_sql`
- `postgrex`

Must not depend on:

- `favn_core`, `favn_runner`

Current status:

- implemented initial Phase 6 Postgres adapter foundation
- module entrypoint: `FavnStoragePostgres.Adapter`
- supports managed and external repo modes
- managed mode supports migration bootstrap (`:auto`) and schema-ready enforcement (`:manual`)
- persists manifest versions, active manifest pointer, run snapshots, run events, and scheduler cursors
- current persisted run/event/scheduler payloads use BEAM term blobs as a temporary foundation choice; this should be replaced with more inspectable canonical payload storage before final cutover

## Runtime modes

- managed mode:
  - configure `repo_mode: :managed` with `repo_config: [...]`
  - use `migration_mode: :auto` for local/dev bootstrap or `:manual` for production migration discipline
- external mode:
  - configure `repo_mode: :external` with `repo: MyApp.Repo`
  - `migration_mode` must remain `:manual`

## Live integration tests

Postgres live integration coverage is opt-in to avoid requiring a local DB for normal CI/dev loops.

- set `FAVN_POSTGRES_TEST_URL`, for example:
  - `export FAVN_POSTGRES_TEST_URL=postgres://postgres:postgres@localhost:5432/favn_test`
- run:
  - `mix test apps/favn_storage_postgres/test/integration/adapter_live_test.exs`

If `FAVN_POSTGRES_TEST_URL` is not set, the live integration test module is skipped.

## Instance model

- current managed mode supports one Postgres adapter instance per BEAM node
- the adapter repo is globally named inside the adapter app, so `supervisor_name` should be treated as supervisor identity only, not as a multi-instance isolation mechanism
