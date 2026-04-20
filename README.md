# Favn

## Status

Favn `v0.5.0` refactor is in progress.

- umbrella structure is scaffolded under `apps/`
- `apps/favn_legacy` contains the active v0.4 reference runtime during migration
- `apps/favn` now owns the public Phase 2 authoring/DSL surface
- internal compiler/manifest/planning/shared-contract ownership has been re-centered into `apps/favn_core`
- `apps/favn_core` now owns Phase 3 manifest/versioning foundations and shared runner contracts
- the core local dev lifecycle is now implemented in `apps/favn_local`
- Phase 0-4 contracts and migration rules are tracked in `docs/REFACTOR.md`

## Current Refactor Reality

- the public `Favn.*` authoring DSL now compiles from `apps/favn`
- orchestrator-owned runtime scheduling, run tracking, rerun/cancel flows, and public runtime contracts now live in `apps/favn_orchestrator`
- canonical manifest schema/versioning, serializer/hash identity, compatibility checks, and shared runner contracts are now implemented in `favn_core`
- initial runner boundary execution now exists in `favn_runner` for manifest registration plus Elixir/source asset execution through runner contracts
- runner-owned connection runtime and SQL execution runtime slices are now hosted in `apps/favn_runner/lib/favn/connection/**`, `apps/favn_runner/lib/favn/sql/**`, and `apps/favn_runner/lib/favn/sql_asset/**`
- local lifecycle commands `mix favn.dev`, `mix favn.stop`, `mix favn.reload`, and `mix favn.status` are implemented in `apps/favn_local`
- the earlier same-BEAM `favn_view` Phoenix prototype still exists in-repo as a transitional reference only
- examples that exercise runtime execution may still reflect legacy-reference behavior during migration

## Current Focus

- Phase 8 baseline work now establishes the `favn_web + favn_orchestrator + favn_runner` boundary in baseline form: private orchestrator HTTP API v1, SSE baseline, orchestrator-owned auth/session/audit baseline, and thin `favn_web` proof flows
- the core local developer loop has landed in `apps/favn_local`, including foreground `mix favn.dev`, `mix favn.stop`, `mix favn.reload`, and `mix favn.status`
- current Phase 9 follow-up is the remaining packaging/install/reset/logs/build work around that lifecycle
- current Phase 10 follow-up is legacy cutover cleanup so the repo docs, CI, and supported flows converge on the new architecture
- keep `favn` as the public DSL/facade package
- keep `favn_core` as the canonical manifest/planning/shared-contract layer
- keep `favn_orchestrator` as the manifest-pinned control plane, auth/authz authority, and storage owner
- keep `favn_runner` as execution-only and transport-pluggable
- treat `favn_view` as transitional only; do not treat same-BEAM UI calls as the product boundary
- breaking changes remain allowed before `v1.0`

## Documentation Pointers

- `docs/REFACTOR.md` - locked architecture boundaries, migration rules, phase plan
- `docs/FEATURES.md` - roadmap status
- `docs/lib_structure.md` - umbrella library layout
- `docs/test_structure.md` - umbrella test layout
- `docs/refactor/PHASE_2_MIGRATION_PLAN.md` - Phase 2 transitional ownership plan
- `docs/refactor/PHASE_3_MANIFEST_VERSIONING_PLAN.md` - Phase 3 manifest/versioning architecture plan
- `docs/refactor/PHASE_3_TODO.md` - Phase 3 implementation checklist
- `docs/refactor/PHASE_4_RUNNER_BOUNDARY_PLAN.md` - Phase 4 runner architecture plan
- `docs/refactor/PHASE_4_TODO.md` - Phase 4 implementation checklist
- `docs/refactor/PHASE_5_ORCHESTRATOR_BOUNDARY_PLAN.md` - Phase 5 orchestrator architecture plan
- `docs/refactor/PHASE_5_TODO.md` - Phase 5 implementation checklist
- `docs/refactor/PHASE_6_STORAGE_ADAPTER_PLAN.md` - Phase 6 storage adapter architecture plan
- `docs/refactor/PHASE_6_TODO.md` - Phase 6 implementation checklist
- `docs/refactor/PHASE_7_DUCKDB_RUNNER_PLAN.md` - Phase 7 runner/plugin architecture plan
- `docs/refactor/PHASE_7_TODO.md` - Phase 7 implementation checklist
- `docs/refactor/PHASE_8_WEB_ORCHESTRATOR_BOUNDARY_PLAN.md` - Phase 8 web/orchestrator boundary, SSE, auth, and audit architecture plan
- `docs/refactor/PHASE_8_TODO.md` - Phase 8 implementation checklist
- `docs/refactor/PHASE_9_DEV_TOOLING_PLAN.md` - Phase 9 local lifecycle and packaging plan
- `docs/refactor/PHASE_9_TODO.md` - Phase 9 implementation checklist

## Storage Adapter Verification Notes

- SQLite adapter coverage runs in normal `mix test` execution.
- Postgres live integration coverage is opt-in via `FAVN_POSTGRES_TEST_URL`:
  - `mix test apps/favn_storage_postgres/test/integration/adapter_live_test.exs`

## Storage Adapter Configuration

Current extracted adapter module names during migration:

- SQLite: `FavnStorageSqlite.Adapter`
- Postgres: `FavnStoragePostgres.Adapter`

Current storage-format note:

- SQLite/Postgres foundation adapters still persist run/event/scheduler payload bodies as BEAM term blobs for the first extracted cut
- this is intentional temporary scaffolding, not the long-term JSON/JSONB end state described in the architecture notes

SQLite local-dev persistence example:

```elixir
config :favn_orchestrator,
  storage_adapter: FavnStorageSqlite.Adapter,
  storage_adapter_opts: [
    database: ".favn/dev.sqlite3",
    migration_mode: :auto,
    pool_size: 1
  ]
```

Postgres managed mode example:

```elixir
config :favn_orchestrator,
  storage_adapter: FavnStoragePostgres.Adapter,
  storage_adapter_opts: [
    repo_mode: :managed,
    repo_config: [
      hostname: "localhost",
      database: "favn",
      username: "postgres",
      password: "postgres"
    ],
    migration_mode: :manual
  ]
```

Postgres external mode example:

```elixir
config :favn_orchestrator,
  storage_adapter: FavnStoragePostgres.Adapter,
  storage_adapter_opts: [
    repo_mode: :external,
    repo: MyApp.Repo,
    migration_mode: :manual
  ]
```

## Transitional View Runtime Configuration

The current `favn_view` Phoenix prototype is transitional only. If it is still run in non-test environments during the migration window, configure endpoint secrets with environment variables:

- `FAVN_VIEW_SECRET_KEY_BASE`
- `FAVN_VIEW_SIGNING_SALT`

## Phase 8 Orchestrator/Web Security Configuration

When running the private orchestrator API and `favn_web` prototype during Phase 8 boundary work:

- orchestrator API service credentials must be explicitly configured (fail-closed):
  - `FAVN_ORCHESTRATOR_API_SERVICE_TOKENS`
  - `FAVN_ORCHESTRATOR_API_ENABLED=1`
- web tier must provide the matching service token when calling orchestrator:
  - `FAVN_ORCHESTRATOR_SERVICE_TOKEN`
- web cookie signing secret must be configured outside local dev/test:
  - `FAVN_WEB_SESSION_SECRET`
