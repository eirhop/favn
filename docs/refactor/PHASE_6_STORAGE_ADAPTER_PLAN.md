# Phase 6 Storage Adapter Plan

## Status

Planned on branch `feature/phase-6-storage-plan`.

Phase 5 delivered the storage boundary that Phase 6 should build on:

- `favn_orchestrator` now owns the authoritative `Favn.Storage.Adapter` behaviour
- `Favn.Storage` and `FavnOrchestrator.Storage` now talk through that single adapter contract
- the in-memory adapter in `favn_orchestrator` proves the contract for manifests, active-manifest selection, run snapshots, run events, and scheduler cursors

What still remains is adapter extraction and durable persistence:

- `apps/favn_storage_sqlite` and `apps/favn_storage_postgres` are still scaffolds
- reusable storage serialization and write-semantics logic still lives partly in legacy-oriented modules
- the public roadmap still needs explicit tracking for the remaining older-phase follow-up slices that affect full end-to-end parity

## Recommendation Summary

Phase 6 should make five architectural moves together:

1. Keep `Favn.Storage.Adapter` in `favn_orchestrator` as the only authoritative adapter contract; adapter apps implement it, they do not define parallel behaviours.
2. Move shared storage-agnostic codecs and write-semantics helpers into `favn_orchestrator` so SQLite and Postgres stay thin persistence layers instead of each re-owning serialization logic.
3. Build SQLite as the smallest complete durable adapter for local developer persistence: managed repo, simple JSON-first tables, deterministic run ordering, and full support for the current orchestrator contract.
4. Build Postgres as the production-oriented adapter: transactional writes, queryable run summary columns, JSONB snapshot cache, managed or external repo support, and manual migrations by default.
5. Keep the remaining SQL payload and temporary seam cleanup out of Phase 6 and bundle them into Phase 7 with DuckDB/plugin extraction, while Phase 6 stays focused on storage.

The most important recommendation is to keep storage ownership split cleanly:

- `favn_orchestrator` owns storage contracts, persistence semantics, and adapter-neutral codecs
- `favn_storage_sqlite` owns SQLite repo, migrations, and adapter implementation
- `favn_storage_postgres` owns Postgres repo, migrations, and adapter implementation
- neither adapter app should reintroduce control-plane policy, planner logic, or runner concerns

## Current Reality On `main`

The Phase 6 plan should fit the code that already exists:

- `Favn.Storage.Adapter` currently persists `%Favn.Manifest.Version{}`, orchestrator `%FavnOrchestrator.RunState{}`, run events, and scheduler cursor payloads
- `FavnOrchestrator.Storage.Adapter.Memory` already enforces the intended monotonic run-write semantics and scheduler-state version checks in memory
- `Favn.Storage` still includes a memory-oriented adapter lifecycle shortcut (`Process.whereis(adapter)`) that will not be correct for real adapter apps, especially Postgres external-repo mode
- the old SQLite and Postgres adapters still exist in `favn_legacy`, but they are shaped around legacy `%Favn.Run{}` storage and old module ownership
- `docs/POSTGRES_STORAGE_FOUNDATION_PLAN.md` remains valuable directionally, but it predates the Phase 5 orchestrator-owned `%FavnOrchestrator.RunState{}` contract and must be applied to the new boundary rather than copied blindly

That means Phase 6 should not start from an empty page, but it also should not port the legacy adapters file-for-file.

## Phase 6 Architecture Decisions

### 1. Keep the adapter contract in `favn_orchestrator`

Phase 5 already collapsed duplicate adapter behaviours. Phase 6 should preserve that decision.

Recommended rule:

- `Favn.Storage.Adapter` remains the only low-level storage behaviour
- `Favn.Storage` remains the public facade
- `FavnOrchestrator.Storage` remains the orchestrator-facing facade
- SQLite/Postgres apps implement `Favn.Storage.Adapter` directly

This keeps the dependency rule intact:

- `favn_orchestrator` owns control-plane contracts
- storage apps depend on `favn_orchestrator`
- the orchestrator does not depend on adapter implementation details

### 2. Re-center shared storage codecs into `favn_orchestrator`

The adapter apps should not each invent their own serialization of manifests, run state, run events, and scheduler state.

Recommended new adapter-neutral modules under `apps/favn_orchestrator/lib/favn_orchestrator/storage/`:

- `write_semantics.ex`
  - canonical stale/idempotent/conflict decisions for run snapshot writes
- `manifest_codec.ex`
  - canonical persisted payload shape for `%Favn.Manifest.Version{}`
- `run_state_codec.ex`
  - canonical persisted payload shape for `%FavnOrchestrator.RunState{}`
- `run_event_codec.ex`
  - canonical event payload validation/normalization
- `scheduler_state_codec.ex`
  - canonical scheduler cursor payload validation/normalization

The exact file count can stay smaller if some codecs collapse together naturally, but the conceptual split matters:

- adapter-neutral value-shape logic belongs with the orchestrator contract
- database-specific SQL, migrations, repo wiring, and transactional orchestration belong in the adapter apps

### 3. Fix the adapter lifecycle seam before real adapters land

The current `Favn.Storage.child_specs/0` path assumes that an adapter module can be treated like a registered process name.

That happens to work for the current memory adapter, but it is the wrong contract for:

- managed SQLite/Postgres supervisors
- Postgres `repo_mode: :external`, where the adapter may intentionally return `:none`
- future adapter implementations that are not named `GenServer`s

Recommended cleanup at the start of Phase 6:

- remove the `Process.whereis(adapter)` shortcut from `Favn.Storage.child_specs/0`
- let `FavnOrchestrator.Storage.child_specs/0` be the only place that asks adapters for runtime children
- treat `adapter.child_spec/1 == :none` as the official signal that the adapter does not need to start anything

This is a small carry-over cleanup from Phase 5, but it should be done before claiming the storage boundary is ready for extracted adapter apps.

### 4. SQLite should optimize for local persistence, not production complexity

SQLite exists to make `mix favn.dev --sqlite` useful and durable without dragging production-only complexity into the local path.

Recommended SQLite goals:

- durable local persistence for the current orchestrator contract
- managed repo only in the first cut
- migration defaults optimized for local developer UX
- deterministic run ordering and monotonic snapshot semantics identical to memory/Postgres behaviour

Recommended SQLite config shape:

```elixir
config :favn_orchestrator,
  storage_adapter: Favn.Storage.Adapter.SQLite,
  storage_adapter_opts: [
    database: ".favn/dev.sqlite3",
    pool_size: 1,
    busy_timeout: 5_000,
    migration_mode: :auto
  ]
```

Recommended SQLite rules:

- `migration_mode` should default to `:auto` for the adapter's managed-repo local-dev role
- `:manual` may exist as an opt-in for embedded or test scenarios, but it should not be the default friction path
- keep the schema JSON-first and boring; do not try to mirror every Postgres table if the current orchestrator contract does not need it yet

Current implementation note:

- the first extracted SQLite cut currently stores run, event, and scheduler payload bodies as BEAM term blobs behind relational summary/version columns
- that is an acceptable extraction shortcut for the first durable cut, but it should be treated as temporary scaffolding rather than the final inspectable persistence shape

Recommended SQLite persisted tables in the first cut:

1. `favn_manifest_versions`
   - immutable manifest versions by `manifest_version_id`
   - store `content_hash` and canonical payload JSON
2. `favn_runtime_settings`
   - single-row or key/value persisted state for the active manifest pointer
3. `favn_runs`
   - `run_id`, manifest identity, status, `event_seq`, `snapshot_hash`, deterministic `updated_seq`, timestamps, and canonical `run_state_json`
4. `favn_run_events`
   - append-only event log keyed by `{run_id, sequence}`
5. `favn_scheduler_cursors`
   - scheduler progress keyed by `{pipeline_module, schedule_id}` with explicit `version`
6. `favn_counters`
   - monotonic sequence allocation for deterministic run ordering where SQLite lacks native sequences

Recommended non-goal for the first SQLite cut:

- do not add a separate `run_nodes` table unless a current orchestrator API actually needs step-level SQL filtering

The current contract can be satisfied by storing canonical `RunState` JSON plus queryable run summary columns. That is enough for local persistence and avoids overbuilding the dev adapter.

### 5. Postgres should be production-oriented and queryable

Postgres should carry forward the direction of `docs/POSTGRES_STORAGE_FOUNDATION_PLAN.md`, but adapted to the orchestrator-owned boundary that exists now.

Recommended Postgres goals:

- production-capable durable orchestrator persistence
- transactionally correct run snapshot and event writes
- managed and external repo support
- manual migrations by default
- queryable run summary fields for future operator APIs and `favn_view`

Recommended Postgres config modes:

```elixir
config :favn_orchestrator,
  storage_adapter: Favn.Storage.Adapter.Postgres,
  storage_adapter_opts: [
    repo_mode: :managed,
    repo_config: [
      hostname: "localhost",
      port: 5432,
      database: "favn",
      username: "postgres",
      password: "postgres",
      pool_size: 10
    ],
    migration_mode: :manual
  ]
```

```elixir
config :favn_orchestrator,
  storage_adapter: Favn.Storage.Adapter.Postgres,
  storage_adapter_opts: [
    repo_mode: :external,
    repo: MyApp.Repo,
    migration_mode: :manual
  ]
```

Recommended Postgres persisted tables in the first cut:

1. `favn_manifest_versions`
   - immutable manifest envelopes with `manifest_json` JSONB and unique `content_hash`
2. `favn_runtime_settings`
   - current active manifest pointer and other future runtime-wide settings
3. `favn_runs`
   - run identity, manifest identity, status, submit kind, lineage fields, `event_seq`, `write_seq`, timestamps, `snapshot_hash`, and `run_state_json` JSONB
4. `favn_run_events`
   - append-only event rows unique by `{run_id, sequence}`
5. `favn_scheduler_cursors`
   - durable scheduler cursor rows keyed by `{pipeline_module, schedule_id}` with optimistic version semantics

Recommended Postgres design rule:

- persist queryable columns for the fields needed by `list_runs/1`, conflict detection, future operator filtering, and scheduler correctness
- keep the full canonical `RunState` payload as versioned JSONB for projection/reconstruction

Current implementation note:

- the first extracted Postgres cut currently stores run, event, and scheduler payload bodies as BEAM term blobs plus relational summary/version columns
- that is intentionally narrower than the target JSON/JSONB-oriented end state and should be cleaned up in a later follow-up before final cutover

This is intentionally narrower than the older legacy-oriented Postgres foundation plan. In particular:

- do not make `favn_asset_window_latest` a blocker for the first Phase 6 cut unless a current orchestrator API starts depending on it
- do not reintroduce legacy `%Favn.Run{}`-first persistence as the primary durable model
- the current first extracted cut still uses temporary term blobs for payload bodies even though the target end state should move away from them

The right first Postgres unit is the orchestrator-owned `RunState` plus queryable summary columns and append-only events.

### 6. Persist the current orchestrator contract directly

Phase 6 should follow the current control-plane contract rather than the old legacy surface.

Recommended persisted units:

- `%Favn.Manifest.Version{}`
- active manifest version id
- `%FavnOrchestrator.RunState{}`
- append-only run event payloads
- scheduler cursor payloads keyed by `{pipeline_module, schedule_id}`

Recommended invariants for both SQLite and Postgres:

1. manifest versions are immutable once accepted
2. active-manifest updates affect future runs only
3. run snapshot writes are monotonic by `event_seq`
4. same `event_seq` plus same `snapshot_hash` is idempotent success
5. same `event_seq` plus different `snapshot_hash` is `{:error, :conflicting_snapshot}`
6. lower `event_seq` is `{:error, :stale_write}`
7. run events are append-only and unique by `{run_id, sequence}`
8. scheduler cursor writes are keyed by `{pipeline_module, schedule_id}` and enforce monotonic `version`

These semantics should be codified once in shared write-semantics helpers and verified across all adapters.

### 7. Keep the module naming boring and preserved

The adapter apps should expose preserved `Favn.*` module names for the actual adapter modules so configuration remains obvious.

Recommended module names:

- `apps/favn_storage_sqlite/lib/favn/storage/adapter/sqlite.ex`
  - `Favn.Storage.Adapter.SQLite`
- `apps/favn_storage_postgres/lib/favn/storage/adapter/postgres.ex`
  - `Favn.Storage.Adapter.Postgres`

The top-level app modules may remain small documentation modules:

- `FavnStorageSqlite`
- `FavnStoragePostgres`

But the actual configured adapter should be the preserved `Favn.Storage.Adapter.*` module.

### 8. Keep older-phase SQL follow-ups attached to Phase 7

Phase 6 should keep one small storage-boundary cleanup from Phase 5, but the remaining SQL-centered carry-overs fit better with Phase 7.

#### Phase 5 carry-over cleanup

Required before the storage-app boundary is truly finished:

- fix the adapter lifecycle seam described above
- move shared storage codecs/write semantics out of legacy-oriented modules and into orchestrator ownership

#### Move these to Phase 7

These should be tracked with DuckDB/plugin extraction instead of Phase 6:

- carry SQL asset execution payload through the manifest/core contract
- enable manifest-pinned SQL asset execution in `favn_runner`
- remove temporary migration seams in `favn`, especially the remaining transitional SQL/runtime bridge placement, once manifest-backed runner SQL execution is complete

This keeps Phase 6 focused on persistence while Phase 7 owns the remaining SQL execution and plugin-shape work in one bounded slice.

## Recommended Runtime Shape

### SQLite runtime shape

Recommended children under the SQLite adapter when configured in managed mode:

- `Favn.Storage.SQLite.Repo`
- optional adapter-owned supervisor wrapper if migration bootstrapping is needed

Do not add:

- a second storage facade
- adapter-specific planner or scheduler services
- SQLite-specific control-plane workers beyond repo and migration bootstrap

### Postgres runtime shape

Recommended children under the Postgres adapter when configured in managed mode:

- `Favn.Storage.Postgres.Repo`
- optional adapter-owned supervisor wrapper that can verify or run migrations before steady-state startup

Recommended behavior in external mode:

- the adapter returns `:none` from `child_spec/1`
- no repo lifecycle is owned by Favn
- startup still verifies schema readiness when the adapter is first used

Current managed-instance constraint:

- the current extracted SQLite and Postgres adapters should be treated as one managed instance per adapter app per BEAM node
- the repo modules are still globally named, so `supervisor_name` is not a true multi-instance isolation mechanism yet

## Legacy Slice Map

### Move or rewrite in Phase 6

- `apps/favn_legacy/lib/favn/storage/adapter/sqlite.ex`
- `apps/favn_legacy/lib/favn/storage/adapter/postgres.ex`
- `apps/favn_legacy/lib/favn/storage/sqlite/repo.ex`
- `apps/favn_legacy/lib/favn/storage/sqlite/supervisor.ex`
- `apps/favn_legacy/lib/favn/storage/sqlite/migrations.ex`
- `apps/favn_legacy/lib/favn/storage/sqlite/migrations/create_runs.ex`
- `apps/favn_legacy/lib/favn/storage/postgres/repo.ex`
- `apps/favn_legacy/lib/favn/storage/postgres/supervisor.ex`
- `apps/favn_legacy/lib/favn/storage/postgres/migrations.ex`
- `apps/favn_legacy/lib/favn/storage/postgres/migrations/create_foundation.ex`
- `apps/favn_legacy/lib/favn/storage/run_serializer.ex`
- `apps/favn_legacy/lib/favn/storage/run_write_semantics.ex`
- `apps/favn_legacy/lib/favn/storage/snapshot_hash.ex`
- `apps/favn_legacy/lib/favn/storage/term_json.ex`

### Rewrite rather than copy verbatim

- legacy SQLite snapshot storage that persists `%Favn.Run{}` term blobs
- legacy Postgres mapper logic that assumes `%Favn.Run{}` is the durable contract
- any fallback scheduler queries that treat `pipeline_module` alone as the durable key

### Leave for later phases

- queue/claim/lease tables and logic
- run deduplication keys
- materialization history tables
- operator query APIs beyond the current storage contract
- view/UI work
- packaging/install tooling

## File Plan

### In `apps/favn_orchestrator/lib/favn_orchestrator/storage/`

- `write_semantics.ex`
- `manifest_codec.ex`
- `run_state_codec.ex`
- `run_event_codec.ex`
- `scheduler_state_codec.ex`

### In `apps/favn_storage_sqlite/lib/favn/storage/`

- `adapter/sqlite.ex`
- `sqlite/repo.ex`
- `sqlite/supervisor.ex`
- `sqlite/migrations.ex`
- `sqlite/migrations/create_foundation.ex`

### In `apps/favn_storage_postgres/lib/favn/storage/`

- `adapter/postgres.ex`
- `postgres/repo.ex`
- `postgres/supervisor.ex`
- `postgres/migrations.ex`
- `postgres/migrations/create_foundation.ex`
- `postgres/queries.ex`

The exact adapter-internal file count can stay smaller, but the ownership split should stay the same:

- orchestrator app owns adapter-neutral semantics
- adapter app owns repo, SQL, and migration details

## Implementation Order

Recommended Phase 6 work order:

1. Fix the Phase 5 adapter lifecycle seam and re-center shared storage codecs/write semantics into `favn_orchestrator`.
2. Replace the `favn_storage_sqlite` scaffold with a real SQLite adapter that fully satisfies the current orchestrator storage contract.
3. Wire and document `mix favn.dev --sqlite` around the extracted SQLite adapter.
4. Replace the `favn_storage_postgres` scaffold with a real Postgres adapter, using the updated orchestrator-owned codecs and production-oriented migration/repo rules.
5. Add a shared adapter contract test suite and run it against memory, SQLite, and Postgres.
6. Hand the remaining SQL payload and temporary seam cleanup work off to Phase 7 rather than expanding Phase 6 scope.

## Testing Plan

### Shared adapter contract tests

Add adapter-agnostic coverage for:

- manifest version immutability and active-manifest selection
- run snapshot insert, replace, stale write rejection, and conflicting snapshot rejection
- same-sequence idempotency
- run event append-only semantics and duplicate-sequence rejection
- scheduler cursor round-trip and version semantics

The same contract suite should run against:

- memory adapter
- SQLite adapter
- Postgres adapter

### `apps/favn_storage_sqlite/test`

- migration bootstrap tests
- repo restart persistence tests
- `list_runs/1` ordering by deterministic write sequence
- manifest activation persistence tests
- run event persistence tests
- scheduler cursor persistence tests

### `apps/favn_storage_postgres/test`

- managed repo mode tests
- external repo mode tests
- manual vs auto migration behavior tests
- transactional run write tests
- deterministic `write_seq` ordering tests
- concurrent conflict/idempotency tests
- schema verification failure tests

### Cross-app integration

- run the orchestrator runtime with SQLite configured and verify manifest registration, run submission, rerun, cancellation, and scheduler restart persistence
- run the orchestrator runtime with Postgres configured and verify the same control-plane flows

## Documentation Updates Required In Phase 6

Phase 6 implementation should update at least:

- `README.md`
- `docs/REFACTOR.md`
- `docs/FEATURES.md`
- `docs/lib_structure.md`
- `docs/test_structure.md`
- adapter app READMEs under `apps/favn_storage_sqlite/` and `apps/favn_storage_postgres/`

The docs must make these points explicit:

- memory remains the default local storage mode
- SQLite is the persistent local-dev adapter
- Postgres is the production-oriented durable adapter
- `favn_orchestrator` still owns the storage contract even after adapter extraction
- the remaining SQL payload, manifest-backed runner SQL execution, and temporary seam-removal work are intentionally deferred to Phase 7

## Explicit Out-Of-Scope List

Do not implement these as part of Phase 6:

- distributed queueing, leasing, or claim coordination
- run deduplication / run-key features
- materialization-history APIs and tables unless they become a concrete blocker
- view/UI work
- packaging/install tooling
- plugin extraction into `favn_duckdb`
- a storage-service abstraction that bypasses the orchestrator-owned contract
- forcing SQLite to match Postgres table-for-table when the current contract does not need it

Phase 6 is successful when the scaffold storage apps are replaced with real SQLite and Postgres adapters that satisfy the orchestrator-owned storage contract, local persistence works through SQLite, production-oriented persistence works through Postgres, and the remaining SQL-centered follow-up work is clearly handed off to Phase 7 instead of inflating storage scope.
