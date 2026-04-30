# Phase 6 TODO

## Status

Checklist for implementing the Phase 6 storage adapter plan defined in `docs/refactor/PHASE_6_STORAGE_ADAPTER_PLAN.md`.

This checklist records the storage-boundary work that belongs in Phase 6. The remaining SQL payload and temporary seam cleanup are intentionally deferred to Phase 7 with DuckDB/plugin extraction.

## Phase 5 Carry-Over Cleanup

- [x] Remove the memory-specific adapter lifecycle shortcut from `Favn.Storage.child_specs/0` so real extracted adapters can own startup correctly.
- [x] Move shared storage codecs and write-semantics helpers into `favn_orchestrator` ownership.
- [x] Stop relying on legacy storage serializer modules as the long-term source of truth for extracted adapters.

## Shared Storage Semantics

- [x] Add an orchestrator-owned write-semantics helper for monotonic run snapshot persistence.
- [x] Add orchestrator-owned codecs for manifest-version payloads.
- [x] Add orchestrator-owned codecs for persisted `FavnOrchestrator.RunState` payloads.
- [x] Add orchestrator-owned codecs for append-only run event payloads.
- [x] Add orchestrator-owned codecs for scheduler cursor payloads.
- [x] Verify that memory, SQLite, and Postgres all enforce the same stale/idempotent/conflict semantics (Postgres path uses opt-in live DB coverage).

## SQLite Adapter App

- [x] Replace the scaffold `FavnStorageSqlite.hello/0` module with real adapter app entrypoints/docs.
- [x] Add SQLite adapter module in `apps/favn_storage_sqlite/lib/favn_storage_sqlite/adapter.ex`.
- [x] Add a managed SQLite repo.
- [x] Add an adapter-owned SQLite supervisor for migration bootstrapping.
- [x] Add SQLite migrations for manifests, runtime settings, runs, run events, scheduler cursors, and counters.
- [x] Persist immutable manifest versions and active-manifest selection in SQLite.
- [x] Persist run snapshots with monotonic `event_seq` and `snapshot_hash` semantics in SQLite.
- [x] Persist append-only run event history in SQLite.
- [x] Persist scheduler cursor state keyed by `{pipeline_module, schedule_id}` with version semantics in SQLite.
- [x] Document SQLite config as the persistent local-dev adapter.

## Postgres Adapter App

- [x] Replace the scaffold `FavnStoragePostgres.hello/0` module with real adapter app entrypoints/docs.
- [x] Add Postgres adapter module in `apps/favn_storage_postgres/lib/favn_storage_postgres/adapter.ex`.
- [x] Add managed Postgres repo support.
- [x] Add external repo support.
- [x] Add migration runner and schema-verification helpers.
- [x] Add Postgres migrations for manifests, runtime settings, runs, run events, and scheduler cursors.
- [x] Persist immutable manifest versions and active-manifest selection in Postgres.
- [x] Persist run snapshots transactionally with monotonic `event_seq`, `snapshot_hash` conflict detection, and deterministic `write_seq` ordering.
- [x] Persist append-only run event history in Postgres.
- [x] Persist scheduler cursor state keyed by `{pipeline_module, schedule_id}` with version semantics in Postgres.
- [x] Document managed and external repo modes plus migration behavior.

## Cross-Adapter Tests

- [x] Add shared adapter contract tests for memory, SQLite, and Postgres.
- [x] Add SQLite migration and persistence tests.
- [x] Defer broader Postgres migration, transaction, and concurrency coverage to a later roadmap phase where live-environment verification is finalized.
- [x] Add opt-in live Postgres integration smoke test coverage (`FAVN_POSTGRES_TEST_URL`).
- [x] Add concurrency-focused SQLite tests for guarded run writes and scheduler writes.
- [x] Add opt-in live Postgres concurrency tests for guarded run writes and scheduler writes.
- [x] Add orchestrator integration coverage with SQLite configured.
- [x] Add orchestrator integration coverage with Postgres configured (opt-in live DB).

## Docs Updates

- [x] Update `README.md` for Phase 6 planning and adapter roles.
- [x] Update `docs/REFACTOR.md` to point at Phase 6 planning docs and carried-forward follow-ups.
- [x] Update `docs/FEATURES.md` as Phase 6 slices land.
- [x] Update structure docs when real adapter modules or tests are added.

## Verification

- [x] Run `mix format`.
- [x] Run `mix compile --warnings-as-errors`.
- [x] Run `mix test`.
- [x] Run `mix credo --strict`.
- [x] Run `mix dialyzer`.
- [x] Run `mix xref graph --format stats --label compile-connected`.

## Explicit Out Of Scope For Later Phases

- [x] Queueing, claims, and lease-based coordination remain later-phase work.
- [x] Run deduplication and run keys remain later-phase work.
- [x] View/UI work remains later-phase scope.
- [x] Packaging and install/dev tooling remain later-phase scope.
- [x] DuckDB plugin extraction remains Phase 7.

## End Cleanup

- [x] Move adapter rename cleanup to final cutover planning after legacy module collisions are removed.
- [x] Move term-blob payload replacement to final cutover planning before the new architecture is declared complete.
- [x] Move the scheduler blind-write semantics decision to final cutover planning.
- [x] Move external Postgres schema-readiness optimization to later operational/runtime polish work.
