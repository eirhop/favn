# Favn Roadmap & Feature Status

## Current Version

- current release: `v0.4.0`
- current repo work: `v0.5.0-dev`

## Current Focus

Favn `v0.5.0` is a refactor release toward a manifest-first product with separate authoring, web, orchestrator, and runner boundaries.

- Phase 9 core local lifecycle has landed in `apps/favn_local`: `mix favn.dev`, `mix favn.stop`, `mix favn.reload`, `mix favn.status`
- public package topology migration is complete: `apps/favn` is now the thin public wrapper, `apps/favn_authoring` owns authoring implementation, and `apps/favn_local` owns lifecycle/tooling internals
- the Phase 9 command surface now includes `mix favn.install`, `mix favn.reset`, `mix favn.logs`, `mix favn.build.web`, `mix favn.build.orchestrator`, `mix favn.build.runner`, and `mix favn.build.single`
- `mix favn.build.runner` is the most complete packaging target today
- `mix favn.build.web` and `mix favn.build.orchestrator` currently exist as first-cut metadata-oriented outputs and still need more operationally honest packaging
- `mix favn.build.single` has the intended topology, but still needs hardening and verification before it should be treated as fully operational
- local storage configuration now supports `memory`, `sqlite`, and `postgres` through `config :favn, :local` plus `mix favn.dev --sqlite|--postgres`
- remaining Phase 9 work is hardening, validation, diagnostics polish, and packaging honesty
- the remaining Phase 9 tooling/packageability design is now captured in `docs/refactor/PHASE_9_DEV_TOOLING_PLAN.md` and `docs/refactor/PHASE_9_TODO.md`
- Phase 10 app deletion is complete: `apps/favn_legacy` and `apps/favn_view` are removed from the umbrella
- shared migration fixture substrate now lives in `apps/favn_test_support` (`priv/fixtures/**` + `FavnTestSupport.Fixtures`) so later per-app parity PRs can reuse one fixture source of truth
- authoring/compiler/planning/window coverage now lives in public-facade suites under `apps/favn/test` and core suites under `apps/favn_core/test`
- execution ownership parity batch 2 expanded runner/plugin confidence in owner apps: `apps/favn_runner/test` now covers richer runner/server/worker/connection runtime SQL paths, and `apps/favn_duckdb/test` now carries broader DuckDB adapter/runtime hardening semantics
- control-plane/runtime-state parity batch 3 is now completed: scheduler/runtime/storage/public-facade coverage has moved into owner suites under `apps/favn_orchestrator/test`, `apps/favn_storage_sqlite/test`, `apps/favn_storage_postgres/test`, and `apps/favn/test`, with shared setup moved out of legacy into `apps/favn_test_support`

## v0.5 Status By Phase

- Phase 0: complete
- Phase 1: complete
- Phase 2: in progress; `favn` public wrapper + `favn_authoring` internal ownership split is now in place, but broader cutover follow-up remains
- Phase 3: complete
- Phase 4: complete
- Phase 5: complete
- Phase 6: complete
- Phase 7: complete
- Phase 8: in progress; the corrected `favn_web + favn_orchestrator + favn_runner` boundary baseline exists, but safe-release follow-up is still open
- Phase 9: in progress; command coverage exists, while hardening, verification, diagnostics, and honest packaging still stay open
- Phase 10: in progress; app deletion is complete, while remaining post-legacy cleanup still stays open

## Pre-v1 Release Blockers / Major Remaining Work

- finish Phase 9 hardening and verification for lifecycle recovery, partial/dead service handling, startup cleanup, SQLite coverage, opt-in Postgres coverage, and targeted diagnostics
- make build outputs operationally honest for `web`, `orchestrator`, and `single`, while keeping the manifest-first `web + orchestrator + runner` boundary strict
- finish remaining Phase 10 post-legacy cleanup, including final storage-format and adapter naming follow-up
- durable orchestrator auth/session/audit persistence before any safe web-facing release
- stronger password/session foundation and browser-edge abuse controls before any safe web-facing release
- durable idempotency and scalable SSE replay/cursor model before any safe web-facing release if those capabilities are shipped
- real end-to-end integration coverage against the live orchestrator boundary
- service credential hardening and rotation model for the web-to-orchestrator boundary

## v1.0 Goal

Ship a stable, production-usable Favn on the refactored architecture:

- stable `favn` authoring surface
- manifest-version-pinned orchestration through separate orchestrator and runner runtimes
- working local development flow plus honest single-node and split deployment packaging
- built-in memory, SQLite, and Postgres storage options
- documentation and operator/developer experience aligned with the new product shape

## Beyond v1

- richer web UX after the boundary contracts settle
- more adapters and plugins
- API and polling triggers
- distributed multi-node execution and resource-aware scheduling
- deeper observability and operator tooling
