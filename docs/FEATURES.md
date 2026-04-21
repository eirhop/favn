# Favn Roadmap & Feature Status

## Current Version

- current release: `v0.4.0`
- current repo work: `v0.5.0-dev`

## Current Focus

Favn `v0.5.0` established the intended manifest-first product architecture and
the runtime boundaries between:

- `favn` as the public authoring surface and public `mix favn.*` entrypoint owner
- `favn_authoring` as the internal authoring/manifest implementation owner
- `favn_local` as the internal local lifecycle/tooling/packaging implementation owner
- `favn_core` as the shared compiler/manifest/planning/contracts layer
- `favn_orchestrator` as the control plane and system of record
- `favn_runner` as the execution runtime
- `favn_web` as the separate public web edge

The refactor closeout is now complete enough to resume normal feature work.

Completed closeout work:

- Phase 9 local lifecycle and packaging is complete: `mix favn.install`, `mix favn.dev`, `mix favn.stop`, `mix favn.reload`, `mix favn.status`, `mix favn.logs`, `mix favn.reset`, `mix favn.build.web`, `mix favn.build.orchestrator`, `mix favn.build.runner`, `mix favn.build.single`, and `mix favn.read_doc`
- Phase 10 app deletion is complete: `apps/favn_legacy` and `apps/favn_view` are removed from the umbrella
- stable storage adapter entrypoints are restored as `Favn.Storage.Adapter.SQLite` and `Favn.Storage.Adapter.Postgres`
- SQL adapters now persist canonical inspectable `json-v1` payloads for run snapshots, run events, and scheduler state instead of BEAM term blobs
- scheduler state writes now use explicit optimistic versions rather than permissive blind increments
- external Postgres repo mode now caches successful schema-readiness validation instead of repeating catalog checks on every adapter call
- public/package ownership is locked: `apps/favn` stays thin, `apps/favn_authoring` owns authoring internals, and `apps/favn_local` owns local tooling internals

## Refactor Complete Means

For Favn, "refactor complete" now means:

- supported runtime paths no longer depend on `favn_legacy` or same-BEAM `favn_view` shortcuts
- owner-app boundaries and dependency directions are locked and exercised in the owner apps
- orchestrator and runner operate on persisted manifest versions and explicit runtime boundaries
- `favn` remains the public package, while runtime/storage/plugin implementation stays behind owner-app boundaries
- remaining work before `v1.0` is release hardening, end-to-end confidence, and product polish, not broad architecture migration

## v0.5 Status By Phase

- Phase 0: complete
- Phase 1: complete
- Phase 2: complete
- Phase 3: complete
- Phase 4: complete
- Phase 5: complete
- Phase 6: complete
- Phase 7: complete
- Phase 8: complete for refactor scope; safe-release hardening remains separate release work
- Phase 9: complete
- Phase 10: complete

## Remaining Work Before v1

The remaining work before `v1.0` is no longer refactor migration work.

### 1. Safe-release web/orchestrator hardening

- durable orchestrator persistence for actors, credentials, sessions, and audit records
- stronger boring password and session foundations suitable for a real release
- browser-edge abuse controls and rate limiting
- service credential hardening, identity binding, and rotation for the web-to-orchestrator boundary
- durable request idempotency behavior if idempotency is part of the shipped API contract
- scalable SSE replay/cursor behavior if replay/resume is part of the shipped operator experience

### 2. End-to-end runtime confidence

- real end-to-end integration coverage against the live orchestrator boundary
- packaging verification for supported deployment modes
- release-oriented verification for local memory, SQLite, and Postgres paths
- broader confidence around manifest publication, activation, run creation, execution, and event flow across process boundaries

### 3. Stable product and operator experience

- stable documented local developer flow
- stable documented single-node packaging flow
- stable documented split deployment flow for web, orchestrator, and runner
- stable documented storage configuration and operator expectations
- documentation aligned with the actual supported product shape rather than transitional refactor language

## v1.0 Goal

Ship a stable, production-usable Favn on the refactored architecture:

- stable `favn` authoring surface
- manifest-version-pinned orchestration through separate orchestrator and runner runtimes
- working and documented local development flow
- honest and documented single-node and split deployment packaging
- built-in memory, SQLite, and Postgres storage options
- release-ready web/orchestrator auth, session, and audit foundations
- end-to-end verified control-plane and execution flows

## After v1

- richer web UX and operator workflows
- more adapters and runner plugins
- API and polling triggers
- distributed multi-node execution and resource-aware scheduling
- deeper observability and operator tooling
- local watch mode and auto-reload ergonomics
- broader environment doctor and validation framework
- richer log querying and service-targeted restart niceties
