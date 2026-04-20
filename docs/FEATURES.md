# Favn Roadmap & Feature Status

## Current Version

- current release: `v0.4.0`
- current repo work: `v0.5.0-dev`

## Current Focus

Favn `v0.5.0` is a refactor release toward a manifest-first product with separate authoring, web, orchestrator, and runner boundaries.

- Phase 9 core local lifecycle has landed in `apps/favn_local`: `mix favn.dev`, `mix favn.stop`, `mix favn.reload`, `mix favn.status`
- remaining Phase 9 work is packaging and local-tooling follow-up: `install`, `reset`, `logs`, and build targets for `web`, `orchestrator`, `runner`, and optional `single`
- Phase 10 remains open for legacy cutover cleanup, final repo/CI convergence, and deleting remaining supported legacy runtime dependence

## v0.5 Status By Phase

- Phase 0: complete
- Phase 1: complete
- Phase 2: in progress; public `favn` ownership and core compiler/manifest recentering are in place, but legacy cutover is not finished
- Phase 3: complete
- Phase 4: complete
- Phase 5: complete
- Phase 6: complete
- Phase 7: complete
- Phase 8: in progress; the corrected `favn_web + favn_orchestrator + favn_runner` boundary baseline exists, but safe-release follow-up is still open
- Phase 9: in progress; core local lifecycle is done in `apps/favn_local`, while packaging/build/install/reset/logs and remaining validation/polish stay open
- Phase 10: not complete; legacy cutover and final cleanup still remain

## Pre-v1 Release Blockers / Major Remaining Work

- finish Phase 9 packaging targets for `web`, `orchestrator`, `runner`, and optional `single`
- finish Phase 9 local-tooling follow-up: `mix favn.install`, `mix favn.reset`, `mix favn.logs`, plus remaining lifecycle and local-storage validation/polish
- complete Phase 10 legacy cutover so supported flows, CI, and docs no longer depend on transitional legacy runtime paths
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
