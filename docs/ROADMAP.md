# Favn Roadmap

This file contains forward-looking work only. Shipped capabilities and current
limitations live in `docs/FEATURES.md`.

PostgreSQL Storage V2 is now the control-plane baseline. The architecture is
defined in `docs/architecture/postgresql-control-plane-storage-v2.md`; production
operations are defined in `docs/production/postgresql_operator_runbook.md`.

## Planned Next

### 1. Prove the production deployment

- Exercise the release against a production-sized restored snapshot on the target
  managed PostgreSQL 18 service.
- Record multi-node contention, failover, lock-wait, pool-pressure, outbox-lag,
  projector-lag, and high-growth query-plan evidence.
- Complete a provider PITR restore drill and verify the restored authority,
  projections, cursors, and workspace isolation before enabling dispatch.
- Establish alerts and dashboards for every signal required by the PostgreSQL
  operator runbook.

### 2. Finish deployment packaging

- Turn `build.web` and `build.orchestrator` into independently runnable,
  supportable release artifacts.
- Keep `build.single` as the one-node developer and acceptance launcher backed by
  PostgreSQL; do not reintroduce an embedded production database.
- Define the private package publishing and upgrade process for the supported
  application split.

### 3. Stabilize the v1 product boundary

- Validate manifest schema and runner contract 6 against real customer projects.
- Measure execution-package fetch latency and reuse under production-sized SQL
  projects. Add a package cache only if evidence justifies it; any cache must be
  byte-bounded and content-addressed rather than retaining whole manifest packages.
- Align all public moduledocs, examples, and package exports with
  `docs/production/public_api_boundary.md`.
- Complete the remaining operator UI flows through the public orchestrator facade.
- Extend whole-backfill cancellation only when product requirements define its
  parent/child semantics.

### 4. Harden the runtime and data plane

- Add adapter-native cancellation where a SQL backend supports it; preserve
  explicit unknown outcomes where cancellation cannot be proved.
- Expand DuckDB/DuckLake stress and failure-injection coverage independently of
  PostgreSQL control-plane recovery.
- Add native Windows CI only when Windows becomes a supported production or
  developer target.

### 5. Re-evaluate local storage only from measured need

- Use PostgreSQL for development and integration tests today.
- Consider a smaller SQLite implementation only if the PostgreSQL developer loop
  proves materially costly. Any SQLite implementation must satisfy the accepted
  capability contracts and must not become a second source of semantics.
- Do not restore a full memory persistence backend. Pure domain tests should use
  values or focused fakes; persistence behavior belongs in PostgreSQL tests.

## Later

- Optional PostgreSQL row-level security as defense in depth after the application
  workspace boundary is proven and operational ownership is clear.
- Richer landed-data inspection and narrowly scoped local SQL tooling.
- Additional SQL adapters and cloud credential providers driven by real customer
  integrations.
- Resource-aware distributed scheduling beyond the current durable admission and
  ownership contracts.
- More deployment automation for managed-cloud and split-topology environments.
