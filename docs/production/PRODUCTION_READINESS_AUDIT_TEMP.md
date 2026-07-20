# Temporary Production Readiness Audit

> **Temporary:** Keep this file only while the July 2026 issue and documentation
> cleanup is in progress. Delete it after the replacement production epics are
> complete or their scope has moved into durable release documentation.

## Issues closed

| Issue | Disposition |
| --- | --- |
| #257 PostgreSQL live verification | Completed by mandatory PostgreSQL Storage V2 CI and PR #491. |
| #334 manifest atom/string semantics | Implemented by string-normalized labels, bounded atom rehydration, and round-trip coverage. |
| #350 SQLite backup and migration commands | Obsolete after removal of SQLite; PostgreSQL migration, grant, provisioning, and restore tasks are implemented. |
| #358 browser session mapping pruning | Obsolete because the in-memory browser session store no longer exists. |
| #262 SQLite-first production umbrella | Superseded by the PostgreSQL production epics below. |
| #349 SQLite restore coverage | Superseded by PostgreSQL production validation; the replacement must explicitly verify auth, audit, session, and idempotency rows. |

The following narrow issues were also closed as superseded after their replacement
epic was linked: #283, #344, #345, #351, #355, #357, #423, and #429.

## Replacement implementation epics

1. **[#522 Release artifacts and deployment topology](https://github.com/eirhop/favn/issues/522)** — runnable, relocatable web,
   orchestrator, and runner releases; package distribution; secrets; health;
   upgrades; rollback; and supported single-/multi-node topology.
2. **[#523 PostgreSQL production proof and observability](https://github.com/eirhop/favn/issues/523)** — managed PostgreSQL 18,
   production-sized restore, auth/idempotency restore evidence, load, contention,
   failover, PITR, query plans, metrics, dashboards, and incident drills.
3. **[#524 Production operator UI and security](https://github.com/eirhop/favn/issues/524)** — complete operator flows, mutation
   audit, actor/session administration, browser acceptance, authorization, and
   cross-node LiveView behavior.
4. **[#525 Durable scheduling and asynchronous orchestration](https://github.com/eirhop/favn/issues/525)** — durable submission
   requests, bounded workers, scheduler dispatch outside the GenServer tick,
   retries, recovery, fairness, idempotency, cancellation, and operator visibility.
5. **[#526 Data-plane production hardening](https://github.com/eirhop/favn/issues/526)** — DuckDB/DuckLake durability decision,
   backup/recovery verification, failure injection, cancellation, resource
   controls, and operator diagnostics.

## Stale documentation

The canonical PostgreSQL documents are current. These areas are historical and
must not be used as current implementation guidance:

- `docs/report/runtime_error_handling_monitoring_review.md`
- `docs/report/n_plus_one_storage_scalability_review.md`
- `docs/report/storage_architecture_data_model_quality_audit.md`
- `docs/refactor/ORCHESTRATOR_CLEANUP_LEDGER.md`
- `docs/refactor/ORCHESTRATOR_REVIEW_PACKET.md`
- completed phase plans and TODOs under `docs/refactor/`
- `docs/DOCUMENTATION_PLAN.md`

Current documentation corrections in this cleanup:

- establish `docs/production/README.md` as the production readiness entry point;
- make web/backend topology wording consistent and explicit;
- separate implemented capability from unproven production operation;
- state that PostgreSQL recovery does not restore the analytics data plane;
- shorten `docs/FEATURES.md` and `docs/ROADMAP.md`;
- mark point-in-time reports and refactor material as historical;
- stop claiming automated advisory checks unless CI actually executes them.
