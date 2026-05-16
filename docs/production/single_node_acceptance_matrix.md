# Single-Node Production Acceptance Matrix

This document records the issue #262 backend acceptance evidence for Favn's first
single-node production target. The suite is intentionally narrow: it proves the
product-critical production path without turning acceptance into exhaustive
feature testing.

## Automated In #262

| Contract item | Classification | Evidence |
| --- | --- | --- |
| Build one single-node artifact from a consumer project | product-critical | `Favn.Local.SingleNodeProductionAcceptanceTest` builds one shared canonical artifact |
| Consumer project uses public dependencies only | foundational | `Favn.Local.CanonicalSampleProject` depends on `:favn` and `:favn_duckdb` path packages |
| Runtime uses production env names only | foundational | Acceptance asserts generated artifact files do not contain `FAVN_DEV_` |
| Start backend/orchestrator/runner/scheduler through generated scripts | product-critical | `bin/start` readiness path in acceptance |
| Bootstrap SQLite storage and first admin | product-critical | Acceptance starts with fresh SQLite and logs in with bootstrap admin |
| Register, activate, and runner-register manifest | product-critical | `mix favn.bootstrap.single` is run and repeated |
| Submit manifest-pinned run through HTTP boundary | product-critical | Acceptance submits through `Favn.Dev.OrchestratorClient` |
| Exercise realistic pipeline path | product-critical | Canonical pipeline targets SQL mart asset with `deps :all` |
| Exercise DuckDB-backed SQL asset execution | product-critical | Canonical run writes raw DuckDB data and materializes SQL summary |
| Verify run status through orchestrator HTTP boundary | product-critical | Acceptance polls `GET /api/orchestrator/v1/runs/:id` through client helper |
| Restart backend and verify durable state | product-critical | Acceptance verifies active manifest, run history, session login, and diagnostics after restart |
| Verify health/readiness/diagnostics | product-critical | Acceptance checks liveness, readiness, and service-auth diagnostics |
| Verify command idempotency/retry behavior | product-critical | Acceptance repeats bootstrap and repeats submit-run with one idempotency key |
| Verify missing runtime config failure | product-critical | Acceptance submits missing-secret asset and expects terminal error |
| Verify artifact immutability | foundational | Acceptance snapshots `dist_dir` before/after runtime operations |

## Existing Focused Evidence

| Contract item | Classification | Evidence |
| --- | --- | --- |
| SQLite scheduler state row survival | product-critical | `apps/favn_storage_sqlite/test/sqlite_single_node_bootstrap_acceptance_test.exs` |
| SQLite restore semantics | product-critical | `apps/favn_storage_sqlite/test/sqlite_control_plane_restore_test.exs` |
| Auth/session API redaction and revocation | product-critical | `apps/favn_orchestrator/test/api/router_test.exs` |
| Idempotency storage response encoding | product-critical | `apps/favn_orchestrator/test/storage/idempotency_response_codec_test.exs` |
| SQL runtime missing-config preflight details | product-critical | `apps/favn_runner/test/sql_runtime_preflight_test.exs` |
| DuckDB adapter hardening and bootstrap details | product-critical | `apps/favn_duckdb/test/sql/adapter/*` and `apps/favn_duckdb_adbc/test/sql/adapter/*` |

## Covered By Docs Or Follow-Up

| Item | Classification | Disposition |
| --- | --- | --- |
| SQLite backup/restore operator procedure | product-critical | Covered by `docs/production/single_node_operator_runbook.md`; automation follow-up #350 |
| DuckDB data-plane backup/restore procedure | product-critical | Covered by `docs/production/single_node_operator_runbook.md`; automation/design follow-up #351 |
| Focused auth/session/audit and idempotency restore evidence | product-critical | Follow-up #349; runbook avoids overclaiming current restore coverage |
| Browser smoke | nice-to-have | Follow-up #345 after backend acceptance is stable |
| favn_view production readiness hardening | product-critical | Follow-up #343 |
| Deterministic scheduler state diagnostics over HTTP | refactor-enabling | Follow-up #342 if current diagnostics plus focused storage tests are insufficient |
| Postgres production mode | refactor-enabling | Existing follow-up #257 |
| Distributed runner and multi-node mode | nice-to-have | Follow-up #344 after Postgres production mode |

## Scope Boundaries

The #262 backend acceptance suite must not include full browser testing, Postgres
production mode, distributed execution, shared SQLite assumptions, high
availability behavior, wall-clock cron waits, or direct reads of storage internals
from `favn_local` tests.

Runtime state must live outside `dist_dir`. The artifact directory is immutable
after build; acceptance tests use fresh `runtime_home`, SQLite, DuckDB, API port,
and auth/session state for each test.

The #262 acceptance suite proves restart survival and runtime-path separation. It
does not prove Favn-owned backup automation; the operator runbook's current
golden path is stopped-backend backup/restore using SQLite- and DuckDB-safe
external procedures.
