# favn_orchestrator

Purpose: control plane and system of record for manifests, runs, schedules,
events, auth/authz/audit, private HTTP API, backfill state, and storage contracts.

Code:
- `apps/favn_orchestrator/lib/favn_orchestrator.ex`
- `apps/favn_orchestrator/lib/favn_orchestrator/`
- Run process internals under `apps/favn_orchestrator/lib/favn_orchestrator/run_server/`
- preserved public contracts under `apps/favn_orchestrator/lib/favn/`
- HTTP contract schemas under `apps/favn_orchestrator/priv/http_contract/v1/`
- Production runtime config, readiness, diagnostics, redaction, and operational event modules under `apps/favn_orchestrator/lib/favn_orchestrator/`

Tests:
- `apps/favn_orchestrator/test/`
- API contract tests under `apps/favn_orchestrator/test/api/`
- Production runtime config/readiness/diagnostics tests under `apps/favn_orchestrator/test/production_runtime_config_test.exs`, `apps/favn_orchestrator/test/readiness_test.exs`, and `apps/favn_orchestrator/test/diagnostics_test.exs`
- storage contract/codec tests under `apps/favn_orchestrator/test/storage/`

Use when changing run lifecycle, scheduling, private API behavior, SSE/events,
auth, bootstrap service-token/runner-registration endpoints, backfill
orchestration, or storage contract semantics.
