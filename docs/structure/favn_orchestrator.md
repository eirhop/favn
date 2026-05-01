# favn_orchestrator

Purpose: control plane and system of record for manifests, runs, schedules,
events, auth/authz/audit, private HTTP API, backfill state, and storage contracts.

Code:
- `apps/favn_orchestrator/lib/favn_orchestrator.ex`
- `apps/favn_orchestrator/lib/favn_orchestrator/`
- Run process internals under `apps/favn_orchestrator/lib/favn_orchestrator/run_server/`
- preserved public contracts under `apps/favn_orchestrator/lib/favn/`
- HTTP contract schemas under `apps/favn_orchestrator/priv/http_contract/v1/`

Tests:
- `apps/favn_orchestrator/test/`
- API contract tests under `apps/favn_orchestrator/test/api/`
- storage contract/codec tests under `apps/favn_orchestrator/test/storage/`

Use when changing run lifecycle, scheduling, private API behavior, SSE/events,
auth, backfill orchestration, or storage contract semantics.
