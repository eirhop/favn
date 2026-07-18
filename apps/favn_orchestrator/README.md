# `apps/favn_orchestrator`

Purpose:

- internal control-plane runtime and API boundary

Visibility:

- internal

Allowed dependencies:

- `favn_core`

Must not depend on:

- `favn_runner` implementation

Current status:

- implemented control-plane runtime and private HTTP API boundary
- owns manifest persistence, run lifecycle, scheduler runtime, auth, audit, and storage contracts
- normalizes configured runner-client dispatch exceptions, exits, and RPC failures into explicit error tuples at the orchestrator boundary
- composes the capability-based persistence backend and requires PostgreSQL in production, development, and integration tests
- validates API/service auth, scheduler, and runner-client configuration and exposes unauthenticated liveness/readiness endpoints at `/api/orchestrator/v1/health/live` and `/api/orchestrator/v1/health/ready`
