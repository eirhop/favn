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
- stages publication without contacting the runner; activation requires explicit
  runner readiness, exact manifest-to-runner release identity, and successful
  runner-cache verification before changing the active deployment
- pins every new run to the deployment manifest's id, content hash, and
  `required_runner_release_id`; rejects caller-forged bindings, mismatched runner
  diagnostics/results/inspection results, and non-terminal legacy unbound recovery
- keeps historical terminal run snapshots audit-readable while exposing runner
  release identity in activation responses, audit metadata, diagnostics, events,
  and operator summaries
- emits bounded publication, activation, and runner-diagnostic telemetry; rejected
  activation audits contain stable reason codes and release ids but no deployment
  configuration or runner error payloads
