# Orchestrator HTTP Contract (v1)

Machine-readable JSON Schemas owned by `favn_orchestrator` for the private API boundary.

- `error-envelope.schema.json` locks the shared error envelope.
- `run-summary.schema.json` locks `RunSummary` payload shape.
- `schedule-detail.schema.json` locks schedule inspection DTO shape.
- `actor-detail.schema.json` locks actor DTO shape.
- `sse-run-event-envelope.schema.json` locks SSE replay event envelope shape.

These schemas are intentionally additive-first for `v1` and are validated by contract-lock tests.
