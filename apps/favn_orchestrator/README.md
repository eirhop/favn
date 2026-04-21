# `apps/favn_orchestrator`

Purpose:

- internal control-plane runtime and API boundary

Visibility:

- internal

Allowed dependencies in Phase 1:

- `favn_core`

Must not depend on:

- `favn_runner` implementation

Current status:

- implemented control-plane runtime and private HTTP API boundary
- owns manifest persistence, run lifecycle, scheduler runtime, auth, audit, and storage contracts
