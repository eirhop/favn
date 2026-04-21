# `apps/favn_runner`

Purpose:

- internal execution runtime boundary for business-code asset execution

Visibility:

- internal

Allowed dependencies in Phase 1:

- `favn_core`

Must not depend on:

- `favn_orchestrator`
- `favn_storage_postgres`, `favn_storage_sqlite`

Current status:

- implemented runner runtime boundary for manifest-backed execution, connection loading, and SQL runtime work
