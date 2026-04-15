# `apps/favn_runner`

Purpose:

- internal execution runtime boundary for business-code asset execution

Visibility:

- internal

Allowed dependencies in Phase 1:

- `favn_core`

Must not depend on in Phase 1:

- `favn_orchestrator`, `favn_view`
- `favn_storage_postgres`, `favn_storage_sqlite`
- `favn_legacy`

Current status:

- scaffold-only, not implemented yet
