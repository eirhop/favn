# `apps/favn`

Purpose:

- public user-facing package for Favn authoring and integration

Visibility:

- public package

Allowed dependencies in Phase 1:

- `favn_core`

Must not depend on:

- `favn_runner`, `favn_orchestrator`
- `favn_storage_postgres`, `favn_storage_sqlite`, `favn_duckdb`

Current status:

- implemented public DSL/facade package
- delegates runtime-facing calls to orchestrator/runner boundaries without owning runtime internals
