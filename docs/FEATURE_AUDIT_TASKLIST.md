# Feature Audit Task List

This task list drives the feature audit for `docs/FEATURES.md`.

## Categories And Project Scope

1. Authoring and public API
   Projects: `apps/favn`, `apps/favn_authoring`
2. Manifests, planning, runner execution, and DuckDB plugin support
   Projects: `apps/favn_core`, `apps/favn_runner`, `apps/favn_duckdb`
3. Orchestration, private API, auth, events, and storage
   Projects: `apps/favn_orchestrator`, `apps/favn_storage_sqlite`, `apps/favn_storage_postgres`
4. Local developer workflow and packaging
   Projects: `apps/favn_local`, public `mix favn.*` tasks in `apps/favn`
5. Web prototype and BFF behavior
   Projects: `web/favn_web`

## Audit Questions Per Category

- What is implemented and user-visible today?
- What is implemented but still clearly partial, prototype-grade, or metadata-only?
- What appears production-ready versus still needing hardening?
- What is ambiguous and needs explicit caveats in `docs/FEATURES.md`?
- What code or tests should be cited as evidence?
