# favn_authoring

Purpose: internal implementation for authoring DSLs and manifest-facing public
facade delegation.

Code:
- `apps/favn_authoring/lib/favn.ex`
- `apps/favn_authoring/lib/favn/*.ex`
- `apps/favn_authoring/lib/favn_authoring/*.ex`

Tests:
- `apps/favn_authoring/test/`
- `apps/favn_authoring/test/assets/` owns authoring DSL parity coverage for the
  core compiler and planner.
- Schedule DSL fetch/load coverage lives here because `Favn.Triggers.Schedules`
  is an authoring macro layered on core schedule values.

Use when changing asset, SQL asset, freshness DSL capture, execution pool DSL
capture, reusable runtime-config bundle authoring, pipeline concurrency clauses, namespace, source, connection, or
authoring documentation behavior.

`Favn.SQLAsset` compiles up to 50 ordered `check` declarations through the same
`Favn.SQL.Template` and visible `defsql` catalog as the asset query. It validates
phase/action/condition combinations at compile time and carries only manifest
runtime data forward; no authoring module is loaded to execute a check.
