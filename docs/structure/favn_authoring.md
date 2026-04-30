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

Use when changing asset, SQL asset, pipeline, namespace, source, connection, or
authoring documentation behavior.
