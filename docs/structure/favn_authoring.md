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
capture, SQL `@resources`, reusable runtime-config bundle authoring, pipeline concurrency clauses, namespace, source, connection, or
authoring documentation behavior.

`Favn.SQLAsset` captures optional list-valued `@resources` before `query`.
`Favn.Namespace` resources inherit additively from root to leaf, while relation
defaults still override by key. Authoring emits only the normalized versioned
session-requirements contract.

`Favn.SQLAsset` compiles up to 50 ordered `check` declarations through the same
`Favn.SQL.Template` and visible `defsql` catalog as the asset query. It validates
phase/action/condition combinations at compile time and carries only manifest
runtime data forward; no authoring module is loaded to execute a check.

`@runtime_inputs ResolverModule` is optional, must appear at most once before
`query`, and accepts only a compiled module that explicitly implements
`Favn.SQLAsset.RuntimeInputs` and exports `resolve/1`. Authoring emits one typed
`Favn.RuntimeInputResolver.Ref`; functions, captures, MFA tuples, AST, and
resolved values never cross into the manifest.
The module attribute is the only public DSL form; anonymous functions, captures,
MFA tuples, and inline resolver blocks are rejected rather than normalized.
