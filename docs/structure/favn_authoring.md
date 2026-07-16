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

Use when changing asset, SQL asset, settings/freshness/execution-pool/retry DSL
capture, SQL `resources`, reusable runtime-config bundle authoring, pipeline concurrency clauses,
namespace, source, connection, or
authoring documentation behavior.

Custom public DSL declarations are macros without `@`; true Elixir attributes
such as `@moduledoc`, function `@doc`, types, behaviours, and private compiler
state remain. `Favn.Asset`, `Favn.MultiAsset`, `Favn.SQLAsset`, and `Favn.Source`
share declaration normalization through `Favn.DSL.AssetDeclarations`. The old
public `Favn.Assets` module and specialized MultiAsset `defaults/rest/extra`
grammar are removed.

`Favn.SQLAsset` captures optional list-valued `resources` before `query`.
`Favn.Namespace` resources inherit additively from root to leaf, while relation
defaults still override by key. Authoring emits only the normalized versioned
session-requirements contract.

`Favn.SQLAsset` compiles one optional typed `contract` plus up to 50 ordered
authored `check` declarations through the same `Favn.SQL.Template` and visible
`defsql` catalog as the asset query. Contract columns, structured/descriptive
grain, explicit lineage, unique keys, and row-count policy normalize into core
types. Required columns and keys are grouped into bounded generated checks, and
the compiler validates that the generated claim set exactly matches the
contract before emitting manifest runtime data. It validates
phase/policy/condition combinations at compile time; no authoring module is
loaded to execute a check.

`runtime_inputs ResolverModule` is optional, must appear at most once before
`query`, and accepts only a compiled module that explicitly implements
`Favn.SQLAsset.RuntimeInputs` and exports `resolve/1`. Authoring emits one typed
`Favn.RuntimeInputResolver.Ref`; functions, captures, MFA tuples, AST, and
resolved values never cross into the manifest.
The declaration macro is the only public DSL form; anonymous functions, captures,
MFA tuples, and inline resolver blocks are rejected rather than normalized.

Pipeline and asset/SQL `retry` declarations compile directly to the same core
`%Favn.Retry.Policy{}`. Asset policy is optional so the orchestrator can apply
operator → asset → pipeline → one-attempt precedence without losing source
attribution.
