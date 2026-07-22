# favn_authoring

Purpose: internal implementation for authoring DSLs and manifest-facing public
facade delegation.

Canonical manifest generation requires a verified `%Favn.RunnerRelease{}` and
derives `required_runner_release_id`; callers cannot supply that ID separately.
Planning-only APIs build a planning index without pretending to create a
publishable manifest and therefore do not require a runner descriptor.

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

Use when changing asset, SQL asset, settings/freshness/execution-pool/retry/resource-recovery DSL
capture, SQL `resources`, reusable runtime-config bundle authoring, pipeline concurrency clauses,
namespace, source, connection, or
authoring documentation behavior.

Custom public DSL declarations are macros without `@`; true Elixir attributes
such as `@moduledoc`, function `@doc`, types, behaviours, and private compiler
state remain. `Favn.Asset`, `Favn.MultiAsset`, `Favn.SQLAsset`, and `Favn.Source`
share declaration normalization through `Favn.DSL.AssetDeclarations`. The old
public `Favn.Assets` module and specialized MultiAsset `defaults/rest/extra`
grammar are removed.

`Favn.Namespace` is a structural declaration module. Relation fields merge by
key; resources and runtime config compose; settings and metadata shallow-merge;
and runtime inputs, freshness, windows, and materialization use the closest
declaration. Descendant Asset, MultiAsset, SQLAsset, and Source modules consume
only the fields supported by their DSL. Authoring emits fully resolved canonical
assets and the normalized versioned SQL session-requirements contract.

`Favn.Namespace` runtime-config bundles also inherit root-to-leaf into
descendant `Favn.SQLAsset` modules and merge before leaf declarations through
`Favn.RuntimeConfig.Requirements`. Namespace and leaf `runtime_inputs`
declarations use closest-wins selection. SQL asset finalization requires an
effective resolver whenever runtime requirements are non-empty, and resolved
configuration does not become SQL parameters.

All asset kinds retain their local declarations and resolve namespace
configuration during explicit asset compilation. Namespace edits therefore
update affected descendant fingerprints without recompiling leaf modules, and
clean, incremental, and parallel builds use the same finalization path.

`Favn.SQLAsset` compiles one optional typed `contract` plus up to 50 ordered
authored `check` declarations through the same `Favn.SQL.Template` and visible
`defsql` catalog as the asset query. `Favn.SQL.ContractFragment` owns reusable
column-only declarations; explicit `include Module` statements flatten those
columns at the declaration position and retain composition provenance. Contract
columns, structured/descriptive grain, explicit lineage, unique keys, and
ordered exact/bounded row-count claims normalize into core types. Repeated
`row_count` declarations retain authored order; a typed claim parameter uses
`row_count equals: param(:name)`. Required columns and keys are grouped into
bounded generated checks while every row-count claim emits its own check,
and the compiler validates that the generated claim set exactly matches the
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

Pipeline `resource_recovery :retry_remaining` compiles to the separate
`%Favn.ResourceRecovery.Policy{}` manifest field. It opts into a linked new run
after a resource probe closes a circuit; it never changes node-attempt policy.
