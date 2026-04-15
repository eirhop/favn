# Phase 2 Migration Plan

## Status

Planned on branch `feature/phase-2-migration-plan`.

Implementation note: current branch prioritizes public namespace ownership in `favn` for Phase 2. Runtime execution remains legacy-owned, and unsupported runtime paths in migrated SQL APIs fail deterministically with `{:error, :runtime_not_available}`.

## Goal

Make user business modules compile against `favn`, build a manifest from authored modules without starting runtime services, and move the stable authoring/domain surface out of `favn_legacy` before runner/orchestrator work begins.

## Deliverables

Move into `favn_core` and/or `favn`:

- public DSL entrypoints
- asset/pipeline/schedule definitions
- canonical structs and types
- graph/dependency planning
- compile-time metadata capture
- manifest data model and manifest generation

`favn` becomes the public facade package that user projects depend on.

## Exit Criteria

- user business code can compile against `favn`
- manifest can be generated from user modules without orchestrator/runtime coupling
- unit tests for DSL/compiler/domain run under the new apps

## Phase 2 Implementation Rule

- copy pure structs, validators, and compile-time helpers
- copy and edit DSL macros that already capture the right metadata shape
- rewrite anything that currently depends on `Application.get_env/3`, `:persistent_term`, `GenServer`, runtime execution, scheduler state, or SQL session management

## Bounded Slices

1. Core domain move into `favn_core`: refs, relations, diagnostics, window/schedule structs, pipeline definitions, SQL template/definition IR, and asset dependency metadata.
2. Public DSL move into `favn`: `Favn.Asset`, `Favn.Assets`, `Favn.MultiAsset`, `Favn.SQLAsset`, `Favn.SQL`, `Favn.Source`, `Favn.Pipeline`, `Favn.Namespace`, `Favn.Window`, `Favn.Triggers.Schedules`, and `Favn.Connection`.
3. Pure compiler/graph foundation in `favn_core`: module compilation, dependency inference, graph building, and pipeline/module resolution rewritten to operate on explicit inputs rather than global runtime registries.
4. New manifest layer in `favn_core` plus facade wiring in `favn`: manifest structs, manifest builder/generator, and a rewritten `Favn` facade focused on compile/introspection APIs instead of runtime execution.

## App Ownership

- `favn`: public facade and public DSL entrypoints
- `favn_core`: canonical structs/types, compile-time metadata capture, graph/dependency planning, manifest model, and manifest generation internals
- `favn_legacy`: runtime, scheduler, storage, SQL execution, and operator APIs until later phases

## Shared Test Support Rule

- cross-app reusable fixtures, helpers, builders, and file fixtures should live in `favn_test_support`
- owner apps such as `favn_core` and `favn` may depend on `favn_test_support` only with `only: :test`
- fixtures used by only one app should remain in that app's local `test/support`
- `favn_test_support` must stay dependency-light so low-level apps can use it without creating umbrella dependency cycles

## File Strategy

### Copy With Little Or No Change

| Legacy file | Target | Strategy | Notes |
| --- | --- | --- | --- |
| `apps/favn_legacy/lib/favn/ref.ex` | `favn_core` | copy | Pure canonical ref type. |
| `apps/favn_legacy/lib/favn/relation_ref.ex` | `favn_core` | copy | Pure relation identity/value object. |
| `apps/favn_legacy/lib/favn/timezone.ex` | `favn_core` | copy | Pure timezone validator helper. |
| `apps/favn_legacy/lib/favn/dsl/compiler.ex` | `favn_core` | copy | Shared compile-time helper functions. |
| `apps/favn_legacy/lib/favn/asset/dependency.ex` | `favn_core` | copy | Pure dependency metadata struct. |
| `apps/favn_legacy/lib/favn/asset/relation_input.ex` | `favn_core` | copy | Pure typed SQL relation input struct. |
| `apps/favn_legacy/lib/favn/asset/relation_resolver.ex` | `favn_core` | copy | Pure relation normalization logic. |
| `apps/favn_legacy/lib/favn/window/spec.ex` | `favn_core` | copy | Pure window spec struct/validation. |
| `apps/favn_legacy/lib/favn/window/anchor.ex` | `favn_core` | copy | Pure anchor window struct/expansion. |
| `apps/favn_legacy/lib/favn/window/runtime.ex` | `favn_core` | copy | Pure runtime window struct. |
| `apps/favn_legacy/lib/favn/window/key.ex` | `favn_core` | copy | Pure window identity encoding/decoding. |
| `apps/favn_legacy/lib/favn/window/validate.ex` | `favn_core` | copy | Shared validation helpers. |
| `apps/favn_legacy/lib/favn/connection.ex` | `favn` | copy | Public behaviour stays valid in Phase 2. |
| `apps/favn_legacy/lib/favn/connection/definition.ex` | `favn_core` | copy | Static provider definition is still useful. |
| `apps/favn_legacy/lib/favn/sql/definition.ex` | `favn_core` | copy | Pure reusable SQL definition IR. |
| `apps/favn_legacy/lib/favn/sql/source.ex` | `favn_core` | copy | Compile-time SQL file loading only. |
| `apps/favn_legacy/lib/favn/sql_asset/materialization.ex` | `favn_core` | copy | Pure SQL materialization metadata. |

### Copy First, Then Edit

| Legacy file | Target | Strategy | Why copy-and-edit |
| --- | --- | --- | --- |
| `apps/favn_legacy/lib/favn/window.ex` | `favn` | copy and edit | Public wrapper is already pure; only app ownership changes. |
| `apps/favn_legacy/lib/favn/namespace.ex` | `favn` | copy and edit | Public helper is stable, but imports/owner app change. |
| `apps/favn_legacy/lib/favn/diagnostic.ex` | `favn_core` | copy and edit | Struct is good, but stage list should become compiler/manifest-focused. |
| `apps/favn_legacy/lib/favn/asset.ex` | `favn` | copy and edit | DSL is mostly correct; adjust emitted compile metadata and remove legacy runtime assumptions from docs/types. |
| `apps/favn_legacy/lib/favn/assets.ex` | `favn` | copy and edit | Same reason as `Favn.Asset`; metadata capture logic is already valuable. |
| `apps/favn_legacy/lib/favn/source.ex` | `favn` | copy and edit | Declarative DSL is good; output should target manifest generation directly. |
| `apps/favn_legacy/lib/favn/multi_asset.ex` | `favn` | copy and edit | Compile-time generation logic is worth preserving, but ownership/output contracts need cleanup. |
| `apps/favn_legacy/lib/favn/pipeline.ex` | `favn` | copy and edit | DSL is good, but downstream resolution must stop building runtime pipeline context. |
| `apps/favn_legacy/lib/favn/pipeline/definition.ex` | `favn_core` | copy and edit | Struct is close, but should align to manifest-facing pipeline fields only. |
| `apps/favn_legacy/lib/favn/triggers/schedule.ex` | `favn_core` | copy and edit | Struct is good; remove app-config default timezone lookup from this layer. |
| `apps/favn_legacy/lib/favn/triggers/schedules.ex` | `favn` | copy and edit | Named schedule DSL is already Phase 2 scope. |
| `apps/favn_legacy/lib/favn/assets/compiler.ex` | `favn_core` | copy and edit | Core module compilation logic is reusable, but naming and outputs should become manifest-first. |
| `apps/favn_legacy/lib/favn/assets/dependency_inference.ex` | `favn_core` | copy and edit | Pure compile-time logic, but should run over manifest catalog data rather than legacy registry shape. |
| `apps/favn_legacy/lib/favn/sql.ex` | `favn` | copy and heavily edit | Keep `use Favn.SQL`, `defsql`, and `~SQL`; remove runtime connection/session APIs from this file. |
| `apps/favn_legacy/lib/favn/sql/template.ex` | `favn_core` | copy and edit | IR/parser is worth keeping; trim any legacy compiler assumptions while preserving tests. |
| `apps/favn_legacy/lib/favn/sql_asset.ex` | `favn` | copy and heavily edit | Keep compile-time SQL metadata capture; remove generated runtime execution entrypoint. |
| `apps/favn_legacy/lib/favn/sql_asset/compiler.ex` | `favn_core` | copy and edit | Small bridge worth reusing after SQL asset output contract is updated. |
| `apps/favn_legacy/lib/favn/sql_asset/definition.ex` | `favn_core` | copy and edit | Good intermediate compile artifact; align it to manifest generation instead of runtime rendering. |
| `apps/favn_legacy/lib/favn/sql_asset/relation_usage.ex` | `favn_core` | copy and edit | Pure compile-time relation usage collector; adapt to the new catalog/manifest pipeline. |

### Rewrite Or Replace

| Legacy file | Target | Strategy | Why rewrite |
| --- | --- | --- | --- |
| `apps/favn_legacy/lib/favn.ex` | `favn` | new file from scratch | Current facade is runtime/operator oriented (`run_*`, history, scheduler, SQL runtime helpers). Phase 2 needs a compile/introspection facade. |
| `apps/favn_legacy/lib/favn/assets/registry.ex` | `favn_core` | new file from scratch | Current registry is global runtime state over app config and `:persistent_term`; Phase 2 needs a pure manifest/catalog builder. |
| `apps/favn_legacy/lib/favn/assets/graph_index.ex` | `favn_core` | rewrite from extracted algorithms | Topology logic is reusable, but file contract is wrong because it is a cached global index. |
| `apps/favn_legacy/lib/favn/assets/planner.ex` | `favn_core` | rewrite from extracted algorithms | Current planner builds execution plans for runs/windows; Phase 2 needs dependency planning for manifest compilation without runtime coupling. |
| `apps/favn_legacy/lib/favn/plan.ex` | `favn_core` | new file from scratch | Current `%Favn.Plan{}` is run/execution oriented; Phase 2 needs manifest/planning structs with cleaner boundaries. |
| `apps/favn_legacy/lib/favn/pipeline/resolver.ex` | `favn_core` | new file from scratch | It currently depends on `Favn.list_assets/0` and builds runtime pipeline context maps. |
| `apps/favn_legacy/lib/favn/pipeline/resolution.ex` | `favn_core` | new file from scratch | It reflects runtime handoff, not manifest compilation output. |
| `apps/favn_legacy/lib/favn/connection/loader.ex` | later phase | do not move now | Runtime config loading is not part of authoring/compiler foundation. |
| `apps/favn_legacy/lib/favn/connection/registry.ex` | later phase | do not move now | GenServer runtime registry belongs with runtime phases. |
| `apps/favn_legacy/lib/favn/connection/resolved.ex` | later phase | do not move now | Runtime resolved config is not needed for manifest-only compilation. |
| `apps/favn_legacy/lib/favn/connection/info.ex` | later phase | do not move now | Operator/runtime inspection shape, not authoring foundation. |
| `apps/favn_legacy/lib/favn/connection/validator.ex` | split later if needed | defer | Only static validation pieces may be reused later; do not move the runtime-resolution file wholesale. |

## New Modules To Write From Scratch

- `apps/favn_core/lib/favn/manifest.ex`
- `apps/favn_core/lib/favn/manifest/asset.ex`
- `apps/favn_core/lib/favn/manifest/pipeline.ex`
- `apps/favn_core/lib/favn/manifest/schedule.ex`
- `apps/favn_core/lib/favn/manifest/catalog.ex`
- `apps/favn_core/lib/favn/manifest/generator.ex`
- `apps/favn_core/lib/favn/graph.ex` or equivalent pure graph/catalog module
- `apps/favn_core/lib/favn/compiler/*` modules that accept explicit module lists and return manifest/domain data without boot-time caches

## Out Of Phase 2

- `apps/favn_legacy/lib/favn/application.ex`
- `apps/favn_legacy/lib/favn/run.ex`
- `apps/favn_legacy/lib/favn/run/**`
- `apps/favn_legacy/lib/favn/runtime/**`
- `apps/favn_legacy/lib/favn/submission.ex`
- `apps/favn_legacy/lib/favn/backfill.ex`
- `apps/favn_legacy/lib/favn/freshness.ex`
- `apps/favn_legacy/lib/favn/scheduler.ex`
- `apps/favn_legacy/lib/favn/scheduler/**`
- `apps/favn_legacy/lib/favn/storage.ex`
- `apps/favn_legacy/lib/favn/storage/**`
- `apps/favn_legacy/lib/favn/sql/render.ex`
- `apps/favn_legacy/lib/favn/sql/result.ex`
- `apps/favn_legacy/lib/favn/sql/error.ex`
- `apps/favn_legacy/lib/favn/sql/session.ex`
- `apps/favn_legacy/lib/favn/sql/materialization_planner.ex`
- `apps/favn_legacy/lib/favn/sql/incremental_window.ex`
- `apps/favn_legacy/lib/favn/sql/adapter/**`
- `apps/favn_legacy/lib/favn/sql_asset/runtime.ex`
- `apps/favn_legacy/lib/favn/sql_asset/renderer.ex`
- `apps/favn_legacy/lib/favn/sql_asset/error.ex`

## Test Migration Plan

Tests to move mostly as-is into `apps/favn_core/test`:

- `ref_test.exs`
- `window_test.exs`
- `triggers_schedules_test.exs`
- `graph_index_test.exs`
- `planner_test.exs`
- `sql_template_ir_test.exs`
- `sql_template_asset_ref_test.exs`
- `sql_dependency_inference_test.exs`

Tests to move mostly as-is into `apps/favn/test`:

- `asset_test.exs`
- `assets_test.exs`
- `multi_asset_test.exs`
- `pipeline_test.exs`
- `sql_asset_test.exs`
- `sql_dsl_test.exs`
- `connection_test.exs`
- `public_docs_test.exs`

Tests that should be rewritten rather than copied wholesale:

- `favn_test.exs` because the public facade contract changes from runtime execution to compile/introspection/manifest APIs
- any current assertions around global registry boot, scheduler defaults, SQL session/runtime execution, or runtime submission because those belong to later phases

Cross-app migration fixtures required by these tests should be extracted into `favn_test_support` rather than duplicated between owner apps.
