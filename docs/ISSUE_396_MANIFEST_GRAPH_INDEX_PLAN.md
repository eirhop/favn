# Issue 396 Manifest Graph/Index Planning Contract Plan

## Goal

Make the pinned manifest graph/index the single runtime planning contract. Runtime
planning should derive from the stored `%Favn.Manifest.Version{}` payload and must
not rebuild an equivalent-but-separate asset graph from authoring modules or global
cache state.

## Current State

- `Favn.Manifest.Graph` is the graph embedded in canonical manifests, but it only
  stores nodes, edges, and topological order.
- `Favn.Manifest.Index` validates manifest lookup tables, then rebuilds an
  `Favn.Assets.GraphIndex` from manifest assets.
- `Favn.Assets.GraphIndex` currently mixes four concerns: compiling authored
  asset modules, global `:persistent_term` caching, pure graph indexing, and graph
  query support for planning.
- `Favn.Assets.Planner` can plan from `asset_modules` or an `Assets.GraphIndex`.
  Orchestrator call sites pass `index.graph_index`, so runtime planning is pinned
  to the manifest version envelope but not directly to the manifest graph.
- Developer ergonomics such as `Favn.plan_asset_run/2`, `GraphIndex.load/1`, and
  graph inspection APIs still need an authoring-module entry point, but that path
  should stay outside runtime planning.

## Design

- Keep `Favn.Manifest.Graph` as the canonical persisted graph contract and make
  it authoritative for runtime validation.
- Add a pure manifest planning index in `favn_core`, owned by manifest/planning
  code. A suitable module name is `Favn.Manifest.PlanningIndex`.
- Build the planning index only from `%Favn.Manifest{}` or from an explicit
  `%Favn.Manifest.Graph{}` plus manifest assets.
- Store manifest assets in the planning index as `%Favn.Manifest.Asset{}` values,
  not compiled authoring asset maps.
- Validate that manifest assets and manifest graph agree exactly: same asset refs,
  same dependency edges, same deterministic topological order, no duplicates, no
  missing dependencies, and deterministic cycle errors.
- Move adjacency, transitive closure, topological rank, subgraph projection, and
  selected-ref behavior needed by planning into the pure manifest planning index.
- Keep authoring-module compilation and global cache mechanics in
  `Favn.Assets.GraphIndex` or a small authoring-facing wrapper. That wrapper may
  compile modules, run dependency inference, build a manifest, and delegate to the
  manifest planning contract, but runtime callers should not use it.
- Extract the planner implementation behind a manifest-index-first API, for
  example `Favn.Plan.Builder.plan(index, targets, opts)` or
  `Favn.Manifest.Planner.plan(index, targets, opts)`. The API should accept only a
  manifest planning/index contract plus target/window options.
- Keep `Favn.Assets.Planner` as an authoring convenience only if needed by public
  developer ergonomics. Its module-compilation path should delegate through
  `Favn.Manifest.Generator` and the manifest planning index instead of building an
  `Assets.GraphIndex` directly.
- Replace orchestrator planning call sites so they build `Favn.Manifest.Index`
  from the pinned `Favn.Manifest.Version` and pass the manifest planning contract
  into the planner.
- Introduce `Favn.Plan.NodeIdentity` as a small `favn_core` seam for planned-node
  identity with only manifest/planning-owned fields: `manifest_version_id`,
  `node_key`, `target_refs`, `planned_asset_refs`, `window`, and
  `execution_pool`.
- Produce `NodeIdentity` from the pinned manifest/index and plan. Do not add
  runner lifecycle state, run attempt state, execution IDs, storage IDs, or runner
  callbacks to this contract.
- Preserve the runner-facing `Favn.Contracts.RunnerWork` contract. Orchestrator
  may translate plan/node identity into existing `RunnerWork` fields and metadata,
  but this issue should not change runner API shape.

## Landing Slices

1. Add manifest planning index tests first.
2. Introduce `Favn.Manifest.PlanningIndex` with pure construction from manifest
   assets and graph.
3. Change `Favn.Manifest.Index` to own a `planning_index` instead of rebuilding an
   `Favn.Assets.GraphIndex`.
4. Extract planner internals to a manifest-index-first module while preserving the
   current plan output shape.
5. Move orchestrator planning call sites to the manifest planning contract.
6. Rework `Favn.plan_asset_run/2` and any remaining authoring conveniences so
   module compilation happens before manifest planning, not inside runtime
   planning.
7. Add `Favn.Plan.NodeIdentity` and have orchestrator/runtime admission derive it
   from the plan where identity is needed, without changing `RunnerWork`.
8. Remove or narrow runtime-facing uses of `Favn.Assets.GraphIndex` once all
   planning paths use the manifest contract.
9. Update docs and module docs to describe the split between authoring graph
   conveniences and manifest runtime planning.

## Tests

- Core manifest graph/index parity: building a planning index from a manifest with
  valid assets and graph preserves nodes, edges, topological order, adjacency,
  transitive upstream/downstream closures, and rank ordering.
- Contract validation: mismatched graph nodes, graph edges, duplicate asset refs,
  missing dependencies, and cycles fail with deterministic error shapes.
- Planner contract: planning succeeds with only a manifest planning index and does
  not require `asset_modules` or global cached graph state.
- Runtime safety: planning from a persisted manifest containing valid module atoms
  does not `Code.ensure_loaded/1` authoring modules.
- Orchestrator planning: manual run, pipeline run, rerun, and freshness planning
  call through `Favn.Manifest.Index` and the manifest planning contract.
- Authoring ergonomics: `Favn.plan_asset_run/2` still works from configured or
  discovered modules by generating a manifest first.
- Runner boundary: `RunnerWork` struct fields and runner submit APIs remain
  unchanged.

## Risks And Tradeoffs

- This is a foundational refactor and should land in small slices because graph
  construction, planner behavior, and orchestrator run submission are tightly
  connected.
- Renaming or replacing `graph_index` on `Favn.Manifest.Index` is a breaking
  internal/public-core change, but the project is pre-v1 and the clearer runtime
  contract is worth the churn.
- Keeping `Favn.Assets.GraphIndex` as an authoring convenience avoids unnecessary
  developer-facing breakage while still removing it from runtime planning.
- Issue #334 should remain separate. This plan should not redefine atom/string
  rehydration semantics, but tests should avoid relying on implicit atomization
  beyond the current manifest contract.

## Non-Goals

- Do not change `Favn.Contracts.RunnerWork`.
- Do not move orchestrator scheduling, storage, retry, or runner lifecycle state
  into `favn_core`.
- Do not use `Favn.Assets.GraphIndex` cache state to satisfy runtime planning.
- Do not solve manifest atom/string rehydration semantics from issue #334 here.
