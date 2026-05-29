# Working Pipeline Execution Plan (v0.3)

## 1) Recommendation summary

"Working pipeline execution" in Favn v0.3 should mean pipelines are fully usable as an **operator execution surface** while remaining a **thin composition layer** over the existing asset planner/runtime. Pipelines should not introduce a second orchestration model. They should resolve selections to asset refs, delegate graph planning to the current planner, and run through the same runtime engine and persistence model as `run_asset/2`.

This should be built now because scheduler/trigger work depends on it. A scheduler without solid pipeline execution only moves uncertainty earlier in the flow. Once `run_pipeline/2` is deterministic, observable, and failure-tolerant (retry/cancel/timeout/rerun/resume), scheduler runtime can simply submit the same run requests with different initiation metadata.

The v0.3 cut should focus on predictable behavior and clear contracts: deterministic pipeline resolution, explicit dependency policy, stable context projection (`ctx.pipeline`), robust anchor-window and backfill semantics, and lineage-safe rerun/resume.

## 2) Assumptions and open questions

### Assumptions

1. Pipeline definitions already exist (or are partially present) and can be loaded deterministically by module/ref.
2. `Favn.plan_pipeline/2`, `Favn.run_pipeline/2`, and `Favn.backfill_pipeline/2` are intended public APIs for v0.3.
3. The planner is already window-aware and supports node identity `{asset_ref, window_key}`.
4. Runtime already supports retry/cancel/timeout/rerun for generic runs and can be reused for pipeline runs.
5. Persistence already stores run snapshots and node/window result records needed by resume logic.

### Open questions (must be resolved before coding)

1. **Selector precedence:** if pipeline defines both explicit refs and tag/module selectors, what is merge order and duplicate policy?
2. **`deps` default for pipeline runs:** should pipeline default to `:all` (recommended) or inherit per-pipeline config?
3. **Empty resolution policy:** should empty selection error by default (`:empty_pipeline_selection`) with opt-in allow-empty?
4. **Rerun scope:** should rerun pipeline use original resolved refs snapshot, or re-resolve current pipeline definition? (recommended: use original snapshot by default for reproducibility).
5. **Backfill submission shape:** one run containing all anchors (recommended for shared dedupe) vs one run per anchor.
6. **Cancellation semantics in backfill runs:** cancel entire run always, no per-anchor cancellation in v0.3.
7. **Timeout semantics:** timeout at run-level only in v0.3; no per-anchor timeout split.

## 3) Desired user-facing behavior

### `plan_pipeline/2`

- Accepts a pipeline identifier plus options.
- Resolves pipeline selection deterministically into target asset refs.
- Applies `deps` policy to derive full planned node set using existing planner.
- Supports optional `anchor_window` and returns expanded node keys for windowed assets.
- Returns a plan structure with:
  - resolved target refs (post-selection)
  - target node keys
  - full planned node stages
  - dependency edges
  - normalized pipeline metadata snapshot

### `run_pipeline/2`

- Primary operator entrypoint for pipeline execution.
- Performs `plan_pipeline` internally and submits resulting plan to runtime.
- Persists pipeline provenance on run:
  - pipeline id/name/version snapshot
  - resolved refs snapshot
  - `deps` policy
  - anchor_window (if any)
  - request params/initiation metadata
- Returns `{:ok, run_id}` (async) and supports await/get/list same as asset runs.

### `backfill_pipeline/2`

- Accepts pipeline + backfill range.
- Expands anchors deterministically and plans across all anchors in one run.
- Deduplicates shared upstream node keys across targets/anchors.
- Persists backfill provenance (`range`, expanded anchors, and pipeline context).

### rerun/cancel/timeout/retry semantics for pipeline runs

- **Retry:** step-level retry unchanged; pipeline runs inherit same runtime retry policy.
- **Cancel:** cancels whole run; status becomes `:cancelled` with persisted reason.
- **Timeout:** run-level timeout transitions to `:timed_out` with persisted context.
- **Rerun:** supports:
  - `:resume_from_failure` (default)
  - `:exact_replay`
- **Resume source of truth:** persisted run snapshot (`resolved_refs`, plan node keys/stages, anchor info), not live pipeline module resolution.

### `ctx.pipeline`

Each executed step should receive stable pipeline context:

```elixir
%{
  id: MyApp.Pipelines.Daily,
  run_kind: :pipeline | :pipeline_backfill,
  resolved_refs: [...],
  deps: :all | :none,
  anchor_window: %{kind: :day, ...} | nil,
  backfill_range: %{...} | nil,
  anchor_ranges: [%{...}] | nil,
  params: %{...}
}
```

### windowed pipeline runs

- Non-windowed assets execute with `ctx.window == nil`.
- Windowed assets execute with concrete window and deterministic `window_key`.
- Mixed pipelines supported in one run via shared node-key model.

## 4) Proposed architecture

### Architectural stance

Keep pipelines as a composition/resolution layer. Do **not** add pipeline-specific planner/runtime engines.

### Module boundaries

1. **`Favn.Pipeline.Definition`**
   - Load/normalize pipeline definition.
   - Validate schema and static options.

2. **`Favn.Pipeline.Resolver`**
   - Resolve selectors to deterministic ref list.
   - Handle dedupe and ordering.
   - Produce `resolved_refs` snapshot.

3. **`Favn` public API (`plan_pipeline/2`, `run_pipeline/2`, `backfill_pipeline/2`)**
   - Normalize options.
   - Delegate definition + resolver.
   - Delegate planning to existing planner.
   - Delegate run submission to existing runtime submit path.

4. **Existing planner (`Favn.Planner*`)**
   - Input: `resolved_refs`, `deps`, optional `anchor_window`/anchor expansion.
   - Output: node-key-aware plan.

5. **Existing runtime submission/coordinator**
   - Input: plan + pipeline run context/provenance.
   - Runtime behavior identical to asset runs.

6. **Run persistence (`Favn.Run`, storage adapters)**
   - Persist pipeline snapshot and backfill provenance required for resume/replay.
   - Ensure rerun uses persisted snapshot deterministically.

### Flow

`pipeline definition -> resolver -> planner -> runtime submit -> run snapshot persistence -> runtime execution -> terminal persisted state`

## 5) Data flow walkthroughs

### A) Non-windowed pipeline run

```elixir
# input
Favn.run_pipeline(MyApp.Pipelines.DailyOps, params: %{requested_by: "alice"})

# 1. definition
{:ok, defn} = Favn.Pipeline.Definition.fetch(MyApp.Pipelines.DailyOps)

# 2. resolve selection
{:ok, resolved_refs} =
  Favn.Pipeline.Resolver.resolve(defn.selection, registry: Favn.Registry, deterministic?: true)
# => [
#   {MyApp.Assets.Raw, :ingest_orders},
#   {MyApp.Assets.Marts, :daily_sales}
# ]

# 3. plan via existing planner
{:ok, plan} =
  Favn.Planner.plan_targets(resolved_refs,
    deps: :all,
    anchor_window: nil
  )

# 4. build pipeline context snapshot
pipeline_ctx = %{
  id: MyApp.Pipelines.DailyOps,
  run_kind: :pipeline,
  resolved_refs: resolved_refs,
  deps: :all,
  anchor_window: nil,
  backfill_range: nil,
  params: %{requested_by: "alice"}
}

# 5. submit runtime run
{:ok, run_id} = Favn.Runtime.submit(plan: plan, pipeline: pipeline_ctx)
```

### B) Windowed pipeline run with explicit `anchor_window`

```elixir
anchor = %{kind: :day, at: ~U[2026-03-10 00:00:00Z], timezone: "Etc/UTC"}

{:ok, run_id} =
  Favn.run_pipeline(MyApp.Pipelines.DailySales,
    anchor_window: anchor,
    deps: :all
  )

# planner expands windowed assets into node keys
plan.target_node_keys
# => [
#   {{MyApp.Assets.Sales, :stg_orders}, "day:2026-03-09:Etc/UTC"},
#   {{MyApp.Assets.Sales, :fct_sales}, "day:2026-03-09:Etc/UTC"}
# ]

# runtime step ctx example
ctx = %{
  window: %{kind: :day, from: ~U[2026-03-09 00:00:00Z], to: ~U[2026-03-10 00:00:00Z]},
  pipeline: %{anchor_window: anchor, id: MyApp.Pipelines.DailySales, ...}
}
```

### C) `backfill_pipeline/2` over range

```elixir
range = %{
  kind: :day,
  start_at: ~U[2026-03-01 00:00:00Z],
  end_at: ~U[2026-03-04 00:00:00Z],
  timezone: "Etc/UTC"
}

{:ok, run_id} = Favn.backfill_pipeline(MyApp.Pipelines.DailySales, range: range)

# anchor expansion (deterministic)
anchors = [
  %{kind: :day, at: ~U[2026-03-01 00:00:00Z], timezone: "Etc/UTC"},
  %{kind: :day, at: ~U[2026-03-02 00:00:00Z], timezone: "Etc/UTC"},
  %{kind: :day, at: ~U[2026-03-03 00:00:00Z], timezone: "Etc/UTC"}
]

# planner called once with anchor_ranges
{:ok, plan} = Favn.Planner.plan_targets(resolved_refs,
  deps: :all,
  anchor_ranges: anchors
)

# dedupe by node key across shared deps and repeated refs
plan.node_count # unique {ref, window_key} only

# persisted provenance
run.backfill.range == range
run.pipeline.backfill_range == range
run.pipeline.anchor_ranges == anchors
```

### D) Rerun failed pipeline run

```elixir
{:ok, original} = Favn.get_run(run_id)
# original.status == :error

{:ok, rerun_id} = Favn.rerun_run(run_id, mode: :resume_from_failure)

# rerun source = persisted pipeline + plan snapshot
# no live re-resolution of pipeline module by default

{:ok, rerun} = Favn.await_run(rerun_id)
rerun.parent_run_id == run_id
rerun.pipeline.resolved_refs == original.pipeline.resolved_refs
```

## 6) Required changes by area

1. **Pipeline definition/fetch**
   - Normalize pipeline struct and enforce deterministic schema.
   - Persist enough metadata to reproduce run intent.

2. **Resolver**
   - Deterministic selector evaluation order.
   - Dedupe without losing stable order.
   - Explicit empty-selection behavior.

3. **Planner integration**
   - Route pipeline refs into existing target planner.
   - Validate `deps` semantics (`:all`/`:none` initially).
   - Apply anchor window/range hooks already present.

4. **Runtime submission**
   - Make `run_pipeline/2` call common submit path.
   - Attach normalized pipeline context/provenance onto run.

5. **Run persistence**
   - Ensure pipeline context/backfill anchor data are persisted and restorable.
   - Keep storage adapter contracts aligned (memory + sqlite).

6. **Run lineage/rerun handling**
   - Ensure rerun modes operate on persisted plan/pipeline snapshot.
   - Preserve parent-child lineage metadata.

7. **Context building**
   - Standardize `ctx.pipeline` map fields and shape.
   - Keep `ctx.window` and `ctx.pipeline.anchor_window` coherent.

8. **Validation and errors**
   - Add explicit error types/messages for invalid selectors, deps, anchor windows, and empty result sets.

9. **Tests**
   - Unit: definition + resolver determinism.
   - Integration: plan/run/backfill/rerun end-to-end with persistence.
   - Property-style checks for dedupe/order invariants where practical.

10. **Docs**
   - Update `lib/favn.ex`, `README.md`, and `FEATURES.md` with final semantics.

## 7) Edge cases and invariants

### Invariants

1. Pipeline resolution is deterministic for same registry + pipeline definition.
2. Planner/runtime remain single source of orchestration truth.
3. Node execution identity is always `{asset_ref, window_key}`.
4. A node key executes at most once per run.
5. Rerun/resume uses persisted snapshots unless explicitly configured otherwise.

### Tricky cases

- **Empty pipeline selection:** default error; include actionable message and selector summary.
- **Duplicate refs:** dedupe in resolver preserving first occurrence order.
- **Mixed windowed/non-windowed assets:** allowed; non-windowed use `window_key=nil`.
- **Shared dependencies:** dedupe at plan node-key level, not only ref level.
- **Conflicting selectors:** deterministic merge + explicit conflict/unknown selector errors.
- **Invalid anchor windows/ranges:** fail fast before planning; include timezone/kind details.
- **Backfill dedupe:** dedupe across both dependency fan-in and multi-anchor overlap.
- **Recovery behavior:** resume must not depend on current registry ordering if snapshot exists.

## 8) Acceptance criteria

1. `plan_pipeline/2` returns stable equivalent plans across repeated calls with same inputs.
2. `run_pipeline/2` executes successfully for non-windowed pipeline with deps `:all`.
3. `run_pipeline/2` supports explicit `anchor_window` and executes windowed assets with expected `ctx.window`.
4. `ctx.pipeline` fields are present and match persisted run pipeline snapshot.
5. `backfill_pipeline/2` expands anchors deterministically and persists range + anchor provenance.
6. Shared upstream dependencies execute once per unique node key in a run.
7. Invalid pipeline definitions/selectors produce typed, actionable errors.
8. Empty selection behavior is explicit and tested.
9. Cancellation, timeout, retry, and rerun (`:resume_from_failure`, `:exact_replay`) work on pipeline runs.
10. Rerun/resume uses persisted snapshot semantics and preserves lineage metadata.
11. Memory and SQLite adapters pass pipeline execution integration tests.
12. Public docs reflect final semantics and examples.

## 9) Delivery plan (incremental PR slices)

### Slice 1 — Pipeline resolution contract

- **Goal:** deterministic definition + resolver semantics.
- **Why now:** everything depends on stable target refs.
- **Modules:** `Favn.Pipeline.Definition`, `Favn.Pipeline.Resolver`, validations.
- **Tests:** resolver ordering, dedupe, empty selection, invalid selectors.
- **Risks:** hidden nondeterminism from registry iteration.

### Slice 2 — Planning integration for pipeline APIs

- **Goal:** wire `plan_pipeline/2` to existing planner with `deps` + anchor support.
- **Why now:** plan quality gates runtime reliability.
- **Modules:** `Favn` public APIs, planner adapter glue.
- **Tests:** plan snapshots for non-windowed/windowed runs.
- **Risks:** accidental divergence from `run_asset` planner semantics.

### Slice 3 — Runtime submission + persisted pipeline context

- **Goal:** `run_pipeline/2` end-to-end submission and persistence.
- **Why now:** operator-facing core path.
- **Modules:** runtime submit path, run struct, storage serialization.
- **Tests:** run submission, context projection, await/get/list pipeline metadata.
- **Risks:** persistence schema mismatches between memory/sqlite.

### Slice 4 — Backfill pipeline execution

- **Goal:** `backfill_pipeline/2` with deterministic anchor expansion and dedupe.
- **Why now:** v0.3 windowing usability target.
- **Modules:** API glue, planner anchor-range integration, run provenance.
- **Tests:** range expansion, dedupe across anchors, provenance persistence.
- **Risks:** large backfills causing unexpected plan size/perf.

### Slice 5 — Pipeline run controls (retry/cancel/timeout/rerun/resume)

- **Goal:** guarantee pipeline runs obey existing runtime controls.
- **Why now:** operational readiness.
- **Modules:** runtime control endpoints, rerun lineage paths.
- **Tests:** failure injection + resume/replay behavior; timeout/cancel paths.
- **Risks:** replay/resume confusion if snapshot contracts are incomplete.

### Slice 6 — Docs + hardening pass

- **Goal:** finalize user contract and close ambiguity.
- **Why now:** reduce future scheduler coupling risk.
- **Modules:** `lib/favn.ex`, `README.md`, `FEATURES.md`.
- **Tests:** doctest/examples smoke checks if present.
- **Risks:** docs drifting from implementation if done too early.

## 10) Recommendation on what to defer

Defer all scheduler and external trigger runtime work until pipeline execution acceptance criteria are met. Specifically keep out of this feature:

- Scheduler runtime/polling/API triggers (depends on reliable `run_pipeline/2` semantics)
- SQL/DuckDB assets (reuse runtime model later; avoid mixing concerns now)
- Distributed execution and queueing policy enhancements (separate reliability track)
- UI-specific APIs beyond persisted contracts already needed for lineage/provenance

This keeps v0.3 focused: one execution model, one planner/runtime, one operator entrypoint (`run_pipeline/2`) with reliable manual + window + backfill operation.
