# Runtime Windowing Foundation Plan (Temporary Working Context)

> Status: planning artifact for implementation slices.
> Scope: shared runtime capability for Elixir assets now and SQL assets later.

## Why this exists

This document captures the architectural direction and PR slicing plan for introducing
**runtime windowing** in Favn before `Favn.SQL` is introduced.

The goal is to make window semantics a shared core capability across asset kinds, rather
than an Elixir-only authoring detail.

## Current baseline (from code)

- Asset metadata currently includes `depends_on` but not a canonical `window_spec`.
- Planner and plan structs are keyed by `asset_ref` only.
- Pipeline DSL currently exposes `partition`, not runtime windowing.
- Pipeline resolver has a placeholder `runtime_window: nil` in `pipeline_ctx`.
- Runtime context includes `ctx.pipeline`, but no first-class `ctx.window` field.
- SQLite storage persists run snapshots (`run_blob`) without normalized per-window state.

## Foundation goals

Replace pipeline `partition` direction with **runtime windowing** that:

1. Works for current Elixir assets.
2. Reuses the same planner/runtime/storage model for future SQL assets.
3. Tracks execution/materialization by `{asset_ref, window_key}`.
4. Enables backfills and missed-window catchup on top of persisted window state.
5. Reframes freshness as policy over window/materialization history.

## Canonical domain model

### 1) Window spec (asset-level)

Represents how an asset is windowed.

Suggested canonical struct:

```elixir
%Favn.Window.Spec{
  kind: :hour | :day | :month,
  refresh_from: :hour | :day | :month | nil,
  lookback: non_neg_integer(),
  timezone: String.t()
}
```

`refresh_from` is intentionally narrow in the first foundation wave to avoid
over-design while still covering mixed-granularity planning requirements.

### 2) Anchor window (run-level request)

Represents window intent from scheduler/operator/pipeline.

```elixir
%Favn.Window.Anchor{
  kind: :hour | :day | :month,
  start_at: DateTime.t(),
  end_at: DateTime.t(),
  timezone: String.t(),
  key: Favn.Window.Key.t()
}
```

### 3) Runtime window (node-level execution)

Represents the concrete window used for one asset execution.

```elixir
%Favn.Window.Runtime{
  kind: :hour | :day | :month,
  start_at: DateTime.t(),
  end_at: DateTime.t(),
  timezone: String.t(),
  key: Favn.Window.Key.t(),
  anchor_key: Favn.Window.Key.t()
}
```

### 4) Windowed planner node key

Replace planner identity from `asset_ref` to:

```elixir
{asset_ref, window_key}
```

This key is the dedupe identity for execution and reruns.

`window_key` should be canonical and structured internally. String keys should be
derived only for storage/indexing/display as needed.

Recommended key shape:

```elixir
%{
  kind: :hour | :day | :month,
  start_at_us: integer(),
  timezone: String.t()
}
```

### 5) Persisted window/materialization state

Persist enough to answer:

- Which windows ran?
- Which succeeded/failed?
- When materialized?
- Which windows are missing?
- What should be backfilled?

## Public DSL direction

## Asset DSL (`use Favn.Assets`)

Add validated function-level `@window`:

```elixir
@window hourly()
@window daily()
@window monthly(refresh_from: :day, lookback: 2)
```

Helpers should live in a reusable shared module (`Favn.Window`), not in Elixir-only internals.

Validation (compile-time):

- `@window` must attach immediately above an `@asset` function.
- Max one `@window` per asset function.
- `lookback >= 0`.
- `refresh_from` (when present) must be valid for declared granularity.

## Pipeline DSL (`use Favn.Pipeline`)

Replace `partition` with `window`.

```elixir
pipeline :daily_sales do
  asset {SalesAssets, :sales_daily}
  deps :all
  schedule {Schedules, :daily_default}
  window :day
end
```

Pipeline `window` should be an optional default anchor-window policy.
Manual runs and backfills can supply explicit anchor windows/ranges.

## Runtime context contract

Expose both:

- `ctx.pipeline.anchor_window` (requested run window intent)
- `ctx.window` (concrete window for this asset execution)

This is required for mixed granularity and SQL parity.

## Planner/runtime behavior

### Window-aware planning

1. Resolve target assets and dependencies as today.
2. Resolve one anchor window (or a range for backfill).
3. Expand each asset into concrete runtime windows from `window_spec`.
4. Build dependency graph over `{asset_ref, window_key}` nodes.
5. Dedupe repeated node keys globally.

### Mixed granularity example

For daily anchor:

- daily asset => 1 daily window node
- hourly upstream => 24 hourly window nodes
- monthly(refresh_from: :day, lookback: 2) => current month + previous 2 month nodes

### Rerun/manual/backfill

Use the same model:

- manual run: explicit anchor window (or default)
- rerun: replay/select subset of same node keys
- backfill: anchor window range expands into many node keys, then dedupes

## Storage direction

Keep run snapshots, add normalized window state.

Persisted concepts to add (exact table names deferred until planner/runtime identity settles):

1. append-only run history by asset/window
2. latest projection/state by asset/window
3. (optional) run-to-windowed-node relation for fast querying

## API direction

### `Favn.plan_pipeline/2`

Support explicit `anchor_window` input and later range/backfill options.

### `Favn.run_pipeline/2`

Support `anchor_window` override while pipeline `window` is default policy.

### `Favn.run_asset/2`

Support explicit window input for direct asset execution.

### Future

Add backfill APIs that reuse planner/runtime model:

- `Favn.backfill_pipeline/2`
- `Favn.backfill_asset/2`

## Freshness direction

Freshness should become a thin policy layer over:

- persisted window state
- materialization timestamps/history
- recency/staleness rules

This avoids a large separate DSL concept before foundation state exists.

## Suggested PR slices (ordered)

- [x] 1. **Domain primitives (no behavior change)**
  - Add `Favn.Window.*` structs + validators + key helpers.
  - Include deterministic key encode/decode helpers for storage/indexing.

- [x] 2. **Asset DSL window annotation**
  - Add `@window` capture/validation in `Favn.Assets`.
  - Add `window_spec` to `Favn.Asset`.

- [x] 3. **Pipeline DSL migration**
  - Add `window` clause.
  - Remove legacy `partition` clause and keep window-only semantics.

- [x] 4. **Runtime context split**
  - Add `ctx.window`.
  - Add `ctx.pipeline.anchor_window`.
  - Keep runtime context deterministic per node key.

- [x] 5. **Window-aware planner v1**
  - Introduce windowed node keys `{asset_ref, window_key}`.
  - Implement hourly/daily/monthly expansion + lookback.

- [x] 6. **Runtime execution adaptation**
  - Switch step state/coordinator identity from `ref` to `node_key`.
  - Preserve retry/rerun semantics per node key.

- [x] 7. **Storage window state foundation**
  - Add SQLite migrations + adapter writes for window run/state records.

- [x] 8. **Public API window options**
  - Add `anchor_window` options to plan/run APIs.

- [x] 9. **Backfill scaffold**
  - Add API and internal range expansion over same planner.

- [x] 10. **Freshness policy layer**
  - Implement freshness checks against persisted window/materialization state.

- [x] 11. **SQL-readiness seam**
  - Ensure future SQL assets compile into same asset/window/planner model.

## Non-goals for first implementation waves

- Full schedule engine semantics.
- Full physical storage partition/layout DSL.
- Rich freshness DSL design before state model is in place.

## Favn SQL alignment (forward-looking constraints)

This runtime-windowing foundation must support later `Favn.SQLAssets` goals:

- asset-first semantics
- multi-asset authoring in one module
- plain SQL-first authoring with compiler-driven inference/validation
- typed by asset/source identity
- same window semantics/planner identity/persisted state as Elixir assets
- simpler than dbt while staying Elixir-native in structure

Design requirement: SQL asset compilation should emit the same internal asset/window
model used by Elixir assets, so planner/runtime/storage behavior is shared.

## Implementation notes for AI agents

- Treat this as a core identity shift: `asset_ref` -> `{asset_ref, window_key}`.
- Keep orchestration concerns outside business asset function bodies.
- Prefer additive slices with compatibility shims removed quickly once migrated.
- Keep docs updated in `lib/favn.ex`, `README.md`, and `FEATURES.md` as slices land.

---

Temporary planning file; may be removed or merged into permanent architecture docs once
runtime windowing is fully implemented.
