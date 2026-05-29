# Working Scheduler Trigger Runtime Plan (v0.3)

## 1) Recommendation summary

In Favn v0.3, “working scheduler trigger runtime” should mean:

- code-defined pipeline schedules are discovered at boot,
- active schedules are evaluated on a single node with deterministic due-time logic,
- due occurrences submit runs through `Favn.run_pipeline/2` (no separate execution engine),
- overlap and missed policies are enforced predictably,
- minimal scheduler cursor/state is persisted so restarts are reliable,
- scheduler-trigger provenance is persisted into run pipeline metadata (`ctx.pipeline.trigger`, `%Favn.Run{}` projection).

The scheduler remains a **thin trigger layer** over existing pipeline resolver/planner/runtime APIs.

## 2) Assumptions and open questions

### Assumptions

1. `run_pipeline/2` remains the primary operator-facing execution entrypoint and is reused verbatim for scheduled submissions.
2. v0.3 scheduler is single-node only; no leader election, leases, or distributed dedupe.
3. Schedule definitions already exist through pipeline DSL (`schedule ...`) and `Favn.Triggers.Schedule`.
4. Runtime/storage already persist enough run lineage to inspect scheduled runs once trigger metadata is passed in.
5. Backfill remains explicit and user-initiated via `Favn.backfill_pipeline/2`.

### Open questions to close before coding

1. **First-boot missed behavior:** Should persisted missed-policy replay happen on first scheduler boot (no state row yet)?
   - Recommendation: **no** (treat as no-history baseline).
2. **Tick cadence:** fixed cadence (e.g. every 15s) vs dynamic next-due timer.
   - Recommendation: dynamic next-due scheduling per pipeline (with short safety floor).
3. **Cron evaluator dependency:** keep current lightweight parser + custom evaluator vs add external cron library.
   - Recommendation: keep internal evaluator for v0.3 for determinism and dependency minimization.
4. **Submission idempotency token:** include a deterministic schedule occurrence key in trigger metadata now?
   - Recommendation: yes (`occurrence_key`) to support future dedupe/hardening.

## 3) Final user-facing behavior

### Active vs inactive schedules

- Add `active: true | false` to schedule model.
- Default: `true`.
- `active: false` means:
  - schedule is discovered and validated,
  - schedule state row may still exist,
  - scheduler does not evaluate or submit runs for that pipeline.

### Schedule discovery

- Scheduler discovers pipeline modules from `config :favn, pipeline_modules: [...]`.
- Only modules exporting `__favn_pipeline__/0` are accepted.
- Pipeline definitions are resolved at boot for scheduler normalization/validation.
- Pipelines without `schedule` are ignored by runtime evaluation but still valid for manual `run_pipeline/2`.

### Due occurrence calculation

For each active scheduled pipeline, the scheduler computes:

1. `now` in schedule timezone.
2. `latest_due_at` = greatest cron occurrence `<= now`.
3. Candidate due occurrences between persisted cursor and `latest_due_at` based on missed policy.

Deterministic baseline:

- No implicit historical catch-up before scheduler state exists.
- On first observed evaluation without state:
  - set evaluation cursor to `latest_due_at`,
  - submit on-time run for `latest_due_at` only if current evaluation tick crosses that occurrence boundary.

### Overlap policies

Given one schedule stream per pipeline:

- `:forbid`
  - if a run is in-flight for this schedule stream, do not submit another.
  - due occurrence is dropped (or advanced per missed policy on next evaluation).
- `:allow`
  - always submit due occurrence regardless of in-flight runs.
- `:queue_one`
  - if in-flight exists, keep exactly one queued occurrence (latest due wins).
  - when in-flight clears, queued occurrence is submitted immediately.

### Missed policies

Let “missed” mean due occurrences between last submitted/evaluated cursor and current `latest_due_at`.

- `:skip`
  - submit only current/latest due occurrence (or none if overlap blocks and no queue policy).
- `:one`
  - submit at most one recovery occurrence (latest missed) per evaluation cycle.
- `:all`
  - submit all due occurrences in order, subject to overlap policy.

### Missed-policy scope recommendation

Missed policies should be honored **only when persisted scheduler state exists**.

Rationale:

- avoids surprising catch-up after first deploy,
- aligns with “no `start_at` yet” decision,
- keeps historical recovery explicit through `backfill_pipeline/2`.

## 4) Proposed architecture

## Module boundaries

1. `Favn.Scheduler`
   - public scheduler introspection/control façade (minimal in v0.3).
   - functions like `list_scheduled_pipelines/0`, `get_schedule_state/1` (optional for v0.3 if time permits).

2. `Favn.Scheduler.Supervisor`
   - owns scheduler runtime tree.
   - started by `Favn.Application` after registry/graph/storage boot.

3. `Favn.Scheduler.Registry`
   - boot-time discovery + normalization of scheduled pipeline specs.
   - immutable in-memory map: `pipeline_module => %ScheduledPipeline{}`.

4. `Favn.Scheduler.Runtime` (GenServer)
   - central single-node evaluator.
   - manages timers, computes due occurrences, enforces overlap/missed logic.
   - delegates persistence to storage boundary and run submissions to `Favn.run_pipeline/2`.

5. `Favn.Scheduler.State`
   - pure state model + transition functions.
   - no side effects; deterministic policy logic.

6. `Favn.Scheduler.Storage` (facade)
   - scheduler persistence boundary (parallel to `Favn.Storage` style).
   - adapter callbacks for loading/upserting schedule runtime state.

7. `Favn.Scheduler.Storage.Adapter.Memory`
   - ETS/Agent-backed volatile scheduler state.

8. `Favn.Scheduler.Storage.Adapter.SQLite`
   - durable scheduler state in same DB as run storage.

9. `Favn.Scheduler.Trigger`
   - builds canonical trigger metadata payload for `run_pipeline/2`.

### Why this split

- Keeps runtime loop small and explicit.
- Keeps policy logic testable via pure transitions.
- Keeps persistence pluggable and aligned with existing adapter style.
- Avoids creating a second planner/executor model.

## 5) Discovery/config model

### Config shape

```elixir
config :favn,
  pipeline_modules: [MyApp.Pipelines.DailySales, MyApp.Pipelines.HourlyOps],
  scheduler: [
    enabled: true,
    default_timezone: "Etc/UTC",
    tick_floor_ms: 1_000
  ]
```

### Startup behavior

1. Read `pipeline_modules` (default `[]`).
2. Validate each module exports `__favn_pipeline__/0`.
3. Resolve/normalize pipeline definition (including schedule timezone defaults).
4. Build scheduler registry for pipelines with non-nil schedules.
5. Load persisted scheduler state rows for discovered scheduled pipelines.
6. Start runtime timers only for active schedules.

### Validation decisions

- Invalid pipeline module in `pipeline_modules`: **fail boot**.
- Invalid scheduled pipeline definition (cron/timezone/policies): **fail boot**.
- Inactive schedules: **still discovered and validated**.
- Runtime state includes **only pipelines with schedule clause**.

## 6) Schedule model changes (`Favn.Triggers.Schedule`)

### Additions

- Add field/typespec: `active :: boolean()`.
- Add default `active: true`.
- Support option in both:
  - inline schedule options (`schedule cron: ...`),
  - named schedules (`schedule :daily, cron: ...`).

### Validation rules

- `active` must be boolean.
- unsupported opts still error.
- duplicate opts still error.

### Named schedule behavior

- Pipeline referencing named schedule inherits the named schedule `active` value.
- v0.3: **no pipeline-level override** for named schedule `active` (defer).

### Recommendation

Keep active semantics on schedule entity itself (inline + named) for consistency and simpler mental model.

## 7) Scheduler state and persistence

### Minimal persisted state (per scheduled pipeline)

Recommended row shape:

- `pipeline_module` (primary identity)
- `schedule_ref` (named ref or synthetic inline key)
- `last_evaluated_at` (UTC datetime)
- `last_due_at` (UTC datetime, latest due occurrence computed)
- `last_submitted_due_at` (UTC datetime or nil)
- `in_flight_run_id` (string or nil)
- `queued_due_at` (UTC datetime or nil; for `:queue_one`)
- `updated_at` (UTC datetime)

Optional but recommended:

- `version` (optimistic update counter for safer future evolution)

### Storage boundary

Use a new scheduler storage boundary (`Favn.Scheduler.Storage`) instead of expanding run storage callbacks, because scheduler state has different lifecycle/query patterns.

### Adapter support in v0.3

- Memory: yes (volatile state, no restart recovery guarantees).
- SQLite: yes (durable state, restart recovery for missed/overlap/queue_one behavior).

### Recovery on restart

At boot:

1. Load discovered schedule set.
2. Load persisted state by schedule identity.
3. Reconcile in-flight run ids:
   - if `in_flight_run_id` terminal/not-found => clear in-flight.
   - if running => keep as in-flight.
4. Resume evaluation from persisted cursor.

## 8) Trigger metadata and run provenance

Scheduled submissions must call `Favn.run_pipeline/2` with `trigger:` map and optional `anchor_window`.

Recommended trigger payload:

```elixir
%{
  kind: :schedule,
  scheduler: %{node: node(), version: "v0.3"},
  pipeline: %{module: MyApp.Pipelines.DailySales, id: :daily_sales},
  schedule: %{
    id: :daily,
    ref: {MyApp.Schedules, :daily},
    cron: "0 2 * * *",
    timezone: "Etc/UTC",
    overlap: :forbid,
    missed: :skip
  },
  occurrence: %{
    due_at: ~U[2026-04-08 02:00:00Z],
    occurrence_key: "schedule:MyApp.Pipelines.DailySales:2026-04-08T02:00:00Z",
    recovery: :on_time | :missed
  },
  evaluated_at: DateTime.utc_now()
}
```

Notes:

- `occurrence_key` should be deterministic and stable.
- This payload naturally projects to `ctx.pipeline.trigger` and persisted run pipeline metadata.

## 9) Window and anchor behavior

### Mapping pipeline `window` to `anchor_window`

- If scheduled pipeline declares `window: :hour | :day | :month`, scheduler derives `anchor_window` from due occurrence:
  - `kind` = pipeline window atom
  - `at` = due occurrence instant
  - `timezone` = schedule timezone
- Scheduler passes derived anchor to `run_pipeline/2`.

### Allowed window values

Keep v0.3 pipeline window values as existing atoms (`:hour`, `:day`, `:month`) for compatibility with current planner/runtime contracts.

### No window on scheduled pipeline

- Submit with `anchor_window: nil`.
- Run executes non-windowed assets normally.

### Incompatible/ambiguous config

- If pipeline window atom unsupported by anchor builder: fail boot validation.
- If schedule timezone invalid: fail boot validation.
- If schedule exists but window omitted: valid (non-windowed scheduled run).

## 10) Boot and runtime lifecycle

1. **Application start** (`Favn.Application`)
   - existing asset/graph/storage preflight,
   - start scheduler supervisor after storage/runtime manager.

2. **Pipeline discovery**
   - load `pipeline_modules`, fetch definitions, validate schedule clauses.

3. **Schedule normalization**
   - apply default timezone,
   - normalize `active/missed/overlap/window`.

4. **State recovery**
   - load persisted schedule state,
   - reconcile in-flight run ids via `Favn.get_run/1`.

5. **Timer setup**
   - compute next due instant per active schedule,
   - schedule earliest wake-up.

6. **Occurrence evaluation**
   - at wake-up, compute due occurrences up to `now`,
   - apply missed policy,
   - apply overlap policy.

7. **Run submission**
   - build trigger metadata,
   - derive `anchor_window` if needed,
   - call `Favn.run_pipeline/2`.

8. **State persist**
   - persist cursor + in-flight/queue transitions after each decision/submission.

9. **Runtime polling/reconciliation**
   - periodic check of in-flight run terminality to release overlap locks and submit queued occurrence.

10. **Restart recovery**
   - repeat steps 2–9 with persisted cursor continuity.

## 11) Edge cases and invariants

### Edge cases

1. **Restart during in-flight run**
   - invariant: do not submit duplicate for same due occurrence solely because process restarted.
2. **Crash after submit before state persist**
   - residual duplicate risk exists in v0.3; mitigated by immediate post-submit state write and deterministic `occurrence_key` provenance.
3. **Inactive schedule at boot**
   - validated, loaded, no timers.
4. **Active -> inactive across deploy**
   - scheduler stops evaluating; persisted row retained.
5. **Named schedule reused by multiple pipelines**
   - each pipeline has independent runtime state stream (identity includes pipeline module).
6. **Invalid cron/timezone**
   - boot fails fast.
7. **`queue_one` with many missed occurrences**
   - queue keeps one occurrence only (latest), consistent with policy intent.
8. **Memory adapter restart**
   - no persisted cursor; behaves as first boot baseline (no implicit catch-up).

### Core invariants

- Scheduler never executes assets directly; only submits `run_pipeline/2`.
- Scheduler never auto-backfills history beyond persisted cursor semantics.
- Per pipeline schedule stream, max queued occurrence under `:queue_one` is 1.
- State transitions are persisted before/after submission boundaries as defined.
- Single-node runtime owns all scheduling decisions.

## 12) Acceptance criteria for v0.3

1. Scheduler process starts from `Favn.Application` when enabled.
2. Scheduled pipelines are discovered from `pipeline_modules` and validated at boot.
3. `active: false` schedules never submit runs.
4. `active: true` schedules submit due runs via `Favn.run_pipeline/2`.
5. Trigger metadata is present in `ctx.pipeline.trigger` and persisted run pipeline metadata.
6. Overlap policies `:forbid`, `:allow`, `:queue_one` behave as specified.
7. Missed policies `:skip`, `:one`, `:all` behave as specified when persisted state exists.
8. First boot does not perform historical catch-up.
9. SQLite-backed scheduler state survives restart and resumes cursors/queue/in-flight reconciliation.
10. Memory adapter works but does not recover scheduler cursor across restart.
11. Invalid pipeline/schedule config fails boot with explicit error.
12. End-to-end tests cover policy matrix and restart recovery paths.

## 13) Recommended delivery plan / PR slices

1. **PR 1 — Schedule model + docs**
   - add `active` to `Favn.Triggers.Schedule` + validations,
   - update DSL docs (`lib/favn.ex`, `README.md`),
   - unit tests for schedule normalization.

2. **PR 2 — Pipeline discovery and scheduler boot wiring**
   - add `pipeline_modules` config consumption,
   - add scheduler supervisor skeleton,
   - fail-fast validation at boot.

3. **PR 3 — Scheduler storage boundary + adapters**
   - introduce `Favn.Scheduler.Storage` behaviour/facade,
   - memory adapter implementation,
   - SQLite migration + adapter implementation.

4. **PR 4 — Runtime evaluator core**
   - due calculation, missed/overlap policy engine,
   - trigger payload builder,
   - run submission + persistence transitions.

5. **PR 5 — Restart reconciliation + reliability hardening**
   - in-flight recovery logic,
   - queued occurrence replay,
   - duplicate-risk regression tests.

6. **PR 6 — Final docs + acceptance suite**
   - update `FEATURES.md` status/checklist,
   - add operator-facing docs/examples,
   - integration tests across memory and SQLite modes.

## 14) Out of scope / defer beyond v0.3

- `start_at` / catch-up lower bounds for scheduler streams.
- DB-managed schedule installations.
- Runtime schedule overrides outside code definitions.
- Distributed scheduler coordination and leader election.
- Queueing/admission control beyond per-schedule overlap policies.
- Generic external trigger framework abstraction.
- Exactly-once submission guarantees across crash boundaries.
