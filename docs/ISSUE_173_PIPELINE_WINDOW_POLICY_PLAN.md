# Issue 173 Pipeline Window Policy Plan

> Status: implemented for issue #173.
> Scope: make pipeline window policy operational for full-load, manual one-window, and scheduled one-window submissions.
> Backfill orchestration remains out of scope for issue #173 and belongs with issue #168.

## Issue Summary

Issue #173 asks Favn to connect four concepts end to end:

- asset `@window` declares what runtime windows an asset can execute
- pipeline `window` declares the default operational window policy for that pipeline
- run input selects no window or one concrete window
- scheduler converts due occurrences into concrete one-window pipeline runs

The asset runtime contract should stay simple. Assets receive either `ctx.window == nil` for full-load style runs or a concrete `%Favn.Window.Runtime{}` for windowed runs. Assets should not care whether the run was manual, scheduled, or backfill-driven.

## Current Baseline

Implemented foundations already present:

- `Favn.Window.Spec`, `Favn.Window.Anchor`, `Favn.Window.Runtime`, validation, and key helpers live under `apps/favn_core/lib/favn/window/`.
- `Favn.Window.hourly/1`, `daily/1`, `monthly/1`, `anchor/4`, and `runtime/5` are exposed from `apps/favn_authoring/lib/favn/window.ex`.
- Asset DSLs capture asset-level `@window` specs.
- Pipeline DSL supports `window atom`, currently stored as `pipeline.window`.
- Manifest pipelines carry `window: atom() | nil`.
- `Favn.Assets.Planner.plan/2` already accepts `:anchor_window`, `:anchor_windows`, and `:anchor_ranges` and can expand windowed assets into `{asset_ref, window_key}` nodes.
- `Favn.Run.Context` already has `ctx.window` and `ctx.pipeline` fields.
- Orchestrator run projections already expose `pipeline.anchor_window` and `pipeline.window` when those are present in pipeline context.

Important gaps to close:

- `FavnOrchestrator.RunManager.build_pipeline_module_submission/2` passes `anchor_window` into `PipelineResolver`, but the final `build_pipeline_submission/2` call does not pass that anchor into the planner, so concrete window intent is not used for execution.
- `FavnOrchestrator.Scheduler.Runtime.submit_occurrence/8` builds an `anchor_window` for resolution, but it does not pass the anchor into `FavnOrchestrator.submit_pipeline_run/2`, so scheduled runs lose the concrete anchor before planning.
- Scheduler window anchoring currently floors `due_at` to the same period. For a monthly schedule firing on `2026-05-01`, that produces May, not the desired previous complete April window.
- Pipeline parallel execution currently collapses plan node keys back to asset refs in `RunServer.pipeline_stage_groups/1`, and runner work metadata uses `{asset_ref, nil}`. This loses concrete runtime window identity for windowed plans.
- `FavnRunner.ContextBuilder` reads `work.trigger.window`, but orchestrator runner work does not currently put the planned node runtime window there.
- The private orchestrator HTTP API accepts pipeline submit targets but has no run-input contract for anchor windows.
- `mix favn.run` and `Favn.Dev.Run` have no `--window` or `--timezone` parsing/submission path.
- The web prototype can submit pipeline runs but cannot submit or display operator-selected anchor windows beyond existing run metadata projections.

## Design Decisions

### Pipeline Window Policy V1

Keep the first public DSL small:

```elixir
window :monthly
```

Normalize accepted policy aliases into canonical anchor kinds:

- `:hourly` and `:hour` -> `:hour`
- `:daily` and `:day` -> `:day`
- `:monthly` and `:month` -> `:month`

Store the normalized value in manifests as an explicit policy structure rather than continuing to treat it as an opaque atom:

```elixir
%Favn.Window.Policy{
  kind: :month,
  anchor: :previous_complete_period,
  timezone: nil,
  allow_manual_full_load?: false
}
```

For the first implementation, `window :monthly` should compile to the same default policy as `window :monthly, anchor: :previous_complete_period`. Pipeline-level timezone should default to the schedule timezone for scheduled runs and to explicit CLI/API timezone or `Etc/UTC` for manual runs.

Defer custom calendars, arbitrary fiscal periods, and multiple policy modes. Keep the struct extensible enough to add them later without changing the run input contract.

### Run Input V1

Represent operator/API intent before it becomes an anchor:

```elixir
%Favn.Window.Request{
  mode: :full_load | :single,
  kind: :hour | :day | :month | nil,
  value: String.t() | nil,
  timezone: String.t() | nil
}
```

This request is local/API-facing. Orchestrator should resolve it to either `nil` or a concrete `%Favn.Window.Anchor{}` before building the plan.

Initial supported forms:

- no request against a pipeline with no window policy -> full-load run, `anchor_window: nil`
- `--window month:YYYY-MM` -> one monthly anchor
- `--window day:YYYY-MM-DD` -> one daily anchor, even if CLI support lands after monthly tests
- `--window hour:YYYY-MM-DDTHH` -> one hourly anchor, even if CLI support lands after monthly tests
- no request against a windowed pipeline -> clear error for manual runs

Do not invent an asset-level ingestion mode. Full-load behavior remains `ctx.window == nil`.

### Scheduler V1

Scheduled runs for windowed pipelines should resolve an anchor using the pipeline policy and the schedule timezone.

Default policy:

- hourly schedule -> previous complete hour
- daily schedule -> previous complete day
- monthly schedule -> previous complete month

Example: monthly due at `2026-05-01T03:00:00 Europe/Oslo` resolves to `2026-04-01T00:00:00 Europe/Oslo` through `2026-05-01T00:00:00 Europe/Oslo`.

The scheduler should continue to submit one run per selected occurrence. Catch-up mode `missed: :all` naturally submits one anchor per due occurrence, bounded by the existing cap.

### Runtime Execution V1

The orchestrator must execute planned node keys, not just asset refs.

For each plan node:

- runner work metadata should carry the actual `node_key`
- runner work trigger should include `window: node.window`
- `ctx.window` should be populated from the planned node runtime window
- `ctx.pipeline.anchor_window` should remain the requested anchor window
- non-windowed assets should still receive `ctx.window == nil`, even in a windowed pipeline

This preserves the current asset behavior model while making concrete runtime windows observable and testable.

## Boundary Design

### `favn`

Owns the public DSL and task surface.

- Extend `Favn.Pipeline.window/1` with alias normalization through shared core code.
- Add `Favn.Pipeline.window/2` only when adding options such as `anchor:` or `timezone:`.
- Extend `mix favn.run` with `--window` and `--timezone`.
- Keep diagnostics user-facing and specific.

### `favn_authoring`

Owns authoring macros behind `favn`.

- Compile pipeline `window` clauses into canonical policy data.
- Validate invalid policy atoms at compile time.
- Update `Favn.Pipeline` docs with full-load and windowed examples.

### `favn_core`

Owns the reusable policy and request semantics.

- Add `Favn.Window.Policy` for pipeline policy.
- Add `Favn.Window.Request` or parser/normalizer if a struct is too much for V1.
- Add `Favn.Window.Policy.resolve_manual/2` and `resolve_scheduled/3` helpers.
- Keep conversion to `%Favn.Window.Anchor{}` in core so CLI, orchestrator API, and scheduler use the same behavior.
- Preserve planner behavior around `:anchor_window`, `:anchor_windows`, and `:anchor_ranges` for issue #168.

### `favn_orchestrator`

Owns authoritative run submission, scheduling, and persisted metadata.

- Resolve run input to `anchor_window` before planning.
- Pass `anchor_window` into `Planner.plan/2` for pipeline submissions.
- Store requested window input and resolved anchor in run metadata/pipeline context.
- Pass planned node runtime windows into runner work.
- Use policy resolution in the scheduler instead of local ad hoc floor/shift helpers.
- Extend private API request parsing and response payloads for run window input.

### `favn_local`

Owns local CLI-to-HTTP plumbing.

- Parse `--window month:YYYY-MM` and `--timezone Europe/Oslo` from `mix favn.run` options.
- Submit the parsed request through the local HTTP boundary.
- Surface clear diagnostics for malformed input, unsupported kind, invalid date, invalid timezone, and missing window on windowed pipelines.

### `favn_runner`

Owns execution only.

- Do not resolve policy in the runner.
- Read only the planned runtime window carried in runner work.
- Keep assets unaware of manual vs scheduled vs backfill origin.

### `favn_web`

Owns operator UX through the web-dev workflow.

- Display pipeline window policy from active-manifest target data.
- Allow manual pipeline submission with one explicit month/day/hour where the pipeline has a window policy.
- Keep full-load submission available only for pipelines without a window policy in V1 unless an explicit policy option later allows it.
- Show requested anchor and concrete asset runtime windows in run details.

## Implementation Slices

### Slice 1: Core Policy And Request Primitives

- Add `Favn.Window.Policy` with canonical kind, default anchor policy, optional timezone, and validation.
- Add request parsing/resolution helpers for `month:YYYY-MM`, `day:YYYY-MM-DD`, and `hour:YYYY-MM-DDTHH`.
- Add previous-complete-period resolution for scheduler due times.
- Add focused unit tests for alias normalization, invalid aliases, manual monthly parsing, timezone handling, DST-safe boundary construction, and scheduled previous complete period.

### Slice 2: Pipeline DSL And Manifest Contract

- Change pipeline `window` normalization from arbitrary atom to canonical policy.
- Preserve public `window :monthly` ergonomics.
- Update `Favn.Pipeline.Definition`, `Favn.Manifest.Pipeline`, serialization, rehydration, and compatibility tests as needed.
- Update pipeline fixtures that currently use `:day`, `:daily`, `:calendar_day`, or other ad hoc values.
- Add compile-time tests for valid aliases and invalid policy atoms.

### Slice 3: Orchestrator Manual Pipeline Submission

- Extend pipeline submission opts with `:window_request` and `:anchor_window` handling.
- For module submissions, resolve the pipeline policy and manual request before calling `build_pipeline_submission/2`.
- For manifest target submissions, resolve the manifest pipeline target and apply the same policy path.
- Pass `anchor_window` into `Planner.plan/2`.
- Store both requested input and resolved anchor in metadata under `pipeline_context`.
- Return clear errors for missing manual window on a windowed pipeline and invalid explicit windows.

### Slice 4: Orchestrator Node-Key Execution

- Change pipeline stage grouping to preserve plan node keys instead of reducing to refs.
- Submit runner work per node key.
- Put `node.window` into `work.trigger.window` and `node_key` into metadata.
- Ensure retry/cancel/result paths continue to reference the correct asset ref while preserving node-key identity.
- Add integration coverage proving a manual monthly run produces concrete `ctx.window` for windowed assets and `nil` for non-windowed dependencies.

### Slice 5: Scheduler Policy Resolution

- Replace `Scheduler.Runtime.maybe_anchor_window/3` with shared policy resolution.
- Resolve previous complete period from `due_at` in the schedule timezone.
- Pass resolved `anchor_window` through to final run submission.
- Persist/run-project the scheduled requested anchor in pipeline context.
- Add scheduler tests for monthly previous complete period, timezone, missed occurrence behavior, and non-windowed full-load scheduled pipelines.

### Slice 6: Private HTTP API And Local CLI

- Extend the private run submission schema with:
  - `window: {mode: "single", kind: "month", value: "2026-03", timezone: "Europe/Oslo"}`
  - no `window` for full-load/manual non-windowed submissions
- Extend `mix favn.run` switches with `window: :string` and `timezone: :string`.
- Normalize local CLI input in `favn_local` or through a shared core parser before submitting HTTP JSON.
- Add Mix task tests for valid monthly input and invalid input diagnostics.
- Add local run tests that assert the HTTP request includes the expected window payload.

### Slice 7: Projection And Web UX

- Ensure run API payloads expose requested window, resolved anchor window, and per-node runtime windows in a web-friendly shape.
- Update active-manifest pipeline target data to expose window policy.
- In `favn_web`, add window controls to manual pipeline submission and run detail display.
- Add browser/BFF tests for submitting a monthly pipeline run with an explicit month and seeing the requested anchor in run details.

### Slice 8: Docs And Examples

- Update `README.md` quickstart/local development examples with a monthly pipeline and `mix favn.run ... --window month:YYYY-MM`.
- Update `docs/FEATURES.md` only after behavior lands.
- Remove or downgrade the issue #173 roadmap entry when complete.
- Update `examples/basic-workflow-tutorial` if it is the best consumer-facing demonstration point.

## Test Plan

Core tests:

- policy alias normalization and validation
- manual `month:YYYY-MM` to anchor conversion
- explicit timezone conversion and invalid timezone diagnostics
- scheduled previous complete hour/day/month resolution
- DST boundary behavior for `Europe/Oslo`

Authoring/manifest tests:

- `window :monthly` compiles to canonical policy
- invalid pipeline window atom fails at compile time
- manifest serialize/rehydrate preserves policy
- compatibility/hash behavior remains deterministic

Orchestrator tests:

- full-load pipeline with no window policy produces no anchor and unwindowed plan
- manual monthly pipeline with explicit request produces one anchor and windowed plan nodes
- manual windowed pipeline without request fails clearly
- scheduler monthly due on May 1 creates April anchor
- scheduled run submission passes anchor through planner and metadata
- run projection includes requested and resolved window data

Runner/orchestrator integration tests:

- windowed asset receives concrete `ctx.window`
- unwindowed dependency in a windowed run receives `ctx.window == nil`
- retries preserve node-key/window identity

CLI/local tests:

- `mix favn.run Pipeline --window month:2026-03 --timezone Europe/Oslo --wait` submits expected payload
- malformed `--window` fails before submission with a clear message
- unsupported kind fails with a clear message
- `--timezone` without `--window` fails unless a later defaulting policy needs it

Web tests:

- pipeline run form exposes a month selector for monthly pipelines
- run details show requested anchor window and concrete runtime windows

Verification commands after Elixir code changes:

```bash
mix format
mix compile --warnings-as-errors
mix test
mix credo --strict
mix dialyzer
mix xref graph --format stats --label compile-connected
```

For `favn_web` changes, use the web-dev workflow and run the workspace's relevant lint/build/e2e checks.

## Acceptance Mapping

- Full-load pipeline can run with no anchor window: slices 3 and 6.
- Monthly pipeline can be manually run for one explicit month: slices 1, 3, 4, and 6.
- `mix favn.run ... --window month:YYYY-MM` submits a concrete monthly anchor: slices 1 and 6.
- Scheduler-created monthly runs include previous-complete-month anchors: slice 5.
- Run metadata/UI shows requested anchor and concrete asset runtime windows: slices 3, 4, and 7.
- Missing/invalid window CLI inputs fail clearly: slices 1 and 6.
- Tests cover full load, manual one-month, scheduled previous-month, timezone, and invalid input: all slices.

## Non-Goals

- Full parent/child backfill ledger from issue #168.
- Complex custom calendars.
- Asset-level ingestion modes.
- Production deployment automation.
- Public external orchestrator API stabilization.

## Resolved V1 Decisions

- Windowed pipelines do not allow full-load submissions by default. A later explicit policy option can opt in; the internal policy field is `allow_full_load`.
- Pipeline window policy is represented as a domain struct inside Elixir and serializes as a canonical map across JSON/web boundaries.
- V1 supports `hour`, `day`, `month`, and `year` window requests and policy aliases from the start.
