# Operate Runs And Schedules

Reader: operators using Favn runtime tooling.

Documentation type: how-to guide.

Use this guide when you need to register a manifest, activate it, submit a run,
inspect run state, cancel work, retry work, operate schedules, or diagnose
runtime state.

A manifest is the saved description of authored Favn work. It contains assets,
pipelines, schedules, dependencies, and runtime metadata.

A run is one accepted attempt to execute an asset, pipeline, scheduled occurrence,
or backfill child. The orchestrator records the run before the runner starts
execution.

For production startup, backup, and restore, use
`docs/production/single_node_operator_runbook.md`.

## Assumptions

- The backend runtime is running.
- You are using supported CLI, API, or UI tooling.
- You have an authenticated operator session for mutating actions.
- A manifest has been built from the public authoring flow.
- You are not editing control-plane storage directly.

## Register And Activate A Manifest

1. Build or obtain the manifest JSON from the supported authoring flow.
2. Register the manifest through supported tooling or the supported backend API
   used by that tooling.
3. Confirm registration returned a manifest version id and content hash.
4. Activate that manifest version if new runs and schedules should use it by
   default.
5. Read the active manifest and confirm it matches the version you intended.
6. If the runner must register the manifest for execution, use the supported
   runner-registration action. Do not edit runner state by hand.

Expected result: the orchestrator has a persisted manifest version and an active
manifest selection. The runner may also know the manifest for execution, but the
orchestrator remains the source of truth.

Common failures:

| Failure | Action |
| --- | --- |
| Manifest validation error | Rebuild the manifest from authoring code and inspect diagnostics. |
| Manifest version conflict | Check the version id and content hash. Do not reuse one version id for different content. |
| Persistence failure | Check orchestrator readiness, storage readiness, and diagnostics before retrying. |
| Runner registration failure | Fix runner availability, then retry the supported registration action. Do not edit runner memory or files directly. |

## Submit A Run

1. Read the active manifest or choose an explicit manifest version.
2. Choose one valid target: an asset, pipeline, scheduled occurrence, or backfill
   child target exposed by operator tooling.
3. Submit the run through the UI, API, CLI, or `mix favn.run` when available for
   your environment.
4. Record the returned run id.
5. Inspect the run through orchestrator-backed tooling such as run history, run
   detail, logs, status, or diagnostics.

Expected result: the orchestrator records the run first, dispatches pinned work,
and later records the final run state.

### Choose Asset Dependency Scope And Refresh

Direct asset submissions have two independent controls:

| Control | Values | Meaning |
| --- | --- | --- |
| Dependency scope | `all`, `none` | Plan the selected asset with all transitive upstream assets, or plan only the selected asset. |
| Refresh | `auto`, `missing`, `force_selected`, `force_selected_upstream`, `force_all` | Apply freshness or force behavior inside the planned graph. |

The safe defaults are dependency scope `all` and refresh `auto`. For a targeted
repair after upstream inputs have been independently verified, run:

```bash
mix favn.run MyApp.Source.Events:movement \
  --window month:2026-07 \
  --dependencies none \
  --refresh force_selected
```

`force_selected_upstream` requires dependency scope `all`. Pipeline targets do
not accept dependency scope and accept only `auto`, `missing`, and `force_all`
refresh. The UI, private HTTP API, and CLI validate the same operator contract;
invalid values and combinations return validation errors before execution.

Do not bypass dependencies merely to make a run cheaper. Scope `none` means the
operator accepts responsibility for the suitability of the target's current
upstream inputs.

Common failures:

| Failure | Action |
| --- | --- |
| Unknown target | Refresh the manifest target list and submit a valid target id. |
| No active manifest | Register and activate a manifest, or submit against an explicit manifest version. |
| Forbidden | Use an operator or admin actor allowed to submit runs. |
| Runner unavailable | Check diagnostics and runner readiness. Do not bypass the orchestrator. |
| Missing runtime config | Add the environment variable or secret named by diagnostics, then retry. |
| Duplicate command key | Re-read the existing command result instead of submitting the same side effect again. |
| Invalid dependency/refresh combination | Use `dependencies=all` with `force_selected_upstream`, or choose a target-only refresh mode. |

## Inspect A Run

1. Start with the run id returned by submission.
2. Read the run summary to see current state.
3. Open the run detail to inspect events, attempts, windows, logs, and errors
   exposed by operator tooling.
4. If the run is still active, check runner availability and in-flight diagnostics
   before deciding whether to cancel.
5. If the run is terminal, use the terminal state and events to decide whether to
   retry, rerun, or leave it unchanged.

Use orchestrator-backed views. Do not infer final state from UI loading state,
raw storage rows, or runner memory.

## Cancel A Run

1. Find the run id from run history or the run detail page.
2. Confirm the run is not already terminal.
3. Request cancellation through the orchestrator boundary.
4. Re-read the run detail and events.
5. If the outcome is unknown or already completed, inspect the final run state
   before submitting replacement work.

Expected result: the orchestrator records cancellation intent and asks the runner
to stop active work when possible. The run becomes cancelled only when
cancellation is acknowledged or no active runner work remains.

Do not mark a run cancelled by editing storage. Do not call runner cancellation
internals directly.

## Retry Or Rerun Work

1. Inspect effective policy/source, current/max attempt, typed failure outcome,
   retry exhaustion, `next_retry_at`, input mode, and safe pin lineage.
2. An automatic node attempt stays in the same run and reuses the run/node pin.
   It occurs only for an explicitly retryable known-safe failure.
3. Use retry-remaining/resume when you want a new run containing failed or
   not-started work with `:inherit` input behavior.
4. Use exact replay when you need a new manifest-pinned run with `:pinned`
   source inputs. Missing required pins fail; they are never silently replaced.
5. Use a fresh rerun only when selecting current external input is intentional.
   It is not exact replay.
6. For backfills, retry failed windows first.
7. Rerun successful windows only when you have an explicit force or refresh
   policy that makes that safe.

Expected result: the orchestrator creates new persisted run records linked to the
source run or backfill group.

`max_attempts` includes the first attempt and defaults to one. Operator
`retry_policy` overrides asset `@retry`, which overrides pipeline `retry`.
Policy changes timing/count only; an unknown write, transaction,
materialization, or external side effect remains terminal. `mix favn.run`
accepts `--retry-max-attempts` and fixed `--retry-backoff-ms`; the HTTP/operator
contract accepts the complete typed `retry_policy`, including exponential
backoff. The rerun API accepts `input_mode: fresh|inherit|pinned`.

Read `apps/favn/guides/retries-and-replay.md` for the canonical mechanism table,
schedule timeline, restart recovery, and safe ingestion recipes.

## Operate Schedules

1. List schedules from the active manifest through operator tooling.
2. Check activation state, runtime state, next due time, last submitted due time,
   and scheduler errors.
3. Preview upcoming occurrences before enabling a schedule.
4. Enable schedules that should submit future work.
5. Disable schedules that should stop future submissions.

Schedule overlap is not execution retry. `:allow` admits an independent run
with independent pins, `:forbid` admits none while the tracked run is active,
and `:queue_one` remembers one occurrence until it can be admitted. A run
waiting in node backoff is still active for these rules. `missed: :skip | :one |
:all` controls catch-up occurrences after delayed evaluation, not attempts in
the existing run.
6. After changing a schedule, re-read the schedule entry and diagnostics.

Expected result: enabled schedules submit due work through the same orchestrator
run path as manual runs. Disabled schedules do not submit future work.

Important rules:

- Enabling starts from the next due occurrence observed at command time. It does
  not automatically submit missed catch-up work.
- Disabling does not cancel existing in-flight runs.
- Schedule state is persisted by the orchestrator storage boundary.
- In the supported single-node topology, exactly one scheduler runtime should be
  active.
- `favn_view` may show schedule state, but it must not calculate schedule
  semantics itself.

## Diagnose Runtime State

Use readiness and diagnostics before retrying uncertain operations.

Check these items:

- Active manifest id and content hash.
- Manifest summaries and target lists.
- Storage readiness and schema readiness.
- Scheduler status and scheduler write errors.
- Runner availability and runner diagnostics.
- In-flight runs and recent failed runs.
- Missing runtime config or secrets.
- Stale or degraded operator views and repair recommendations.

If readiness fails, fix the failing check before submitting more work. If an
operation has an unknown outcome, re-read run state and diagnostics before retrying
so you do not create duplicate work.

Production command examples are in
`docs/production/single_node_operator_runbook.md`.

## Boundaries To Preserve

- Do not edit SQLite or other control-plane storage rows by hand.
- Do not ask the runner to own schedules, active manifest selection, or persisted
  run state.
- Do not infer lifecycle truth from UI state.
- Do not let `favn_view` call storage, scheduler internals, runner internals,
  repositories, compiler internals, plugins, or adapters directly.
- Do not treat DuckDB data-plane backup as part of SQLite control-plane backup.
- Do not run multiple active schedulers against one single-node SQLite
  control-plane database.
