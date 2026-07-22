# Runtime Model

Reader: Favn contributors and AI agents changing runtime behavior.

Documentation type: explanation.

This document explains who owns runtime behavior in Favn. It is contributor
guidance, not a public API promise.

## Problem

Favn needs runtime behavior that survives restarts and deploys. Runs, schedules,
backfills, auth state, and diagnostics cannot depend on what modules happen to be
loaded in memory.

The unsafe shortcut is to let UI code, Mix tasks, or runner code rebuild runtime
truth from raw storage rows, in-memory worker state, or current source modules.
That breaks the manifest-first model.

## Decision

The orchestrator is the runtime owner. It persists manifest versions, active
manifest selection, runs, schedules, backfills, command idempotency,
auth/session/audit state, diagnostics, and operator-facing data.

The runner executes pinned work. Pinned work is work that names the manifest
version and content hash it came from. The runner owns execution mechanics,
plugins, SQL runtime behavior, timeouts, cancellation attempts, crash reporting,
relation inspection, and execution diagnostics.

The view is thin. It renders data returned by the orchestrator and sends commands
to the orchestrator. It must not call storage adapters, scheduler internals,
runner internals, repositories, compiler internals, or execution plugins.

## Main Flow

```text
Favn authoring code
  -> manifest data
  -> persisted manifest version in the orchestrator
  -> active manifest or explicit manifest version selection
  -> operator command, API command, CLI command, or scheduler tick
  -> orchestrator run admission
  -> persisted run state
  -> pinned runner work
  -> runner result, timeout, cancellation outcome, or crash report
  -> orchestrator terminal run state
  -> operator UI, API, or CLI response
```

A run is one accepted attempt to execute an asset, pipeline, or scheduled/backfill
unit of work. The orchestrator records the run before dispatching execution.

## Orchestrator Ownership

The orchestrator owns these runtime facts:

| Fact | Rule |
| --- | --- |
| Manifest versions | Persisted by the orchestrator. Active selection is orchestrator state. |
| Runs | Created, updated, recovered, and terminalized by the orchestrator. |
| Runner ownership records | Persisted before dispatch so recovery and cancellation do not rely only on memory. |
| Schedules | Derived from the active manifest and persisted as scheduler state. |
| Backfills | Expanded, grouped, submitted, retried, and reported by the orchestrator. |
| Operator data | Bounded run, schedule, target, log, and diagnostic views come from orchestrator-owned functions. |
| Auth/session/audit | Durable operator identity and audit state remains orchestrator-owned. |

Repair jobs may rebuild derived operator data from authoritative history. They
must not invent run lifecycle facts or rewrite runner outcomes.

## Runner Boundary

The orchestrator talks to the runner through a runner client contract. The runner
can register manifests for execution, accept pinned work, return results, attempt
best-effort cancellation, expose bounded diagnostics, and inspect relations
safely.

Runner memory is not the source of truth for the control plane. If the runner
restarts, durable run state must still come from orchestrator storage.

The runner reports cancellation outcomes. The orchestrator decides how those
outcomes affect persisted run state.

## Run Lifecycle

1. A caller sends run intent through an operator, API, CLI, or scheduler boundary.
2. The orchestrator authenticates and authorizes the command when needed.
3. The orchestrator resolves the target in the selected manifest version.
4. The orchestrator persists the run before dispatching execution.
5. The orchestrator records runner ownership details before dispatch.
6. The runner executes pinned work.
7. The runner returns a result, timeout, cancellation outcome, or crash report.
8. The orchestrator persists the terminal state and events.
9. Operator surfaces read bounded summaries and details from the orchestrator.

Non-terminal run states are `:pending` and `:running`. Terminal run states are
`:ok`, `:partial`, `:error`, `:cancelled`, and `:timed_out`.

## Schedule Lifecycle

Schedules come from the active manifest. In the first supported production
topology, the single control-plane container owns one scheduler runtime that
evaluates them and submits due work through orchestrator run admission.

Schedule activation state means:

| State | Meaning |
| --- | --- |
| `:pending_activation` | The manifest contains the schedule, but scheduler state has not been activated yet. |
| `:enabled` | Future due occurrences may submit runs. |
| `:disabled` | Future submissions are stopped. Existing in-flight runs continue. |
| `:needs_review` | Persisted schedule state no longer matches the manifest schedule and needs operator review. |
| `:retired` | Persisted schedule state no longer matches an active manifest schedule. |

Runtime schedule state is separate: `:inactive`, `:idle`, `:running`, or
`:queued`. UI and CLI clients should display these values. They should not infer
schedule behavior from raw cursor fields.

## Backfill Lifecycle

A backfill is an operator-created group of runs for a range of work. The
orchestrator validates the range, expands it into concrete windows, persists the
group, submits child runs, and reports group progress.

Partial submission can happen. If some child runs are accepted and a later child
fails to submit, the orchestrator returns partial information instead of claiming
the whole command failed with no side effects.

## Cancellation Lifecycle

Cancellation is orchestrator-owned.

1. The operator requests cancellation through the orchestrator boundary.
2. The orchestrator checks the operator session and role.
3. The orchestrator records cancellation intent.
4. If runner work is active, the orchestrator asks the runner to cancel known
   execution ids.
5. The runner reports the outcome.
6. The orchestrator records the outcome and changes run state only when the
   outcome supports that change.

A run becomes `:cancelled` only when cancellation is acknowledged or when there
is no active runner work left. Unknown runner outcomes must stay visible instead
of being marked as success.

## Failure Rules

| Failure | Required behavior |
| --- | --- |
| Validation failure | Return a structured error before persistence when possible. |
| Persistence failure | Do not claim accepted state unless it was durably recorded. |
| Dispatch timeout | Keep the outcome explicit when success or failure cannot be proven. |
| Runner unavailable | Report a runner-boundary failure. Do not bypass the runner from orchestrator or view code. |
| Runner crash | Normalize the failure through orchestrator lifecycle handling. |
| Cancellation ambiguity | Persist the observed outcome instead of marking success blindly. |
| Derived operator data is stale | Rebuild from authoritative history where supported. |
| Duplicate command key | Use idempotency records. Do not repeat side effects. |

## Contributor Rules

- Add new runtime commands at the orchestrator boundary, then let UI, API, CLI,
  or scheduler callers use that boundary.
- Do not let `favn_view` call storage, scheduler, runner, repository, compiler,
  plugin, or adapter internals.
- Do not let the runner choose active manifests, own schedules, or own persisted
  run state.
- Keep runner work pinned with `manifest_version_id` and content hash.
- Keep operator reads bounded.
- Treat storage adapters as persistence mechanisms, not policy owners.
- Keep local development and production packaging separate from these ownership
  rules.

## Related Docs

- `apps/favn/guides/runtime-model.md` is the public package explanation.
- `docs/operators/runs-and-schedules.md` gives operator procedures.
- `docs/structure/favn_orchestrator.md` maps orchestrator-owned areas.
- `docs/structure/favn_runner.md` maps runner-owned areas.
- `docs/structure/favn_view.md` maps UI ownership rules.
- `docs/production/README.md` routes the supported production topology and
  operational contracts.
