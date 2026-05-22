# Issue 389 Run Execution Lifecycle Plan

Issue: #389

Status: phase 1 contract extraction in PR #405; full message-driven execution
state machine still pending.

## Goal

Extract explicit orchestrator-owned runtime contracts for step attempts and
in-flight runner work without changing observable run semantics in the first
pass. The full #389 refactor should make submit, await, timeout, retry,
cancellation, persistence conflict handling, admission cleanup, and
materialization claim cleanup consistent between sequential and pipeline
execution.

This follows `docs/refactor_review_standard.md`: expose real lifecycle
contracts that already exist implicitly, do not split files for cosmetic
reasons, and keep orchestrator control-plane behavior in `favn_orchestrator`.

PR #405 is intentionally phase 1 only. It adds `StepAttemptLifecycle` and
`RunWorkSet` seams and improves submit-failure cleanup, but it does not close
#389 because `RunServer` still executes through a blocking callback stack. The
remaining work is to make `RunServer` own execution as a message-driven state
machine with explicit timers, monitors, await result messages, retry messages,
admission retry messages, and cancellation handling.

## Current Baseline

PR 388 already introduced useful runtime boundaries:

- `FavnOrchestrator.RunServer.Execution.StageAdmission` owns pipeline stage
  admission, materialization claim acquisition, queued-step dedupe,
  `:step_queued`, and `:step_started` persistence.
- `FavnOrchestrator.RunServer.Execution.StageAttemptState` owns pipeline
  stage-local scheduler bookkeeping for admitted, deferred, retryable,
  completed, and draining work.
- `FavnOrchestrator.RunServer.Execution.AwaitTasks` owns await worker spawn,
  monitor, timeout, stale reply, and stop mechanics.
- `FavnOrchestrator.RunServer.Cancellation` owns the runner cancellation
  envelope and best-effort runner cancellation dispatch.

The remaining lifecycle is still partly implicit:

- Sequential execution builds `RunnerWork`, submits, persists `:step_started`,
  awaits, maps terminal outcomes, schedules retries, and sleeps inline in
  `Execution.execute_ref_with_retry/5`, `await_sequential_result/7`, and
  `maybe_retry_step/7`.
- Pipeline execution separately submits stage entries, awaits results, maps
  terminal outcomes, records node results, schedules retries, drains siblings,
  refills stage capacity, and sleeps or timers across `Execution` and
  `StageAdmission`.
- Active work is represented by runner execution ids, `:in_flight_execution_ids`
  metadata, `runner_execution_id`, await tasks, admission leases,
  materialization claims, and persisted events.
- Persistence conflicts are raised by `RunServer.Persistence.persist_run_step/3`
  except for external cancellation, which means lifecycle callers cannot handle
  persistence outcomes as normal runtime decisions.
- Retry backoff in sequential and pipeline paths uses blocking
  `Process.sleep/1`, which delays external cancellation responsiveness.
- Cleanup is duplicated across await timeout, await error, external cancel,
  pending await shutdown, admission failure, and terminal run finalization.

## Architectural Decision

Introduce two small, boring orchestrator-owned contracts and make the run server
an explicit OTP state machine for execution events:

- `FavnOrchestrator.RunServer.Execution.StepAttemptLifecycle`
- `FavnOrchestrator.RunServer.Execution.RunWorkSet`

Keep both modules under `apps/favn_orchestrator`. Do not move lifecycle logic to
`favn_core`; it is orchestrator control-plane behavior, not a shared manifest or
runner contract.

`StepAttemptLifecycle` should own one attempt's identity, context, submitted
work, event persistence outcomes, runner outcome classification, retry decision,
and post-step cleanup decision. `RunWorkSet` should own active in-flight work for
one run or stage: runner execution ids, admission leases, materialization claims,
await task entries, cancellation dispatch input, and idempotent cleanup.

`RunServer` should own process mechanics: timers, monitors, await result
messages, cancellation messages, retry messages, admission retry messages, and
terminalization. `StepAttemptLifecycle` decides what an execution event means;
`RunServer` applies the transition and schedules the next event. This preserves
the BEAM/OTP split between pure-ish decision logic and process ownership.

The first implementation should be behavior-preserving for persisted event order,
terminal statuses, result ordering, and cleanup semantics. It should deliberately
improve cancellation responsiveness by removing blocking sleeps and moving retry
waits, sequential awaits, and pipeline admission waits toward message-driven
GenServer state.

Suggested run-server execution state:

```elixir
%RunExecutionState{
  run: RunState.t(),
  version: Version.t(),
  mode: :sequential | :pipeline,
  status: :starting | :submitting | :awaiting | :retry_wait | :draining | :terminalizing,
  work_set: RunWorkSet.t(),
  active_attempts: %{optional(term()) => StepAttemptLifecycle.t()},
  await_tasks: AwaitTasks.t(),
  timers: %{optional(term()) => reference()},
  accumulated_results: [term()],
  stage_state: StageAttemptState.t() | nil,
  freshness_context: map() | nil,
  terminal_failure: map() | nil
}
```

Run-server messages should be explicit and scoped by attempt/stage identifiers:

```elixir
{:runner_result, attempt_id, result}
{:runner_await_down, attempt_id, reason}
{:attempt_timeout, attempt_id}
{:retry_attempt, attempt_id, next_attempt}
{:stage_admission_retry, stage, attempt}
{:cancel_run, reason}
```

Cancellation should be delivered to the active run server when possible, with the
persisted run snapshot remaining the durable source of truth for races, restarts,
and external observers.

## Contract 1: StepAttemptLifecycle

`StepAttemptLifecycle` is the semantic state machine for one node/window/stage
attempt. It should not spawn processes, poll storage directly outside explicit
persistence calls, or own stage scheduling loops.

Suggested struct fields:

```elixir
%StepAttemptLifecycle{
  run: RunState.t(),
  version: Version.t(),
  node_key: Favn.Plan.node_key(),
  asset_ref: Favn.Ref.t(),
  asset_step_id: String.t(),
  window: term(),
  stage: non_neg_integer(),
  attempt: pos_integer(),
  max_attempts: pos_integer(),
  execution_pool: atom() | String.t() | nil,
  work: RunnerWork.t() | nil,
  entry: map() | nil
}
```

Public functions should stay minimal and typed:

```elixir
new(run, version, node_key, stage, attempt, opts \\ []) :: t()
build_work(t()) :: {:ok, t()} | {:error, outcome()}
mark_submitted(t(), execution_id) :: {:ok, t()} | {:error, outcome()}
record_runner_result(t(), RunnerResult.t()) :: {:ok, outcome()} | {:error, outcome()}
record_await_timeout(t()) :: {:ok, outcome()} | {:error, outcome()}
record_await_error(t(), reason) :: {:ok, outcome()} | {:error, outcome()}
record_submit_error(t(), reason) :: {:ok, outcome()} | {:error, outcome()}
schedule_retry(t(), reason_or_result) :: {:ok, retry()} | {:terminal, outcome()}
```

Return outcomes should be explicit structs or tagged tuples, not metadata bags:

```elixir
{:step_ok, run, asset_results}
{:step_retry, run, retry_after_ms, reason, asset_results}
{:step_error, run, reason, asset_results}
{:step_cancelled, run, reason, asset_results}
{:step_timed_out, run, reason, asset_results}
{:persistence_cancelled, run}
{:persistence_failed, run, reason}
```

Contract rules:

- Build `RunnerWork` in one place for sequential and pipeline execution.
- Use `RunnerWork.lifecycle_metadata/1` as the only source for per-attempt
  lifecycle metadata, then add only orchestrator-owned state explicitly.
- Centralize mapping from runner statuses to run statuses, terminal event types,
  retryability, and node result status.
- Centralize `:step_started`, `:step_finished`, `:step_failed`,
  `:step_timed_out`, `:step_cancelled`, and `:step_retry_scheduled` payload
  construction.
- Return persistence conflicts as runtime outcomes. Do not raise from lifecycle
  code when callers must decide cancellation, retry, or cleanup.
- Keep freshness writes and materialization claim completion/failure in the
  lifecycle only where they are part of terminal step outcome handling.
- Preserve current result ordering and terminal status behavior.

## Contract 2: RunWorkSet

`RunWorkSet` is the explicit representation of active runner work owned by the
orchestrator. It replaces scattered reads and writes of runner ids, admission
leases, materialization claims, and await entries.

Suggested struct fields:

```elixir
%RunWorkSet{
  run_id: String.t(),
  entries: %{optional(String.t()) => entry()},
  runner_execution_ids: MapSet.t(String.t()),
  leases: %{optional(String.t()) => map()},
  materialization_claims: %{optional(String.t()) => map()}
}
```

Public functions should cover the real lifecycle operations:

```elixir
new(run) :: t()
from_entries(run, entries) :: t()
add_entry(t(), entry) :: t()
complete_entry(t(), execution_id) :: {entry() | nil, t()}
execution_ids(t()) :: [String.t()]
sync_run_metadata(run, t()) :: RunState.t()
cancel_all(run, t(), reason, runner_client, runner_opts) :: {RunState.t(), t()}
release_entry(entry) :: :ok
fail_entry_claim(entry, reason) :: :ok
cleanup_all(t(), reason) :: :ok
```

Contract rules:

- Cleanup is idempotent. Releasing a missing lease or failing a missing claim is
  successful.
- Cancellation dispatch remains best-effort and uses `Cancellation.cancel_runner_work/5`.
- `runner_execution_id` and `:in_flight_execution_ids` metadata are derived from
  `RunWorkSet`, not hand-maintained in multiple functions.
- Await task cleanup callbacks call `RunWorkSet.release_entry/1` and
  `RunWorkSet.fail_entry_claim/2` rather than local helper functions.
- Admission leases are still acquired by `StageAdmission`, but returned entries
  must be shaped so `RunWorkSet` can own later release and failure semantics.

## Persistence Outcome Contract

Change `FavnOrchestrator.RunServer.Persistence.persist_run_step/3` from a
raise-on-error helper into an explicit lifecycle boundary.

Recommended return shape:

```elixir
:ok
| {:error, :external_cancel}
| {:error, {:persistence_conflict, :stale_write | :conflicting_snapshot}}
| {:error, {:persistence_failed, term()}}
```

Then update lifecycle callers to map these outcomes deliberately:

- External cancel becomes cancelled terminal state and triggers active work
  cleanup.
- Conflict that is not external cancel becomes `:persistence_conflict` terminal
  error unless the owning caller has a stronger recovery rule.
- Storage errors become `:post_step_persistence_failed` or
  `:run_event_persistence_failed` terminal errors, depending on where they occur.
- No lifecycle helper should raise for expected runtime races.

This can be introduced behind a new helper first, for example
`Persistence.persist_lifecycle_event/3`, then old callers can be migrated and
`persist_run_step/3` removed or reduced.

## Event-Driven Wait Contract

Replace direct `Process.sleep/1` and local blocking `receive` waits in execution
paths with run-server timers and `handle_info/2` messages.

Recommended retry shape:

```elixir
StepAttemptLifecycle.schedule_retry(t(), reason_or_result) ::
  {:ok, %{attempt_id: term(), next_attempt: pos_integer(), retry_after_ms: non_neg_integer()}}
  | {:terminal, outcome()}
```

`RunServer` should persist `:step_retry_scheduled`, store retry state, schedule a
timer with `Process.send_after/3`, return from the callback, and resume the
attempt in `handle_info({:retry_attempt, ...}, state)`.

Contract rules:

- `retry_backoff_ms == 0` schedules immediate continuation through the same
  message path or submits synchronously only if it does not block cancellation
  handling.
- External cancellation during retry wait wins before the next attempt is
  submitted.
- The retry scheduled event remains persisted before waiting, preserving current
  event ordering.
- Retry timers are tracked in run-server state and cancelled or ignored after
  terminalization.
- Pipeline stage admission retries should use the same timer/message pattern
  instead of local blocking `receive` waits.
- Await timeouts should be represented as explicit timer messages or delegated
  to `AwaitTasks` only when `AwaitTasks` reports back by message without blocking
  the run server.

This does not require a new supervision tree in the first pass. It does require
`RunServer` to keep execution state and continue work across callbacks rather
than executing the whole plan inside one blocking `handle_continue/2` stack.

## Sequential Execution Shape

Sequential execution should become the single-attempt variant of the same
message-driven execution state machine used by pipeline execution:

- Build `StepAttemptLifecycle` from the run, version, asset ref/node key, stage,
  and attempt.
- Build and submit `RunnerWork` through the configured runner client.
- On submit success, mark the attempt submitted and persist `:step_started`.
- Start a monitored await worker or runner async await path and return from the
  GenServer callback instead of awaiting directly in the run server process.
- Handle await success, await worker failure, await timeout, retry timer, and
  cancellation as `RunServer` messages.
- Use lifecycle retry outcomes to schedule a retry message, continue to the next
  sequential node, or terminalize failure.
- Use `RunWorkSet` for the single active execution id so cancellation and cleanup
  match pipeline semantics.

Expected result: `execute_ref_with_retry/5`, `await_sequential_result/7`, and
`maybe_retry_step/7` shrink to orchestration flow instead of owning lifecycle
semantics, and sequential execution no longer blocks the run server while waiting
for runner results or retry backoff.

## Pipeline Execution Shape

Pipeline stage execution should retain the current stage scheduler and sibling
drain semantics, but express scheduler progression through run-server messages
instead of blocking loops:

- `StageAdmission` continues to own admission and initial submission, but entries
  should be lifecycle-compatible and include enough attempt identity for
  `StepAttemptLifecycle` to reconstruct the attempt.
- `StageAttemptState` continues to own stage-local pending/deferred/retry queues.
- `AwaitTasks` continues to own worker mechanics only if its workers report to
  `RunServer` by message and do not require the run server to block in local
  receive loops.
- `process_one_stage_attempt_result/8` should delegate runner result, timeout,
  and await-error classification to `StepAttemptLifecycle`.
- `RunWorkSet` should derive and update in-flight metadata when entries are
  added, completed, cancelled, or stopped.
- Deferred admission retry should schedule `{:stage_admission_retry, stage,
  attempt}` timers and return from the callback while no work is runnable.
- Stage draining after the first terminal failure remains in `Execution`, because
  it is a stage scheduling policy, not a single-step lifecycle decision.

Expected result: pipeline and sequential paths share attempt outcome semantics,
while pipeline keeps its concurrent scheduling policy explicit and the run server
stays responsive to cancellation, timeouts, and terminalization.

## Implementation Tasks

- [ ] Add `RunWorkSet` with idempotent entry release, claim failure, cancellation,
  and run metadata sync helpers.
- [ ] Add focused unit tests for `RunWorkSet` cleanup idempotency and metadata
  derivation.
- [ ] Add `StepAttemptLifecycle` with work construction, event payload building,
  runner outcome classification, retry decision, and persistence outcome mapping.
- [ ] Add focused unit tests for lifecycle classification: success, retryable
  error, non-retryable error, retry exhaustion, timeout, runner cancellation,
  submit error, and persistence outcomes.
- [ ] Introduce explicit persistence outcome returns in `RunServer.Persistence`
  and migrate lifecycle callers away from raise-based control flow.
- [ ] Introduce `RunExecutionState` in `RunServer` so execution can continue
  across GenServer callbacks instead of one blocking `handle_continue/2` call.
- [ ] Change active run cancellation forwarding so `RunManager` sends a
  cancellation message to the run server when the process is alive, while still
  persisting cancellation as durable truth.
- [ ] Migrate sequential execution to use `StepAttemptLifecycle` and
  `RunWorkSet` without changing terminal run results or event order.
- [ ] Replace sequential direct await and retry `Process.sleep/1` with monitored
  await messages, timeout messages, and retry timer messages handled by
  `RunServer`.
- [ ] Migrate pipeline result handling to use `StepAttemptLifecycle` for per-step
  outcomes while preserving `StageAttemptState`, sibling drain, deferred
  admission, and result ordering semantics.
- [ ] Replace pipeline retry `Process.sleep/1` and deferred admission local
  receives with run-server timer messages.
- [ ] Update `StageAdmission` entry shape only as needed so lifecycle and
  `RunWorkSet` have explicit identity, lease, claim, and execution id data.
- [ ] Remove duplicated helpers from `Execution` after callers use the new
  contracts: status mapping, retryability checks, step event payload builders,
  in-flight metadata mutation, release helpers, and post-step persistence helpers.
- [ ] Update docs if public operator-visible run semantics or event payloads
  intentionally change. The first pass should avoid such changes.

## Testing Plan

Add or preserve tests at the owning layer, favoring focused run-server tests over
end-to-end flows:

- Sequential success preserves current `:run_started`, `:step_started`,
  `:step_finished`, `:run_finished` event order.
- Sequential retryable runner error schedules retry, waits, retries, and succeeds.
- Sequential retry exhaustion terminalizes as error with no leaked runner id.
- Sequential non-retryable runner error does not schedule retry.
- Sequential await timeout cancels runner work, records timeout, and either
  retries or terminalizes according to `max_attempts`.
- Sequential external cancellation after submit but before or during await wins
  and cleans active work.
- Sequential external cancellation during retry wait wins before the next submit.
- Sequential run server remains able to process cancellation while awaiting a
  runner result or retry timer.
- Pipeline retryable runner error matches sequential retryability and retry
  exhaustion semantics.
- Pipeline non-retryable error drains submitted siblings, blocks downstream, and
  does not schedule retry.
- Pipeline await timeout cancels only the timed-out work and keeps existing
  sibling drain semantics.
- Pipeline external cancellation cancels all active in-flight runner ids and
  fails/release claims and leases idempotently.
- Pipeline deferred admission timeout terminalizes the stage without leaking
  leases or materialization claims.
- Pipeline run server remains able to process cancellation while no stage work is
  runnable and admission retry timers are pending.
- Persistence conflict and persistence failure outcomes terminalize explicitly
  instead of crashing the run server.
- Storage adapter contract tests continue to cover execution leases and
  materialization claims; add adapter-level coverage only if the plan changes
  storage callback behavior.

## Verification

Run the narrowest useful checks first while implementing:

```bash
MIX_ENV=test mix do --app favn_orchestrator cmd mix test --no-compile --exclude acceptance --exclude slow --exclude browser test/run_server_test.exs
MIX_ENV=test mix do --app favn_orchestrator cmd mix test --no-compile --exclude acceptance --exclude slow --exclude browser test/integration/storage_adapter_contract_test.exs
```

Before finishing implementation, run the repository standard checks:

```bash
mix format
mix compile --warnings-as-errors
mix test
```

## Non-Goals

- Do not change runner client behaviour contracts unless a lifecycle bug proves
  the current runner boundary is insufficient.
- Do not move lifecycle code to `favn_core`.
- Do not add a new supervision tree or long-lived retry process in the first
  pass; use the existing per-run `RunServer` process as the owner of timers,
  monitors, and execution state.
- Do not redesign persistence schemas unless explicit lifecycle outcomes expose
  a missing persisted fact.
- Do not change UI/API response shapes as part of this refactor.

## Risks And Mitigations

- Runtime behavior is critical and concurrency-sensitive. Mitigate by migrating
  one path at a time and keeping event order assertions.
- Shared lifecycle code can become too broad. Mitigate by keeping stage
  scheduling policy in `Execution`/`StageAttemptState` and only sharing per-step
  semantics.
- Cleanup changes can hide leaks. Mitigate with tests that inspect execution
  leases, materialization claims, and run metadata after terminal outcomes.
- Persistence error behavior may become more visible. Mitigate by documenting
  explicit terminal errors and preserving external-cancel wins races.

## Acceptance Criteria

- Sequential and pipeline execution use the same per-step lifecycle contract for
  runner outcomes, retryability, timeout, cancellation, and post-step cleanup.
- `RunServer` owns execution as an event/message-driven OTP state machine rather
  than executing the whole plan inside one blocking callback stack.
- Active in-flight runner work is represented through `RunWorkSet`, not scattered
  metadata manipulation.
- Persistence helper failures are explicit lifecycle outcomes, not hidden raises
  in expected runtime races.
- Retry waits, sequential awaits, and pipeline admission waits are
  cancellation-aware and no longer block the run server process.
- Existing run-server behavior remains compatible unless a deliberate bug fix is
  covered by focused tests.
