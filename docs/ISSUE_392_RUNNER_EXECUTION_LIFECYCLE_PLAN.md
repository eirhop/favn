# Issue 392 Runner Execution Lifecycle Plan

Issue: #392

Status: implemented in this branch.

## Goal

Extract explicit runner-owned lifecycle state from `FavnRunner.Server` without
changing the public runner facade or the shared `Favn.Contracts.RunnerClient`
contract. The refactor should make running executions, completed executions,
worker monitors, await waiters, log subscribers, buffered logs, buffered events,
retention, and cleanup visible as one internal runtime contract.

This follows `docs/refactor_review_standard.md`: expose a real lifecycle
contract that already exists implicitly, separate GenServer protocol mechanics
from runtime state transitions, improve cleanup and bounded memory behavior, and
avoid splitting modules only by size.

## Current Baseline

`FavnRunner.Server` currently owns several different contracts at once:

- Public protocol calls for manifest registration, work submission, result
  awaits, cancellation, log subscription, relation inspection, and diagnostics.
- Manifest target resolution, manifest lookup, SQL runtime preflight, execution
  id creation, worker child specs, and worker monitoring.
- Lifecycle state in anonymous maps: `executions`, `monitor_to_execution`,
  `waiters`, and `log_subscribers`.
- Running execution state with work, worker pid, monitor ref, buffered events,
  and buffered logs.
- Completed execution state with result, buffered events, and buffered logs.
- Waiter timers and waiter replies.
- Log subscription replay and live fanout.
- Cancellation result construction and worker termination.
- Worker crash result construction.
- Diagnostics counts derived directly from anonymous execution maps.

The current behavior is simple but hides lifecycle ownership:

- Completed executions are retained indefinitely in memory.
- Per-execution log and event buffers are retained indefinitely while the
  execution is retained.
- Log subscribers are not monitored, so dead subscriber pids remain in
  `log_subscribers` until the execution completes or is explicitly
  unsubscribed.
- Waiter callers are not monitored, so abandoned waits are cleaned only by
  timeout or execution finalization.
- Finalization, cancellation, worker crash handling, waiter cleanup, subscriber
  cleanup, and monitor cleanup are split across GenServer callbacks.
- Diagnostics report only running and completed execution counts, not retention,
  waiter, subscriber, or buffer pressure.

## Architectural Decision

Introduce an internal runner lifecycle component and keep `FavnRunner.Server` as
the single GenServer protocol/process owner in the first implementation.

Recommended modules:

- `FavnRunner.ExecutionLifecycle`
- `FavnRunner.ExecutionLifecycle.Execution`

Do not introduce a new supervised lifecycle process in the first pass. The
current `FavnRunner.Server` already serializes lifecycle messages correctly, and
adding another process would create a second ownership boundary before there is
a concurrency need. The extraction should be a state contract plus pure-ish
transition functions that the server applies from `handle_call/3` and
`handle_info/2`.

Keep these boundaries stable:

- `FavnRunner` remains the public runner facade.
- `Favn.Contracts.RunnerClient` remains unchanged.
- `Favn.Contracts.RunnerWork`, `RunnerResult`, `RunnerError`,
  `RunnerCancellation`, and `RunnerEvent` remain the shared boundary shapes.
- `FavnRunner.Worker` continues to execute one asset and send
  `{:runner_result, execution_id, result}`, `{:runner_log_entry, execution_id,
  entry}`, and `{:runner_event, execution_id, event}` messages to the server.
- Manifest lookup, SQL preflight, relation inspection, and data-plane
  diagnostics stay outside the lifecycle component because they are not
  execution retention or process cleanup state.

## Lifecycle Contract

`FavnRunner.ExecutionLifecycle` should own the state currently spread through
server maps:

```elixir
%FavnRunner.ExecutionLifecycle{
  executions: %{optional(String.t()) => Execution.t()},
  monitor_to_execution: %{optional(reference()) => String.t()},
  waiters: %{optional(String.t()) => [waiter()]},
  waiter_monitor_to_execution: %{optional(reference()) => String.t()},
  log_subscribers: %{optional(String.t()) => MapSet.t(pid())},
  subscriber_to_monitor: %{optional(pid()) => reference()},
  subscriber_monitor_to_pid: %{optional(reference()) => pid()},
  subscriber_executions: %{optional(pid()) => MapSet.t(String.t())},
  completed_order: :queue.queue({DateTime.t(), String.t()}),
  retention: retention_policy(),
  counters: counters()
}
```

`FavnRunner.ExecutionLifecycle.Execution` should make one execution's state
explicit:

```elixir
%FavnRunner.ExecutionLifecycle.Execution{
  id: String.t(),
  work: Favn.Contracts.RunnerWork.t(),
  status: :running | :completed,
  pid: pid() | nil,
  monitor_ref: reference() | nil,
  result: Favn.Contracts.RunnerResult.t() | nil,
  events: [term()],
  logs: [term()],
  started_at: DateTime.t(),
  completed_at: DateTime.t() | nil,
  dropped_event_count: non_neg_integer(),
  dropped_log_count: non_neg_integer()
}
```

The lifecycle module should expose only operations that map to real runtime
events:

```elixir
new(opts) :: t()
put_running(t(), execution_id, work, pid, monitor_ref) :: t()
put_completed(t(), execution_id, work, result) :: t()
fetch_result(t(), execution_id) :: {:ok, RunnerResult.t()} | {:error, :execution_not_found | :not_completed}
add_waiter(t(), execution_id, from, timer_ref, waiter_monitor_ref) :: t()
pop_waiter(t(), execution_id, from) :: {[waiter()], t()}
remove_waiter_monitor(t(), monitor_ref) :: {[waiter()], t()}
subscribe_logs(t(), execution_id, subscriber, monitor_ref) :: {:ok, replay_entries, t()} | {:error, term()}
unsubscribe_logs(t(), execution_id, subscriber) :: {demonitor_refs, t()}
remove_subscriber_monitor(t(), monitor_ref) :: t()
append_log(t(), execution_id, entry) :: {subscribers, t()}
append_event(t(), execution_id, event) :: t()
finalize(t(), execution_id, result) :: {waiters, cleanup, t()}
cancel_running(t(), execution_id) :: {:ok, execution, cleanup, t()} | {:completed, t()} | {:not_found, t()}
pop_worker_monitor(t(), monitor_ref) :: {execution_id | nil, t()}
diagnostics(t()) :: map()
```

The exact function names can change during implementation, but the contract
should remain centered on lifecycle events, not generic map manipulation.

## Retention Semantics

Use bounded retention by default. Retention should be explicit server state, not
an unbounded anonymous side effect of `await_result/3`.

Recommended defaults:

```elixir
%{
  max_completed_executions: 1_000,
  max_logs_per_execution: 500,
  max_events_per_execution: 500
}
```

Rules:

- Completed executions remain awaitable only while retained.
- `await_result/3` for an evicted execution returns `{:error,
  :execution_not_found}`.
- `subscribe_execution_logs/3` can replay logs only while the execution is
  retained.
- Running executions are never evicted by completed-execution retention.
- Log and event buffers are bounded independently for running and completed
  executions.
- Buffer pruning should drop the oldest entries and increment explicit dropped
  counters.
- Completed-execution pruning should remove execution state, waiter state,
  subscriber state, and completed-order entries together.
- Time-based retention can be added later if there is a concrete product need;
  count-based retention is enough to remove unbounded memory growth in the first
  pass.

Configure retention through `FavnRunner.Server.start_link/1` options and read
application config only once in `FavnRunner.Application` if defaults need to be
overridden. Do not repeatedly read global application env in lifecycle
transitions.

## Cleanup Semantics

Waiters:

- Monitor waiter caller pids when a call is parked.
- On waiter timeout, reply `{:error, :timeout}`, cancel or remove that waiter's
  monitor, and remove the waiter entry.
- On execution finalization, cancel waiter timers, demonitor waiter pids, and
  reply `{:ok, result}` once.
- On waiter process `:DOWN`, cancel that waiter's timer and remove the waiter
  without replying.

Log subscribers:

- Monitor a subscriber pid once no matter how many executions it subscribes to.
- Track subscriber-to-execution membership explicitly.
- On subscriber `:DOWN`, remove the pid from all execution subscriber sets and
  remove monitor bookkeeping.
- On unsubscribe, remove only that execution membership and demonitor the pid
  only when it has no remaining execution subscriptions.
- On execution finalization, stop live fanout for that execution, but keep
  buffered logs replayable until retention evicts the completed execution.

Worker monitors:

- `monitor_to_execution` remains lifecycle-owned.
- Normal worker `:DOWN` after a result should only remove stale monitor
  bookkeeping.
- Abnormal worker `:DOWN` while running should finalize exactly once with a
  worker-crash result.
- Cancellation should terminate the worker, finalize with a cancellation result,
  and ignore a later worker result or monitor message.

## Server Responsibilities After Refactor

`FavnRunner.Server` should still own process mechanics and side effects:

- `GenServer.call/3` and `GenServer.reply/2` protocol handling.
- `Process.send_after/3`, `Process.cancel_timer/1`, `Process.monitor/1`, and
  `Process.demonitor/2` calls.
- `DynamicSupervisor.start_child/2` and `DynamicSupervisor.terminate_child/2`.
- Manifest resolution and SQL preflight before lifecycle insertion.
- Relation inspection and data-plane diagnostics.
- Creating `RunnerResult` values for preflight failure, cancellation, and worker
  crash unless those result builders are moved to a small runner-result helper as
  part of the same lifecycle contract.

`FavnRunner.Server` should stop owning direct map updates for lifecycle state.
Its callbacks should read as protocol dispatch plus lifecycle transition
application.

## Diagnostics Contract

Extend runner diagnostics to expose lifecycle pressure without leaking raw work,
logs, events, or secrets:

```elixir
%{
  available?: true,
  server: FavnRunner.Server,
  in_flight_executions: non_neg_integer(),
  completed_executions: non_neg_integer(),
  waiters: non_neg_integer(),
  log_subscribers: non_neg_integer(),
  retention: %{
    max_completed_executions: pos_integer(),
    max_logs_per_execution: non_neg_integer(),
    max_events_per_execution: non_neg_integer(),
    evicted_completed_executions: non_neg_integer(),
    dropped_logs: non_neg_integer(),
    dropped_events: non_neg_integer()
  },
  data_plane: map()
}
```

Keep current diagnostics keys working where practical, but do not preserve
unbounded retention behavior for compatibility. This is private pre-v1 software,
and bounded memory is the more important contract.

## Implementation Plan

1. Add lifecycle structs and focused unit tests.

Create `FavnRunner.ExecutionLifecycle` and
`FavnRunner.ExecutionLifecycle.Execution`. Move only state operations first:
running insertion, completed insertion, finalization, result lookup, worker
monitor mapping, waiter storage, log subscriber membership, log append, event
append, and diagnostics counts. Keep behavior equivalent except for explicit
state shapes.

2. Move `FavnRunner.Server` to lifecycle-owned state.

Change server state from anonymous maps to `%ExecutionLifecycle{}` plus any
non-lifecycle fields that remain. Server callbacks should delegate state changes
to lifecycle functions and perform process side effects around returned cleanup
instructions.

3. Add bounded completed-execution and buffer retention.

Implement count-based completed retention and per-execution log/event limits.
Apply retention on finalization and on append. Return cleanup instructions for
evicted subscribers and waiters if any retained state is removed. Add diagnostics
counters for evicted completed executions and dropped log/event entries.

4. Add explicit waiter and subscriber cleanup.

Monitor waiter caller pids and subscriber pids. Handle waiter `:DOWN` and
subscriber `:DOWN` separately from worker `:DOWN`. Ensure monitors are
demonitored when no longer needed.

5. Tighten server tests around race behavior.

Preserve existing cancellation, late result, worker crash, await timeout,
invalid argument, and log forwarding tests. Add regression tests for retention,
dead subscribers, abandoned waiters, and concurrent diagnostics.

6. Update structure documentation if the module lands.

After implementation, update `docs/structure/favn_runner.md` to name
`FavnRunner.ExecutionLifecycle` as the owner of runner execution lifecycle state,
retention, waiter cleanup, subscriber cleanup, and diagnostics counts.

## Test Plan

Add focused tests at the runner layer:

- `ExecutionLifecycle` unit tests for running insertion, finalization,
  idempotent late result handling, monitor lookup, waiter add/pop, subscriber
  add/remove, and diagnostics counts.
- Retention tests proving the oldest completed executions are evicted and
  running executions are never evicted.
- Log buffer tests proving replay order is chronological and oldest entries are
  dropped after `max_logs_per_execution`.
- Event buffer tests proving oldest entries are dropped after
  `max_events_per_execution`.
- Server tests proving `await_result/3` returns retained results and returns
  `{:error, :execution_not_found}` after retention eviction.
- Server tests proving dead log subscribers are removed on process death and do
  not receive later entries.
- Server tests proving abandoned await callers are removed on process death and
  do not leave stale waiters.
- Server tests proving worker crash finalizes once and a late result cannot
  overwrite crash or cancellation terminal state.
- Diagnostics tests proving counts reflect concurrent running executions,
  retained completed executions, waiters, subscribers, evictions, and dropped
  buffers.

Run the narrow runner checks first:

```bash
MIX_ENV=test mix do --app favn_runner cmd mix test --no-compile --exclude acceptance --exclude slow --exclude browser
```

Before finishing the implementation branch, run:

```bash
mix format
mix compile --warnings-as-errors
mix test
```

## Out Of Scope

- Changing `Favn.Contracts.RunnerClient` or orchestrator-to-runner public return
  shapes.
- Moving lifecycle state into `favn_core`.
- Introducing admission control or execution concurrency limits. The lifecycle
  extraction should make that future work easier, but it should not implement it.
- Reworking worker asset execution, SQL runtime behavior, SQL session pooling,
  manifest registration, or relation inspection.
- Adding a second supervised lifecycle process unless a later design identifies
  a concrete concurrency or isolation need.
- Removing the currently unused `FavnRunner.ExecutionRegistry` application child
  unless implementation confirms it has no planned role in the lifecycle design.

## Risks And Mitigations

- Risk: Retention eviction can surprise callers that await very late.
  Mitigation: Use generous count defaults, document the behavior in diagnostics,
  and keep orchestrator usage on submit-then-await paths.
- Risk: Monitor bookkeeping can become more complex than the current maps.
  Mitigation: centralize monitor ownership in `ExecutionLifecycle` and test each
  cleanup path directly.
- Risk: Extracting too many helpers recreates the same ambiguity in more files.
  Mitigation: start with one lifecycle module and one execution struct; add
  smaller modules only when a real contract appears.
- Risk: Log replay semantics can change while adding bounds.
  Mitigation: test chronological replay order and oldest-entry pruning explicitly.
- Risk: A new lifecycle process would create message-ordering races.
  Mitigation: keep lifecycle as server-owned state in phase 1.

## Acceptance Criteria

- `FavnRunner.Server` no longer directly mutates execution, waiter, subscriber,
  monitor, or retention maps.
- `FavnRunner.ExecutionLifecycle` owns all runner execution lifecycle state and
  exposes typed transition functions.
- Completed executions and per-execution log/event buffers are bounded by
  explicit retention policy.
- Dead log subscribers and abandoned waiters are cleaned through monitors.
- Cancellation, worker crash, timeout, late result, and completion races finalize
  executions exactly once.
- Diagnostics expose lifecycle counts and retention pressure.
- `FavnRunner`, `Favn.Contracts.RunnerClient`, and orchestrator-facing runner
  behavior stay stable except for documented bounded retention eviction.
- Focused runner tests cover lifecycle transitions, retention, cleanup, races,
  and diagnostics.
