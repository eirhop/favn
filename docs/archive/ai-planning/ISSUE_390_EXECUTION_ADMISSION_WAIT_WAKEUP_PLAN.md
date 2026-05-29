# Issue 390 Execution Admission Wait/Wakeup Plan

Issue: #390

Status: proposed design plan.

## Goal

Replace fixed-interval pipeline admission polling with an explicit
orchestrator-owned wait/wakeup contract.

The new contract should reduce repeated storage-backed acquisition attempts when
capacity is unchanged, preserve storage as the source of truth for admission
capacity, and make queued admission lifecycle behavior visible to `RunServer`.

This follows `docs/archive/ai-planning/refactor_review_standard.md`: the change should expose a real
runtime contract, improve concurrency behavior, and keep control-plane state in
`favn_orchestrator` instead of moving code around cosmetically.

## Current Baseline

- `FavnOrchestrator.ExecutionAdmission.acquire/2` builds admission scopes and
  calls `FavnOrchestrator.Storage.try_acquire_execution_lease/1`.
- `FavnOrchestrator.RunServer.Execution.StageAdmission` persists `:step_queued`
  when storage returns `{:execution_capacity_exceeded, scope}`.
- `FavnOrchestrator.RunServer.Execution` keeps deferred pipeline nodes in
  `StageAttemptState` and schedules `:stage_admission_retry` every `100ms` until
  a deferred entry acquires capacity or the stage admission deadline expires.
- Lease release is centralized through `RunWorkSet.release_entry/1`,
  `ExecutionAdmission.release/1`, and `ExecutionAdmission.release_run/1`, but a
  release only deletes storage rows. It does not wake specific queued runs.
- Storage adapters currently expose lease acquire, release, expire, and list
  callbacks. They do not persist admission waiters or provide ordered wakeup
  candidates.

The existing lease acquire operation remains the correctness boundary. Any new
wakeup path must still re-run storage-backed acquisition before submitting runner
work.

## Architectural Decision

Introduce a small admission wait contract in `favn_orchestrator` backed by
persisted waiter records and an optional supervised local coordinator.

The persisted waiter record is durable orchestration truth: it records that a run
server has deferred a stage entry because a specific admission scope was full.
The coordinator is a BEAM-local delivery mechanism: it indexes live run-server
subscribers, receives release or expiration notifications, and sends wakeup
messages. A lost coordinator message must not corrupt capacity accounting or lose
the queued intent.

Keep all new contract modules under `apps/favn_orchestrator`:

- `FavnOrchestrator.ExecutionAdmission.Waiter`
- `FavnOrchestrator.ExecutionAdmission.Coordinator`
- storage facade functions and adapter callbacks for persisted waiters

Do not move this into `favn_core`. Admission is run lifecycle and control-plane
state, not shared manifest/compiler domain behavior.

## Contract Shape

Add an explicit waiter struct with boring, storage-safe fields:

```elixir
%FavnOrchestrator.ExecutionAdmission.Waiter{
  waiter_id: String.t(),
  run_id: String.t(),
  asset_step_id: String.t(),
  queue_reason: :pipeline_concurrency | :execution_pool | :global_concurrency,
  blocked_scope: %{kind: atom() | String.t(), key: String.t(), limit: pos_integer()},
  requested_scopes: [map()],
  stage: non_neg_integer(),
  attempt: pos_integer(),
  inserted_at: DateTime.t(),
  updated_at: DateTime.t(),
  deadline_at: DateTime.t() | nil,
  wake_generation: non_neg_integer()
}
```

Suggested `ExecutionAdmission` facade functions:

```elixir
@spec acquire_or_wait(RunState.t(), entry(), keyword()) ::
        {:ok, lease() | nil}
        | {:waiting, ExecutionAdmission.Waiter.t()}
        | {:error, term()}

@spec cancel_wait(String.t() | ExecutionAdmission.Waiter.t()) :: :ok
@spec cancel_run_waits(String.t()) :: :ok
@spec release(lease() | nil) :: :ok | {:error, term()}
@spec release_run(String.t()) :: :ok
```

`acquire_or_wait/3` should perform one storage-backed acquire. If capacity is
available, it returns the lease as today. If capacity is exceeded, it upserts a
waiter record for the blocking scope returned by storage and registers the
current run server with the coordinator.

Wakeups are hints. A run server that receives a wakeup must delete or supersede
the old waiter, attempt `acquire_or_wait/3` again, and only submit runner work
after storage grants a lease. If another scope is full by then, the waiter is
updated to the new blocking scope.

## Storage Design

Extend `Favn.Storage.Adapter` and `FavnOrchestrator.Storage` with persisted
waiter operations:

```elixir
@callback upsert_execution_admission_waiter(map(), adapter_opts()) ::
            {:ok, map()} | {:error, error()}

@callback delete_execution_admission_waiter(String.t(), adapter_opts()) ::
            :ok | {:error, error()}

@callback delete_execution_admission_waiters_for_run(String.t(), adapter_opts()) ::
            {:ok, non_neg_integer()} | {:error, error()}

@callback list_execution_admission_waiters_for_scope(map(), keyword(), adapter_opts()) ::
            {:ok, [map()]} | {:error, error()}

@callback expire_execution_admission_waiters(DateTime.t(), adapter_opts()) ::
            {:ok, non_neg_integer()} | {:error, error()}
```

For SQLite and Postgres, add tables similar to:

- `favn_execution_admission_waiters`
- `favn_execution_admission_waiter_scopes` if adapter queries need normalized
  scope identities instead of payload decoding

Store the current blocking scope separately from the full requested scope list.
Wake candidate selection should use the blocking scope identity ordered by
`inserted_at`, then `waiter_id`. The full requested scope list is retained so the
next acquire can validate all constraints and produce precise diagnostics.

The memory adapter should implement the same ordering and idempotency semantics
as SQL adapters so the shared storage contract tests exercise the real behavior.

Lease expiration needs a wake story because expired leases free capacity without
an explicit release call. Prefer extending expiration internals so the
coordinator can learn which scopes lost leases and wake candidates for those
scopes. If keeping the existing `expire_execution_leases/1` return shape, add a
coordinator recovery sweep that lists leases and waiters and wakes candidates
after expiration. Do not reintroduce per-run fixed retry polling as the fallback.

## Coordinator Design

Add a supervised `ExecutionAdmission.Coordinator` under the orchestrator
supervision tree.

Responsibilities:

- Register the live owner process for a persisted waiter id.
- Monitor registered run-server processes and remove in-memory subscriptions on
  `:DOWN`.
- Receive release notifications from `ExecutionAdmission.release/1`,
  `release_run/1`, and lease-expiration cleanup.
- Query storage for a bounded batch of waiters blocked on released scopes.
- Send `{:execution_admission_wakeup, waiter_id, wake_generation}` to registered
  owners.
- Run a low-frequency recovery sweep on startup and after coordinator restart to
  wake currently subscribed waiters whose blocking scope appears to have
  capacity.

The coordinator must not own admission correctness. It does not grant leases,
delete runner work, or decide stage completion. It only delivers wakeup hints to
run servers that already persisted waiters.

Use bounded wake batches per released scope. A release from a pool with limit `1`
should generally wake one candidate first, not every waiter. If the woken run
cannot acquire because another scope is full or it was cancelled, the next
release or recovery sweep can wake the next candidate.

## RunServer Integration

Replace `:stage_admission_retry` as the normal capacity-wait mechanism.

Suggested changes:

- Extend `RunExecutionState` with admission waiter state, for example
  `admission_waiters: %{optional(String.t()) => map()}` and a single admission
  deadline timer.
- Change `StageAdmission.submit/1` to call `ExecutionAdmission.acquire_or_wait/3`
  for deferred pipeline entries.
- Keep `:step_queued` persistence in `StageAdmission`, but persist it once per
  `{asset_step_id, queue_reason, scope}` as today.
- When a stage has deferred entries and no awaits, put the run server in
  `:admission_wait` without scheduling `100ms` retry timers.
- Add `RunServer.handle_info({:execution_admission_wakeup, waiter_id,
  wake_generation}, state)` and route it to `Execution.handle_event/2`.
- On wakeup, ignore stale waiter ids or stale generations. For live waiters,
  remove or supersede the waiter and call the existing stage refill path.
- On external cancellation, terminalization, and timeout, call
  `ExecutionAdmission.cancel_run_waits/1` in addition to existing lease cleanup.

The stage admission deadline should remain run-server owned. It protects the run
from waiting forever even if all wakeup delivery fails. The deadline timer is not
capacity polling; it is the lifecycle timeout for queued stage work.

## Failure And Concurrency Semantics

- Success: release deletes the lease, coordinator wakes the oldest subscribed
  waiter for the released scope, and the run server re-acquires before submit.
- Capacity race: multiple woken run servers may race, but storage acquisition is
  still atomic and only the winners get leases.
- Cancellation: cancelling a run deletes persisted waiters and ignores late
  wakeup messages by waiter id or generation.
- Timeout: stage timeout deletes waiters and terminalizes through the existing
  deferred-stage timeout path.
- Run-server crash: waiters do not consume capacity. Stale persisted waiters are
  removed by run cancellation, run repair, or waiter expiration. A restarted run
  must create or refresh its waiter before it can be woken again.
- Coordinator crash: persisted waiters remain. On restart, the coordinator
  accepts fresh registrations and runs a bounded recovery sweep.
- Storage conflict: duplicate waiter upserts for the same waiter id are
  idempotent when the normalized waiter payload matches, or they update the
  blocking scope and wake generation when the run is still waiting on the same
  stage entry.

## Landing Slices

1. Add `ExecutionAdmission.Waiter` normalization, codec tests, storage facade
   functions, and memory-adapter waiter support.
2. Add shared storage adapter contract tests for waiter upsert, ordered
   scope-based listing, run-scoped deletion, expiration, and idempotent updates.
3. Add SQLite and Postgres migrations plus adapter implementations for persisted
   admission waiters.
4. Add the supervised coordinator and unit tests for registration, monitor
   cleanup, bounded wake batches, stale subscriber handling, and restart recovery.
5. Wire `ExecutionAdmission.acquire_or_wait/3`, `release/1`, `release_run/1`,
   and lease expiration notifications through the coordinator while preserving
   the current `acquire/2` behavior for narrow callers during the transition.
6. Wire `RunServer` and `StageAdmission` to waiters and wakeup messages, keeping
   the existing stage deadline as a timeout only.
7. Remove the fixed `@stage_admission_retry_ms` retry path and any now-unused
   admission retry timer state.
8. Update docs and structure notes for orchestrator admission ownership and the
   storage-backed waiter contract.

## Testing Plan

- Unit-test `ExecutionAdmission.Waiter` normalization for atom-keyed and
  string-keyed maps, invalid scopes, invalid deadlines, and deterministic waiter
  ids.
- Add storage contract tests that run against memory, SQLite, and Postgres
  adapters for ordered waiters, idempotent upserts, run deletion, expiration, and
  scope filtering.
- Add coordinator tests for multiple contending waiters, FIFO wake ordering,
  bounded wake batches, cancellation before wake, late wake after cancellation,
  owner process crash, and coordinator restart.
- Add run-server tests for a pool limit of `1` with multiple pipeline siblings:
  one admitted, one queued, release wakes the queued sibling, and no repeated
  fixed admission retry loop is required.
- Add timeout tests where queued stage work reaches its deadline and waiter rows
  are deleted.
- Add crash/restart tests at the smallest reliable layer: coordinator restart
  must not lose persisted waiters, and stale waiters from dead run servers must
  not block future capacity.

## Non-Goals

- Do not implement a distributed scheduler or cross-BEAM fairness protocol in
  this issue. Storage remains the cross-process correctness boundary; local
  wakeups are an optimization and lifecycle contract.
- Do not move admission or waiter structs to `favn_core`.
- Do not replace storage lease acquisition with coordinator-granted tokens.
- Do not make materialization claim waiting part of this first issue unless the
  same waiter contract is explicitly extended later.

## Risks And Mitigations

- A coordinator adds moving parts. Keep it small, supervised, and limited to
  delivery of wakeup hints.
- Wakeups can be lost. Persist waiter records, keep a deadline timer, and run a
  bounded recovery sweep after coordinator restart.
- Fairness can be approximate under races. Enforce correctness through storage
  leases and use persisted FIFO ordering only for selecting wake candidates.
- Lease expiration can free capacity without a release event. Add explicit
  expiration-triggered wakeups or a coordinator sweep; do not fall back to
  per-run polling.
- Storage migrations add adapter work. Land memory support and shared contract
  tests before SQL migrations to lock behavior.
