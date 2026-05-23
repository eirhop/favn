# Run Cancellation Implementation Plan

Status: single-run cancellation is wired through the local CLI and run-detail UI;
whole-backfill parent cancellation remains future work.

## Goal

Operators must be able to cancel in-flight runs from both the local CLI and the
Phoenix/LiveView UI. Cancellation should be fast, idempotent from the user's
point of view, auditable, visible in live run views, and implemented through the
existing orchestrator-owned run lifecycle rather than a second control path.

## Current Baseline

- The orchestrator already owns the cancellation contract through
  `FavnOrchestrator.cancel_run/2` and `FavnOrchestrator.RunManager.cancel_run/2`.
- The private HTTP API already exposes `POST /api/orchestrator/v1/runs/:run_id/cancel`
  with command idempotency and audit integration.
- `Favn.Dev.OrchestratorClient.cancel_run/4` already knows how to call that HTTP
  endpoint with a deterministic local idempotency key.
- `RunServer` already accepts `{:favn_run_cancel_requested, reason}` and delegates
  cleanup to `RunServer.Execution.cancel/2`.
- `RunServer.Cancellation` already owns the runner cancellation envelope and
  best-effort dispatch to `runner_client.cancel_work/3`.
- The first product surface is now wired: `mix favn.runs cancel RUN_ID` and the
  run-detail LiveView can cancel active non-backfill-parent runs. Run-list row
  actions and whole-backfill parent cancellation remain future work.

## Design Decision

Use the orchestrator as the single system of record for cancellation.

- CLI cancellation goes through `apps/favn_local` and the private orchestrator
  HTTP API, the same way local run submission, rerun, backfill, and inspection
  already work.
- UI cancellation goes through the public `FavnOrchestrator` facade in the same
  BEAM. `favn_view` must not call HTTP, storage, `RunManager`, `RunServer`, or
  runner modules directly.
- The runner remains best-effort and asynchronous from the operator perspective:
  the accepted cancellation request moves the run toward `:cancelled`; active
  runner work is asked to stop, but stale runner execution ids or already-finished
  work must not block the operator command unless the orchestrator cannot persist
  the cancellation.

## Phase 1: Harden The Orchestrator Contract

Keep this phase small. Do not redesign the execution state machine before adding
the CLI/UI surface.

- Follow-up: add or confirm a stable facade error contract for `cancel_run/2`:
  `:not_found`, `:already_terminal`, `:backfill_parent_cancel_not_supported`,
  `{:runner_cancel_failed, reason}`, and storage/persistence failures.
- Treat repeated cancellation of an already cancelled run as success at the API
  and operator facade boundary. Repeated operator clicks or CLI retries should be
  safe.
- Reject successful, failed, timed-out, and non-cancelled terminal runs with a
  stable `:already_terminal` result unless the terminal status is already
  `:cancelled`.
- Preserve `run_cancel_requested` and `run_cancelled` events for new
  cancellations so SSE and LiveView refresh paths remain authoritative.
- Keep generic backfill parent cancellation rejected for now. If product behavior
  needs parent cancellation, add an explicit `cancel_backfill/2` command owned by
  the backfill manager rather than overloading single-run cancellation.

Tests:

- `apps/favn_orchestrator/test/run_manager_test.exs` for already-terminal,
  already-cancelled, active-run, stale-runner-id, and not-found outcomes.
- `apps/favn_orchestrator/test/api/router_test.exs` for HTTP status mapping,
  idempotent replay, audit behavior, and repeated cancellation.

## Phase 2: Add Local CLI Cancellation

Cancellation is exposed as a subcommand of `mix favn.runs`, not `mix favn.run`.
`favn.run` submits a new run; `favn.runs` owns run inspection and run-level
operations.

Implemented UX:

```bash
mix favn.runs cancel RUN_ID
mix favn.runs cancel RUN_ID --wait
mix favn.runs cancel RUN_ID --wait-timeout-ms 30000
mix favn.runs cancel RUN_ID --poll-interval-ms 500
```

Implementation:

- `Favn.Dev.Runs.cancel/2` reuses the existing `session/1` helper and
  calls `OrchestratorClient.cancel_run/4`.
- Local trusted-loopback auth and service-token behavior stays identical to
  `list`, `show`, and `logs` flows.
- Optional `--wait` polling fetches only the requested run by id until terminal
  status or local wait timeout.
- The CLI prints a concise result: run id, accepted/cancelled status, and the next command
  to inspect details if the run is still transitioning.
- `mix favn.reload` stale-run instructions include `mix favn.runs cancel RUN_ID`.

Tests:

- `apps/favn/test/mix_tasks/public_tasks_test.exs` for argument parsing and user
  error text.
- `apps/favn_local/test/dev_orchestrator_client_test.exs` for endpoint,
  idempotency, and response parsing if response handling changes.
- `apps/favn_local/test/dev_runs_test.exs` or equivalent for `Favn.Dev.Runs.cancel/2`
  behavior with a local test HTTP server.

## Phase 3: Add LiveView Cancellation

Expose cancellation in places where an operator naturally sees active work.

- Run detail: add a primary danger action when `@run.active?` is true.
- Runs list: add a compact action on active rows/cards, or start only with detail
  page cancellation if the list is too dense.
- Backfill child/window runs: allow cancelling active child runs through the same
  single-run command. Keep parent-run cancellation hidden or disabled until an
  explicit backfill cancellation contract exists.
- Use a confirmation step for destructive intent. A simple modal or `phx-click`
  confirmation is enough for the first pass.

Implementation:

- `RunDetailLive` handles `"cancel_run"` by calling
  `FavnOrchestrator.cancel_operator_run/2` through the public facade.
- If adding list-row cancellation, add the same event in `RunsListLive` and reload
  only the affected list/read model after success.
- Render "Cancel requested" flash on success and keep live SSE/polling responsible
  for the final status transition.
- Map stable error atoms to UI labels in the LiveView, not in the orchestrator.
- Disable the action while the command is in flight to avoid duplicate clicks;
  duplicate requests should still be safe at the facade/API layer.

Tests:

- LiveView tests for rendering the button only on active runs, calling the cancel
  event, hiding or disabling it for terminal runs, and showing stable error copy.
- Existing orchestrator facade tests remain the behavior authority; LiveView tests
  should not assert storage internals.

## Phase 4: Optional Backfill Parent Cancellation

Generic `cancel_run/2` intentionally rejects backfill parent runs today. If users
expect "Cancel backfill" from parent-run pages, add a separate command.

Suggested contract:

```elixir
FavnOrchestrator.cancel_backfill(backfill_run_id, actor_context_or_reason)
```

Semantics:

- Persist cancellation on the parent run.
- Cancel active child runs through `cancel_run/2` using a shared cancellation
  reason that includes `backfill_run_id`.
- Mark pending/not-yet-submitted windows cancelled only through the backfill
  manager's ledger contract, not by manufacturing child runs.
- Reproject parent progress from child/window states after cancellation.

This is product-critical for large operational backfills, but it should not block
single-run cancellation from CLI/UI.

## Refactoring Opportunities

- Extract local run polling from `Favn.Dev.Run` into a small `Favn.Dev.RunWait`
  or `Favn.Dev.Runs.Wait` helper only if `favn.runs cancel --wait` needs it.
  Keep it in `favn_local`; do not move it to `favn_core`.
- Add a tiny shared status predicate for local tooling terminal statuses instead
  of duplicating `ok/error/cancelled/timed_out` checks across `run`, `runs`, and
  `backfill` wait paths.
- Centralize CLI formatting for orchestrator HTTP errors used by `favn.run`,
  `favn.runs`, and `favn.backfill` only if the third call site appears during the
  implementation.
- Keep `RunServer.Cancellation` as the only runner cancellation envelope builder.
  Do not let `RunManager`, API router, CLI, or UI construct runner cancellation
  payloads.
- Consider extracting a public orchestrator operator command DTO for cancellation
  only if UI/API/CLI inputs diverge. A DTO is useful for stable reason/audit data;
  it is unnecessary if the command remains `run_id + actor context`.

## Performance Improvements

- Prefer SSE/live run events for UI refresh; keep polling only as fallback. The
  current run detail and run list LiveViews already subscribe to run events and
  poll active views, so cancellation should rely on those paths rather than adding
  a new polling loop.
- For CLI `--wait`, poll one run by id with a bounded interval and timeout. Do not
  list all runs while waiting for cancellation.
- Keep cancellation dispatch outside long blocking GenServer work. The existing
  lifecycle plan in `docs/ISSUE_389_RUN_EXECUTION_LIFECYCLE_PLAN.md` should still
  remove blocking sleeps from retry paths so cancellation is responsive during
  retry backoff and admission waits.
- Avoid broad scans to decide whether a run is cancellable. Use `get_run(run_id)`
  and active run-server state already held by `RunManager`.
- Do not retry runner cancellation blindly. Runner cancellation can be best-effort;
  retries belong around safe inspection/query paths, not unknown data-plane
  mutation or materialization outcomes.

## Acceptance Criteria

- `mix favn.runs cancel RUN_ID` accepts cancellation for an active run and exits
  successfully.
- `mix favn.runs cancel RUN_ID --wait` exits only after the run is terminal or the
  local wait timeout expires.
- Run detail UI shows a cancel action for active runs and no cancel action for
  terminal runs.
- Successful UI cancellation shows immediate feedback and the run transitions to
  cancelled through the existing live update path.
- API and same-BEAM facade cancellation remain audited and idempotent for operator
  retries.
- Backfill parent runs either show a clear unsupported state or use the future
  explicit backfill cancellation command; they must not pretend generic
  single-run cancellation is sufficient.
