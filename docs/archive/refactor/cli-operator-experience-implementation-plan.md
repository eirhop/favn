# CLI Operator Experience Implementation Plan

Status: temporary implementation plan. This is not a product contract. Replace
it with updates to canonical architecture, operator, public guide, and module
documentation as each phase is implemented.

## Reader And Outcome

Reader: Favn contributors implementing and reviewing the local CLI operator
workflow.

Outcome: a clean example project can prepare data, run current or exact windows,
inspect data, retry, cancel, backfill, operate schedules, and rebuild changed
targets entirely through supported CLI commands.

## Scope And Assumptions

- CLI is the first supported operator surface. UI activation and other UI
  lifecycle work are deferred, but must later call the same orchestrator
  facades.
- PostgreSQL remains the only control-plane store.
- The orchestrator owns run, schedule, activation, cancellation, and rebuild
  truth.
- The runner owns SQL sessions and execution.
- Schedule definitions are manifest truth. Whether a schedule is active is
  workspace-specific PostgreSQL truth.
- A schedule without an explicit workspace activation record is disabled.
- Existing schedules are disabled once when the new activation model is
  introduced. Operators must review and activate them.
- An unchanged schedule definition may preserve an explicit activation across
  manifest deployments. A changed schedule fingerprint requires review and is
  disabled until activated again.
- Favn does not expose arbitrary operator SQL. Structured inspection remains in
  `mix favn.inspect`; direct SQL belongs in native database tooling.
- Favn is private pre-v1 software, so prefer clean breaking changes over
  compatibility aliases.

## Decisions

### Run Windows

`mix favn.run PIPELINE` resolves one latest complete window from the manifest
policy at submission time. The orchestrator persists the evaluation time and
resolved selection before dispatch.

`--window` remains an optional override for one exact anchor. Ranges are
rejected with guidance to use `mix favn.backfill`.

Retries, reruns, recovery, and cancellation reconciliation reuse the persisted
selection. They never recalculate “current” from a later wall clock.

### Schedule Activation

Remove schedule activation from authoring and manifest authority. The manifest
declares cron, timezone, missed-occurrence behavior, overlap behavior, target,
and window behavior. It does not activate work in any workspace.

Activation is a separate, durable workspace decision:

- no activation record: disabled;
- matching enabled record: enabled;
- matching disabled record: disabled;
- enabled record with a different schedule fingerprint: `needs_review` and
  effectively disabled.

Activation begins with the first occurrence after the activation command time.
It does not catch up older occurrences. Deactivation stops future submissions
but does not cancel runs that were already submitted.

### SQL Queries

Removed by product decision. Reusing the runner's read-write DuckDB session
cannot provide a database-enforced read-only boundary, while a separate
read-only handle conflicts with the one-session local model on Windows.
Operators use `mix favn.inspect` for structured metadata and samples. They stop
Favn before opening a file-backed database with the DuckDB CLI for arbitrary
SQL.

## Public CLI Shape

```text
mix favn.run PIPELINE
mix favn.run PIPELINE --window day:2026-07-23

mix favn.schedules list
mix favn.schedules show SCHEDULE_ID
mix favn.schedules preview SCHEDULE_ID --limit 5
mix favn.schedules activate SCHEDULE_ID --reason REASON
mix favn.schedules deactivate SCHEDULE_ID --reason REASON

```

`activate` and `deactivate` are idempotent. Their output includes the workspace,
stable schedule id, fingerprint, previous/effective state, command time, and
next due time when enabled.

## Phase 1: Unify Local CLI Authorization

1. Define one explicit local CLI authority contract:
   - the local service token has explicit `:platform_operator`;
   - the automatic local actor has every required local workspace role
     explicitly, including operator and administrator capabilities;
   - exact-role semantics remain unchanged outside trusted local development.
2. Route ordinary runs, cancellation, backfill, schedules, and rebuilds through
   the same service-plus-actor session resolution.
3. Keep production authorization unchanged.
4. Return structured `401`, `403`, and validation errors consistently.
5. Add an endpoint matrix test covering list/read/mutate behavior for every CLI
   operator route.

Expected consequence: a user who can submit a local run can also use authorized
local backfill, cancellation, schedule, and rebuild commands without manually
constructing credentials.

## Phase 2: Persist Workspace Schedule Activation

1. Remove `active` from `Favn.Triggers.Schedule`, manifest schedule DTOs, public
   authoring docs, examples, and `Favn.AI`.
2. Bump the manifest schema for the clean breaking change.
3. Add a PostgreSQL `schedule_activations` capability table keyed by:
   - workspace id;
   - pipeline target id;
   - stable schedule id.
4. Persist:
   - enabled or disabled state;
   - approved schedule fingerprint;
   - optimistic version;
   - actor, reason, command id, and timestamps.
5. Treat absence as disabled. Migrate existing workspaces with no enabled
   records, intentionally deactivating existing schedules.
6. Deploy all selected schedule definitions into the active deployment
   catalogue, including disabled definitions. Do not filter them out using the
   old manifest `active` field.
7. Derive effective state by joining the active deployment definition with the
   workspace activation:
   - matching fingerprint and enabled → `enabled`;
   - missing or disabled → `disabled`;
   - fingerprint mismatch → `needs_review`.
8. Preserve activation across deployments only when stable identity and
   schedule fingerprint match.

Expected consequence: publishing code cannot silently start scheduled work in
any workspace. Each workspace must activate a reviewed schedule explicitly.

## Phase 3: Implement Schedule Activation Lifecycle

1. Add typed persistence commands and results for activate and deactivate.
2. Use optimistic versioning and command idempotency.
3. On activation:
   - re-read the active deployment definition;
   - pin its current fingerprint;
   - calculate the first due time strictly after command time;
   - create or reset its cursor without missed catch-up;
   - write an audit event.
4. On deactivation:
   - persist disabled state;
   - prevent new occurrence intents;
   - release or invalidate schedule claims;
   - leave already submitted runs unchanged;
   - write an audit event.
5. Before dispatching a claimed occurrence, recheck that activation is still
   enabled for the same fingerprint. If not, mark the occurrence suppressed
   rather than submitting a run.
6. Make repeated activate/deactivate commands return the current state without
   duplicate side effects.
7. Expose bounded orchestrator facade functions and private API routes:

```text
GET  /api/orchestrator/v1/schedules
GET  /api/orchestrator/v1/schedules/:id
GET  /api/orchestrator/v1/schedules/:id/occurrences/preview
POST /api/orchestrator/v1/schedules/:id/activate
POST /api/orchestrator/v1/schedules/:id/deactivate
```

8. Implement `mix favn.schedules` list, show, preview, activate, and deactivate.
9. Fix local scheduler startup by passing `workspace_ids`, not `workspaces`, to
   `FavnOrchestrator.Scheduler.PersistenceRuntime`.

Expected consequence: schedules are safe by default, activation is auditable,
and disabling prevents future submissions without pretending to cancel work
that already exists.

## Phase 4: Repair Cancellation

1. Reproduce the current HTTP 500 through the API boundary and identify whether
   failure occurs in idempotency, persistence, run ownership, or runner
   acknowledgement.
2. Persist cancellation intent before contacting the runner.
3. Make runner cancellation idempotent and bounded.
4. Record `cancelled` only after acknowledgement or proof that no active runner
   work remains.
5. Preserve `ok` if execution committed success before cancellation won the
   race.
6. Represent an inconclusive cancellation as an explicit unknown/reconciling
   state instead of HTTP 500 with lost intent.
7. Ensure `mix favn.runs cancel --wait` reports the final persisted outcome.

Expected consequence: operators can trust cancellation status, retry a command
safely, and distinguish cancellation from a run that completed first.

## Phase 5: Repair Backfill And Rebuild CLI Paths

1. Apply the shared local authorization contract to backfill planning,
   submission, window listing, and repair.
2. Apply it to rebuild plan, start, status, cancel, retry, and reconcile.
3. Preserve rebuild role separation: planning requires operator capability;
   start and mutation controls require administrator capability.
4. Keep immutable plan/hash approval and stale-plan rejection unchanged.
5. Return safe structured errors instead of the generic “inspect orchestrator
   logs” message.

Expected consequence: the existing backfill and rebuild engines become usable
through the supported local CLI without weakening their safety contracts.

## Phases 6-7: Remove `mix favn.query`

The proposed runner-owned query contract was rejected during security review.
SQL keyword filtering cannot make an existing read-write DuckDB session
read-only, and opening a second read-only handle conflicts with the local
one-session model. Remove the Mix task, CLI helper, API route, runner callback,
contracts, tests, and public documentation instead of shipping a misleading
boundary.

Expected consequence: Favn has no arbitrary SQL surface. Structured inspection
remains supported, while direct SQL requires stopping Favn and deliberately
opening DuckDB with native tooling.

## Phase 8: Default Manual Runs To The Latest Complete Window

1. Define a typed manual-window resolution command owned by the orchestrator.
2. For a windowed pipeline with no `--window`, resolve the latest complete
   anchor using:
   - pipeline window kind;
   - effective timezone;
   - availability delay;
   - one persisted evaluation time.
3. Persist requested/effective selection before dispatch.
4. Keep one explicit `--window` override.
5. Reject range syntax with a direct backfill command example.
6. Preserve exact selection for retries, reruns, recovery, and replay.
7. Define behavior for:
   - non-windowed assets and pipelines;
   - windowed assets invoked directly;
   - policies using current versus previous-complete periods;
   - evaluation around timezone and daylight-saving transitions.

Expected consequence: the common command runs the intended current data without
losing historical reproducibility.

## Phase 9: CLI Read Models And Errors

1. Populate run-list target labels from persisted target references so
   `favn.runs list` does not display `target=n/a`.
2. Standardize command errors across run, backfill, rebuild, schedules, and
   cancellation:
   - HTTP status;
   - stable error code;
   - short safe message;
   - actionable next step;
   - bounded details.
3. Never expose tokens, SQL literals, database credentials, or secret runtime
   inputs.
4. Keep list and history operations cursor-bounded.

Expected consequence: users can diagnose normal failures from CLI output
without opening server logs or inspecting huge JSON documents.

## Phase 10: Example Project And Documentation

1. Keep the example's one-file DuckDB connection and isolated path/workspace
   overrides.
2. Keep lifecycle retry, cancellation, schedule, and schema-change probes.
3. Remove `active: true` from the schedule fixture after the DSL change.
4. Update the manual sequence:

```text
mix favn.dev --scheduler
mix favn.schedules list
mix favn.schedules preview SCHEDULE_ID
mix favn.schedules activate SCHEDULE_ID --reason manual-qa
mix favn.schedules deactivate SCHEDULE_ID --reason manual-qa-complete
```

5. Update canonical operator docs, public guides, Mix-task help, moduledocs,
   `docs/FEATURES.md`, and `Favn.AI`.
6. Keep this temporary plan and the lifecycle QA report out of public HexDocs.

Expected consequence: the example demonstrates the real safe operator workflow
instead of activating schedules merely by compiling code.

## Verification Plan

### Focused tests

- Manifest compilation rejects the removed `active` schedule option.
- A newly deployed schedule is listed as disabled in every workspace.
- Activation in workspace A does not activate workspace B.
- An unchanged fingerprint preserves explicit activation across deployment.
- A changed fingerprint becomes `needs_review` and does not submit.
- Activation starts with the first future due time and performs no catch-up.
- Deactivation suppresses undispatched occurrences and leaves submitted runs
  untouched.
- Duplicate activation/deactivation commands are idempotent.
- Scheduler claims only enabled, fingerprint-matching definitions.
- Local scheduler starts with the selected workspace id.
- Cancellation covers pending, running, already-successful, repeated,
  runner-unavailable, and unknown-reply cases.
- Backfill and rebuild authorization works through the automatic local session.
- Rebuild role separation and stale-plan safety remain intact.
- `mix favn.query` and its private API/runner contracts are absent.
- No-window runs pin one latest complete anchor.
- Explicit-window runs pin exactly the requested anchor.
- Range input points users to backfill.
- Run list entries include useful target labels.

### Manual CLI acceptance

Run the generic CRM example in one disposable workspace and DuckDB path:

1. reload and bootstrap;
2. run the default latest-complete window;
3. run one explicit historical window;
4. dry-run and submit a two-window backfill;
5. verify authored retry and operator retry override;
6. cancel the cancellation probe;
7. start with scheduler enabled and verify no schedule runs;
8. list, preview, activate, observe two runs, then deactivate and observe none;
9. switch to V2, verify `rebuild_required`, plan/start the rebuild, and verify
    the V2 table;
10. exercise stale rebuild approval and supported cancellation/retry controls;
11. stop Favn and inspect final relations with the DuckDB CLI.

Record run, schedule, plan, and operation ids in the temporary lifecycle QA
document.

## Implementation Order

1. Shared local authorization and structured errors.
2. Schedule activation persistence and migration.
3. Schedule lifecycle/API/CLI plus local scheduler startup fix.
4. Cancellation repair.
5. Backfill and rebuild CLI repairs.
6. Remove the arbitrary SQL command and its draft contracts.
7. Default latest-complete run-window behavior.
8. Run-list target labels and remaining CLI diagnostics.
9. Example updates, acceptance tests, canonical docs, and `Favn.AI`.

This order first restores access to existing engines, then introduces the new
schedule contract, removes the unsafe query surface, and finally improves
defaults and presentation.

## Stop Conditions

Stop implementation and report before:

- weakening production authorization to make local commands pass;
- inferring schedule activation from manifest publication;
- activating a changed schedule fingerprint automatically;
- dispatching an occurrence after deactivation without a final activation
  recheck;
- opening a second DuckDB coordination domain while the runner is active;
- reintroducing arbitrary operator SQL without a genuinely read-only,
  single-session design;
- recomputing a run window during retry or recovery;
- retrying a cancellation, rebuild activation, or SQL write whose outcome is
  unknown.
