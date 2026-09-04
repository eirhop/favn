# Operate Runs And Schedules

Reader: operators using Favn runtime tooling.

Documentation type: how-to guide.

Use this guide when you need to register a manifest, activate it, submit a run,
inspect run state, cancel work, retry work, operate schedules, or diagnose
runtime state.

A manifest is the saved description of authored Favn work. It contains assets,
pipelines, schedules, dependencies, and runtime metadata.

A run is one admitted attempt to execute an asset, pipeline, scheduled occurrence,
or backfill child. Before admission, the orchestrator durably records the
accepted submission intent and reserves its run id.

For production database startup, backup, and restore, use
`docs/production/postgresql_operator_runbook.md`. Image deployment, upgrade,
and rollback procedures are in `docs/production/control_plane_image.md`.

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

If the manifest contains execution-pool defaults, activation must explicitly
approve them. `mix favn.activate` is an explicit activation command and sends
that approval. API callers use the `execution_pool_policy` object:

```json
{
  "execution_pool_policy": {
    "approve_manifest_defaults": true,
    "overrides": {"partner_api": {"max_concurrency": 4}},
    "reset": ["warehouse"],
    "discard_orphaned": ["retired_pool"]
  }
}
```

`overrides` replaces the manifest default for named active pools. `reset`
removes an existing override so the manifest default applies. When a later
manifest removes a pool, its override becomes inactive and does not silently
return if that pool is reintroduced; `discard_orphaned` permanently removes
such an inactive override. Omit fields that are not part of the change.

Activation creates one immutable deployment configuration. A concurrent
activation is replanned against the winner so an operator override is not lost.
Lowering concurrency below the number of active leases returns a conflict and
leaves the previous deployment active; wait for work to drain, then retry with
the same intended policy. Circuit policy changes serialize with admission, so
overlapping old and new runs use the active policy for a pool that still exists.
Lowering a failure threshold immediately opens a closed circuit whose retained
failure count already meets the new threshold. Changing the probe delay
recomputes the next probe time for an open circuit; a live half-open probe keeps
its current lease. Disabling a circuit stops using it but retains its health
state, which is reconciled against the policy if the circuit is enabled again.

`mix favn.activate` uses a finite 180-second request timeout by default and then
spends at most 10 seconds reconciling a lost response or unknown gateway
outcome against the workspace's authoritative active manifest. Configure those
budgets with `--timeout-ms` (maximum 900000) and
`--reconcile-timeout-ms` (maximum 60000).
Favn creates a fresh operation id when one is omitted. CI/CD should pass an
explicit non-secret `--operation-id` and must reuse it for retries of that
activation.

Reconciled success means the exact requested manifest was proven active in the
exact workspace. `activation_outcome_unknown` means the bounded read could not
prove success or failure; retry only with the same operation id. Do not treat
it as a rejection, start a new operation, scrape logs, or read PostgreSQL. A
normal validation or compatibility response is still a definitive error.

Common failures:

| Failure | Action |
| --- | --- |
| Manifest validation error | Rebuild the manifest from authoring code and inspect diagnostics. |
| Manifest version conflict | Check the version id and content hash. Do not reuse one version id for different content. |
| Pool defaults not approved | Approve the manifest defaults explicitly, then activate the same reviewed manifest. |
| Invalid pool override | Use only pools present in the manifest and provide a complete valid policy for each override. |
| Capacity decrease conflict | Wait until active leases are at or below the requested limit, then retry activation. |
| Persistence failure | Check orchestrator readiness, storage readiness, and diagnostics before retrying. |
| Activation outcome unknown | Retry the exact request with the same operation id; do not start a new activation identity. |
| Runner registration failure | Fix runner availability, then retry the supported registration action. Do not edit runner memory or files directly. |

## Submit A Run

1. Read the active manifest or choose an explicit manifest version.
2. Choose one valid target: an asset, pipeline, scheduled occurrence, or backfill
   child target exposed by operator tooling.
3. Submit the run through the UI, API, CLI, or `mix favn.run` when available for
   your environment.
4. Record the returned run id.
5. Inspect the submission or run through orchestrator-backed tooling such as
   submission diagnostics, run history, run detail, logs, or status.

Expected result: the private HTTP API returns `202 Accepted` after the durable
submission is committed. A bounded worker plans and admits it asynchronously,
then the orchestrator dispatches pinned work and records the final run state.
During that interval, `GET /api/orchestrator/v1/runs/<run-id>` returns the
accepted run identity together with its submission state instead of reporting
the run as missing. Cancellation by that id is valid during preparation and admission.

Submission diagnostics expose total and per-state counts, queued and active
depth, oldest queued time, retries, pending cancellations, and safe, permanent,
or unknown failures. Authenticated viewers can inspect
`GET /api/orchestrator/v1/runs/submissions`,
`GET /api/orchestrator/v1/runs/submissions/<run-id>`, and
`GET /api/orchestrator/v1/runs/submissions/stats`. A failed submission is not
silently converted into a missing run.

The run detail page follows the same contract. Before admission it shows the
durable `queued`, `preparing`, or `starting` submission state. A preparation
failure remains attached to the reserved run id and links to `/runners` instead
of becoming a not-found page.

Use `/runners` to inspect runner health:

- workspace task counters and capacity per pool and release, so queued work
  with no compatible connected runner is called out directly;
- connected runners and their current pool, release, capabilities, and live
  state;
- durable runner session history: when each runner registered, how long it was
  awake, what it completed, and whether it shut down, crashed, or is presumed
  dead after a control-plane restart, with date and state filters and busy/idle
  totals over the selected window.

Live runner presence is process-local and disappears when a runner disconnects;
session history is durable and survives runner and control-plane restarts. A
crashed session records the interrupted task, and expanding a session lists its
failed tasks from your workspace with the redacted terminal error and
remediation. For a missing DuckDB ADBC driver, that detail preserves the
redacted driver error and points local development to `DUCKDB_ADBC_DRIVER`.
Repeated short-lived sessions that die before claiming work collapse into one
"struggling to start" entry. Run and asset outcomes stay on the runs pages.

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

### Understand Window Selections

A scheduled pipeline run records its one requested anchor, the pipeline
`lookback` expansion, and the resulting effective anchors separately. Manual
window submissions and explicit backfill ranges use no expansion: the requested
anchors and effective anchors are identical. Assets map only the effective
anchors to their own concrete runtime windows; neither the planner nor runner
adds lookback.

Reruns and retry-remaining submissions preserve the original selection so the
same effective anchors are planned again. Inspect `window_selection` in run
detail or API output when verifying which anchors were requested and executed.

### Inspect And Repair Asset Coverage

Coverage answers a different question from freshness: coverage checks whether
every window expected at one recorded evaluation time has successful evidence
in the asset's active evidence generation. A successful zero-row result counts
as covered. Failed, running, skipped, retired-generation, and candidate-generation
results do not.

The asset catalogue and detail page show coverage as `complete`, `incomplete`,
or `unknown` independently from run health and freshness. Unknown responses name
the reason: coverage is undeclared, the asset is not windowed, a persisted target
has no active generation, or authoritative state is unavailable. The detail page
shows the declared and environment-effective start, expected-through boundary,
availability delay, and counts.

Its Coverage page draws one calendar unit at a time — a day of hours, a month of
days, a year of months, or every year at once — with each expected period marked
covered or missing, and steps back through the range to where coverage starts.
Selecting missing periods there plans a backfill over exactly those, which is the
same operation the CLI performs below.

To repair gaps from the CLI:

```bash
mix favn.backfill missing-plan MyApp.Assets.Orders --plan-file coverage-plan.json
# Review the printed checksum and every exact window key.
mix favn.backfill missing-submit MyApp.Assets.Orders --plan-file coverage-plan.json
```

The plan pins the active manifest, deployment, evidence generation, target
generation when present, evaluation time, checksum, and exact keys. Submission
re-evaluates that same selection. If any pinned identity or gap changes, it
returns `coverage_selection_stale`; create and review a new plan. One submission
is limited to 10,000 windows. Use a page selection of at most 500 windows when
the complete missing set is larger. Coverage evaluation itself stops above
100,000 expected windows rather than loading an unbounded history.

The accepted backfill retains the approved generation pin. Each delayed child
checks it again before planning, so a generation change after submission fails
as `coverage_selection_stale` instead of writing the newer generation.

The private operator API exposes the same contract at:

```text
GET  /api/orchestrator/v1/coverage/assets/:target_id
GET  /api/orchestrator/v1/coverage/assets/:target_id/missing
POST /api/orchestrator/v1/coverage/assets/:target_id/backfill/plan
POST /api/orchestrator/v1/coverage/assets/:target_id/backfill
```

Missing-window pages default to 100 and accept at most 500. Their opaque cursor
pins the evaluated boundary, manifest, evidence generation, target generation,
and checksum; stale cursors return a conflict instead of mixing evaluations.

### Inspect Target Compatibility

For every persisted SQL table, deployment compares the desired manifest target
descriptor with the active generation and an inspection of the physical
relation. The result is persisted with the deployment and appears separately
from health, freshness, and coverage in the asset catalogue and asset detail.

| Status | Ordinary writes | Meaning |
| --- | --- | --- |
| `ready` | Allowed | The desired descriptor and physical relation are compatible. |
| `uninitialized` | Allowed | No managed relation exists yet; the first successful materialization may establish it. |
| `rebuild_available` | Allowed | Transformation semantics changed, but the physical write contract remains compatible. |
| `rebuild_required` | Blocked | The desired contract, grain, materialization, relation, connection, or window identity is incompatible with the active generation. |
| `unexpected_drift` | Blocked | The observed physical relation no longer matches the recorded active fingerprint. |
| `operator_decision` | Blocked | Favn cannot prove ownership or safe compatibility, including an unmanaged pre-existing relation. |

A deployment containing a blocked target remains active so operators can inspect
it, and stable reads continue against the active relation. Before an ordinary
run starts, the orchestrator checks every persisted target on the selected
dependency path. It returns the exact blocked target and structured reason
instead of attempting a write. Assets outside that path remain runnable.

For `rebuild_required`, review the desired-versus-active field diff before
changing the target. For `unexpected_drift`, compare the observed relation with
the recorded generation and investigate out-of-band DDL. For
`operator_decision`, establish ownership explicitly; Favn never adopts an
unmanaged relation automatically. Repeated ordinary runs cannot clear any of
these blocking states.

### Recover An Interrupted Initial Materialization

Use target recovery only when Favn successfully materialized its initial
generation but lost the control-plane activation step. Open the blocked asset
and choose **Recover ownership**, or open `/recoveries`.

Planning first persists its control-plane intent, then queues read-only runner
tasks for the fresh physical fingerprint and exact pre-existing Favn generation
marker. It also requires the original Favn-created `building` generation, its
successful materialization and historical descriptor, and the current logical
relation and contract. An unmarked or unbound table cannot be recovered even
when its schema looks identical. Tables whose markers predate relation-instance
binding are deliberately refused. An interrupted initial generation has no
active generation, so it is not eligible for a normal managed rebuild. The
planning intent remains resumable across temporary runner unavailability;
conclusive invalid or stale evidence closes it as a durable failed operation.
The operator UI retains an opaque attempt identity in the recovery URL so a
refresh or LiveView restart resumes the same durable intent; the operator reason
is restored from the operation and is not copied into the URL.
The supported non-destructive fallback is to restore a coordinated, verified
control-plane and data-plane backup that contains the matching active binding
and bound marker. If no such checkpoint exists, preserve the relation and
escalate for an audited remediation; Favn has no generic in-place command that
can prove ownership of that legacy table. Never reset only the control-plane
database or assert ownership from schema alone.

An administrator starts the exact plan id and hash. Favn persists intent, then
atomically rechecks the operation
fence, binding version, materialization, generation, fingerprint, and exact
table-bound marker before activating the binding. **Reconcile marker** only
rereads that existing marker and never creates or replaces one.

This workflow cannot adopt a manually created or otherwise unproven table. A
mismatch stops recovery and leaves ordinary writes blocked.

```text
mix favn.recover plan ASSET --reason REASON
mix favn.recover start PLAN_ID --plan-hash HASH
mix favn.recover status OPERATION_ID
mix favn.recover reconcile OPERATION_ID
```

The private service API exposes the same contract:

```text
POST /api/orchestrator/v1/target-recoveries/plan
POST /api/orchestrator/v1/target-recoveries
GET  /api/orchestrator/v1/target-recoveries/:operation_id
POST /api/orchestrator/v1/target-recoveries/:operation_id/reconcile
```

Planning requires operator authority. Start and reconcile require administrator
authority. Every accepted or rejected mutation records bounded actor/session
audit evidence.

### Rebuild A Managed Target

Use a rebuild only for a managed persisted SQL target whose active generation
must be replaced. Unexpected drift and ownership-unknown targets require an
operator decision first; Favn does not use rebuild as implicit adoption.

1. Open the blocked asset and follow **Plan rebuild**, or open `/rebuilds`.
2. Enter the target and a bounded operator reason.
3. Review the immutable plan, including expiry, hash, affected targets, action
   order, exact work items, generation pins, and downstream impact.
4. An administrator explicitly starts that exact plan.
5. Follow operation progress and logical items. The old active generation stays
   readable while the candidate is built and validated.
6. If cancellation is necessary, provide a reason and wait for durable cleanup.
7. Use **Retry** only when the server exposes it. Use **Reconcile** when
   activation is unknown.

Planning never mutates customer data. Start returns a conflict if the manifest,
generation, physical fingerprint, coverage selection, mapping proof, or runtime
input expectation changed after planning. Create and review a new plan instead
of forcing the stale one.

`activation_unknown` means the activation request may have committed even though
the control plane did not receive a conclusive reply. Ordinary writes remain
blocked. Reconciliation reads the runner's authoritative marker and either
continues from the candidate, resumes from the previous generation, or preserves
the unknown state when neither can be proven. Never retry the activation blindly.

The local CLI keeps approval explicit:

```text
mix favn.rebuild plan ASSET --reason REASON
mix favn.rebuild start PLAN_ID --plan-hash HASH
mix favn.rebuild status OPERATION_ID
mix favn.rebuild cancel OPERATION_ID --reason REASON
mix favn.rebuild retry OPERATION_ID
mix favn.rebuild reconcile OPERATION_ID
```

The private service API uses the same workspace authorization and immutable
command contract:

```text
POST /api/orchestrator/v1/rebuilds/plan
POST /api/orchestrator/v1/rebuilds
GET  /api/orchestrator/v1/rebuilds
GET  /api/orchestrator/v1/rebuilds/:operation_id
GET  /api/orchestrator/v1/rebuilds/:operation_id/items
POST /api/orchestrator/v1/rebuilds/:operation_id/cancel
POST /api/orchestrator/v1/rebuilds/:operation_id/retry
POST /api/orchestrator/v1/rebuilds/:operation_id/reconcile
```

Planning requires operator authority. Start, cancel, retry, and reconcile require
administrator authority. Mutations require an idempotency key and write bounded
actor/session audit evidence for accepted and rejected attempts, including
whether an accepted result was an idempotent replay. Operation and item pages
use opaque keyset cursors, default to 100 rows, and accept at most 200.

Common failures:

| Failure | Action |
| --- | --- |
| Unknown target | Refresh the manifest target list and submit a valid target id. |
| No active manifest | Register and activate a manifest, or submit against an explicit manifest version. |
| Forbidden | Use an operator or admin actor allowed to submit runs. |
| Runner unavailable | Check diagnostics and runner readiness. Do not bypass the orchestrator. |
| Missing runtime config | Add the environment variable or secret named by diagnostics, then retry. |
| Duplicate command key | Re-read the existing command result instead of submitting the same side effect again. |
| Submission remains queued | Check oldest queued age, worker concurrency, PostgreSQL health, and retry state. |
| Submission failed | Inspect its failure kind. Retry only safe failures; investigate unknown admission outcomes before resubmitting. |
| Invalid dependency/refresh combination | Use `dependencies=all` with `force_selected_upstream`, or choose a target-only refresh mode. |
| Resource circuit open | Inspect the blocking execution pool or SQL connection, consecutive count, threshold, and next probe time. Unrelated branches continue. |
| Rebuild required | Inspect the compatibility diff. Do not retry the ordinary write against the incompatible active generation. |
| Target drift | Investigate the observed physical fingerprint and any out-of-band DDL before changing control-plane state. |
| Operator decision required | Confirm ownership and impact explicitly. Do not adopt or overwrite the relation automatically. |

## Inspect A Run

1. Start with the run id returned by submission.
2. Read the run summary to see current state. `mix favn.runs list` prints the
   persisted pipeline identity when available and otherwise the persisted asset
   target refs, so completed history does not depend on the current manifest.
3. Open the run detail to inspect events, attempts, windows, logs, and errors
   exposed by operator tooling.
4. If the run is still active, check runner availability and in-flight diagnostics
   before deciding whether to cancel.
5. If the run is terminal, use the terminal state and events to decide whether to
   retry, rerun, or leave it unchanged.

Use orchestrator-backed views. Do not infer final state from UI loading state,
raw storage rows, or runner memory.

CLI failures use one bounded operator-safe shape: operation, HTTP status when
available, stable error code, short message, allowlisted scalar details, and an
actionable next step. The CLI never prints arbitrary response payloads,
credentials, runtime-input secrets, or SQL text. Use the stable code for
automation; use the next step for interactive recovery.

## Cancel A Run

1. Open the run detail and choose **Cancel run** or **Cancel full backfill**.
   The action covers the submitted operation even when a completed window is selected.
2. Confirm the scope and wait for **Cancelling** to finish.
3. If the page shows **Needs attention**, inspect the preserved diagnostics before
   submitting replacement work.

The [runtime cancellation contract](../../apps/favn/guides/runtime-model.md#cancellation-and-retry)
explains ownership, restart cleanup and the narrower exact-run HTTP/CLI commands.
A request acknowledgement alone does not prove cancellation. Do not change run
status by editing storage or call runner internals directly.

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

If the pipeline declares `resource_recovery :retry_remaining`, a successful
half-open resource probe may create a linked run for unexpired circuit-blocked
nodes and explicitly repeat-safe failed nodes. This applies equally to ordinary
runs and backfill child runs because circuit identity is workspace plus resource
kind and name. The source run or child remains terminal and immutable; inspect
the linked recovery run separately. Pipelines without that declaration remain
manual recovery.

Expected result: the orchestrator creates new persisted run records linked to the
source run or backfill group.

`max_attempts` includes the first attempt and defaults to one. Operator
`retry_policy` overrides asset `retry`, which overrides pipeline `retry`.
Policy changes timing/count only; an unknown write, transaction,
materialization, or external side effect remains terminal. `mix favn.run`
accepts `--retry-max-attempts` and fixed `--retry-backoff-ms`; the HTTP/operator
contract accepts the complete typed `retry_policy`, including exponential
backoff. The rerun API accepts `input_mode: fresh|inherit|pinned`.

Read `apps/favn/guides/retries-and-replay.md` for the canonical mechanism table,
schedule timeline, restart recovery, and safe ingestion recipes.

## Operate Schedules

Newly published schedules are inactive in every workspace. Use the supported
operator boundary rather than changing persisted state directly:

```text
mix favn.schedules list
mix favn.schedules show SCHEDULE_ID
mix favn.schedules preview SCHEDULE_ID --limit 5
mix favn.schedules activate SCHEDULE_ID --reason "reviewed"
mix favn.schedules deactivate SCHEDULE_ID --reason "maintenance"
mix favn.schedules activate SCHEDULE_ID --reason "reviewed" --idempotency-key CHANGE_ID
```

1. List schedules from the active manifest through operator tooling.
2. Check activation state, runtime state, next due time, last submitted due time,
   and scheduler errors.
3. Preview upcoming occurrences before enabling a schedule.
4. Enable schedules that should submit future work.
5. Disable schedules that should stop future submissions.
6. After changing a schedule, re-read the schedule entry and diagnostics.

Activation and deactivation return an immutable command receipt: the original
previous and effective states, activation version, approved fingerprint,
decision time, and resulting next due time. Repeating the same idempotency key
and command content returns that exact receipt even after later schedule
changes. Use `mix favn.schedules show` for current live state.

Schedule overlap is not execution retry. `:allow` admits an independent run
with independent pins, `:forbid` admits none while the tracked run is active,
and `:queue_one` remembers one occurrence until it can be admitted. A run
waiting in the durable submission queue, admission, or node backoff is still
active for these rules. `missed: :skip | :one | :all` controls catch-up
occurrences after delayed evaluation, not attempts in the existing run.

Expected result: enabled schedules submit due work through the same orchestrator
run path as manual runs. Disabled schedules do not submit future work.

Important rules:

- Enabling starts from the next due occurrence observed at command time. It does
  not automatically submit missed catch-up work.
- Disabling does not cancel already accepted runs.
- Schedule state is persisted by the orchestrator storage boundary.
- Multiple orchestrator nodes may run scheduler workers; durable PostgreSQL
  claims and fencing decide ownership. Process registration is not authority.
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

Production deployment commands are documented in
`docs/production/control_plane_image.md` and
`docs/production/postgresql_operator_runbook.md`.

## Boundaries To Preserve

- Do not edit PostgreSQL control-plane rows by hand.
- Do not ask the runner to own schedules, active manifest selection, or persisted
  run state.
- Do not infer lifecycle truth from UI state.
- Do not let `favn_view` call storage, scheduler internals, runner internals,
  repositories, compiler internals, plugins, or adapters directly.
- Do not treat DuckDB/DuckLake data-plane recovery as part of PostgreSQL
  control-plane recovery; reconcile their recovery points explicitly.
- Do not bypass scheduler claims or fencing with node-local locks.
