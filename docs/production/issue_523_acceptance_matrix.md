# PostgreSQL production qualification

This matrix separates evidence that can be produced by Favn's local
production-shaped Docker deployment from evidence that must come from the
managed data platform where Favn will actually run.

The local qualification extends the elastic-runner drill from PR #565. It is a
release gate for the deployment artifacts and PostgreSQL behavior, but it does
not by itself close issue #523 or justify a managed-production claim.

## Acceptance matrix

| ID | Claim | Automated evidence | Pass condition | Environment |
| --- | --- | --- | --- | --- |
| L1 | Production images start with PostgreSQL 18, verified TLS, separate migrator/runtime roles, and no runners. | Existing Compose bootstrap and schema verification logs. | PostgreSQL and the control plane become healthy; migrations, grants, workspace provisioning, and schema verification exit zero. | Local, this PR |
| L2 | The immutable manifest and runner release work before load starts. | Existing PR #565 publication, activation, and `0 -> 3 -> 2 -> 1 -> 0` smoke evidence. | Publication, activation, three probe runs, exact scale sequence, and final drain all pass. | Local, this PR |
| L3 | API commands are authenticated and idempotent. | `api-requests.jsonl` and the phase event log. | Replaying an identical run request returns the same run ID; reusing its key for different content returns `409 idempotency_conflict`; no mutation is blindly retried with a new key. | Local, this PR |
| L4 | Sustained API load creates durable work and elastic runners keep pace. | Submitted-run ledger, API latency samples, queue samples, scaler timeline, Docker statistics, PostgreSQL statistics, and `performance-summary.json`. | No unresolved API submission outcomes; API p95 is at most 250 ms; queue age is at most 5 seconds; queue depth is at most 48; at least four sustained submissions per minute are accepted; at least 80% of samples scheduled outside measured blocking fault-handling windows are present; accepted submissions become durable and terminal; unaffected runs end `ok`. Fault recovery remains covered by its event and recovery assertions. | Local, this PR |
| L5 | Runners scale up, churn, replace a killed runner, and return to zero. | Fault events, `runner-fault.json`, scaler timeline, runner inspect records, and final capacity response. | The harness submits a slow probe and kills its sole runner only after PostgreSQL confirms that exact run is active; positive demand is recorded after the fault and a distinct post-fault runner container has a PostgreSQL task assignment within 120 seconds; unexpected non-zero runner exits fail the qualification; final partition state is drained with zero registered runners. | Local, this PR |
| L6 | A control-plane crash does not lose accepted work or unsafely replay writes. | Before/after samples, crash event, container health transition, run/submission status counts, per-run outcomes, and control-plane logs. | The same container restarts healthy; accepted work drains; no run remains pending/running. An in-flight, already-completed write may end with the explicit `non_reusable_materialization_claim_succeeded` recovery error instead of being replayed. | Local, this PR |
| L7 | A PostgreSQL crash recovers without corrupting durable control-plane state. | Before/after database statistics, crash event, health transitions, schema verification, and final database counts. | PostgreSQL crash recovery completes; the control plane becomes ready again; schema verification passes; no qualification run or runner task remains non-terminal. | Local, this PR |
| L8 | Operators can diagnose load and recovery after the test. | `run.json`, `events.jsonl`, `api-requests.jsonl`, `samples.jsonl`, `database-samples.jsonl`, Docker statistics, component logs, and `final-validation.json`. | Evidence is timestamped, identifies the source revision and configuration, scans every generated credential value for leakage, and is sufficient to reproduce every final assertion. | Local, this PR |
| L9 | The system reaches a clean terminal state. | Final API capacity/submission/in-flight responses and read-only PostgreSQL status aggregates. | Queue, in-flight runs, outstanding runner tasks, active runs, pending operations, durable blockers, and registered runners are all zero; outbox and projection lag are zero; every run and task is terminal. At most one runner task is `unknown`, corresponding to the deliberate runner kill, and at most one safely classified run error is allowed per injected fault. | Local, this PR |
| M1 | Managed PostgreSQL backup, restore, point-in-time recovery, failover, alerting, and provider limits are production-ready. | Provider-native backup/restore records, monitoring history, incident drills, and operating notes. | The work data platform meets its chosen RPO/RTO and alert thresholds under real traffic. | Deferred to workplace test deployment |
| M2 | Favn remains healthy under representative production traffic for weeks. | Multi-week service, database, queue, runner, and alert history from the work test platform. | No unexplained data loss, stuck durable state, tenant leak, alert gap, or unbounded resource trend. | Deferred to workplace test deployment |

Rows M1 and M2 are deliberately not executed or claimed by this PR.

## Automated local test plan

Run the qualification from `deployment/docker-compose`:

```sh
sh ./run-qualification.sh
```

The default sustained-load duration is four hours. It can be shortened only
for harness development:

```sh
FAVN_QUALIFICATION_DURATION_SECONDS=600 sh ./run-qualification.sh
```

The wrapper first runs the complete PR #565 smoke drill. It then starts the
qualification controller in the same image used by the local KEDA-like scaler
and returns immediately with a run ID and controller container ID.

The detached controller executes these phases:

1. Record the exact revision, image/release IDs, load settings, and start time.
2. Resolve the activated fast probe target through the orchestrator API.
3. Prove idempotent replay and conflict handling, then create an initial backlog
   while runner capacity is zero.
4. Start the bounded local scaler and sustain authenticated API submissions.
5. Kill one busy runner and require bounded replacement.
6. Crash and recover the control plane while durable work exists.
7. Crash and recover PostgreSQL while durable work exists.
8. Stop submissions, drain all accepted work and runners, re-verify the schema,
   collect logs/statistics, scan evidence for configured secrets, and write the
   machine-readable final verdict.

The three fault phases occur at approximately 25%, 50%, and 75% of the
configured load duration. A mutation with an uncertain network result is
resolved only by replaying the exact payload with the exact same idempotency
key.

The default local performance budgets are intentionally explicit:

- API p95 latency: 250 ms;
- maximum queued age: 5 seconds;
- maximum queued depth: 48;
- minimum sustained submission rate: four accepted runs per minute;
- minimum sample coverage: 80%; and
- busy-runner replacement: 120 seconds.

Each budget has a matching `FAVN_QUALIFICATION_*` environment override in
`compose.yml`. A non-default budget is recorded in `run.json`; changing a
budget changes the claim being tested and must be justified in the result
report.

## Evidence contract

Each run writes an ignored directory at
`deployment/docker-compose/qualification-results/<run-id>/`:

| File | Meaning |
| --- | --- |
| `run.json` | Immutable test identity and configuration, without credentials. |
| `events.jsonl` | Phase changes, injected faults, health recovery, drain, and verdict. |
| `api-requests.jsonl` | Operation, path, response status, request ID, latency, and resolution outcome. Payloads, bearer tokens, and raw idempotency keys are excluded. |
| `submitted-runs.jsonl` | One record for every uniquely accepted run. |
| `samples.jsonl` | Readiness, submission queue, in-flight runs, runner capacity, and live container counts. |
| `database-samples.jsonl` | Read-only PostgreSQL throughput, connection, lock, outbox, projection, run, submission, and runner-task aggregates. |
| `workload-outcomes.json` | Per-run and per-task terminal status plus a bounded, redacted recovery-failure classification. |
| `performance-summary.json` | Measured API p95, queue pressure, sustained submission rate, and sample completeness against recorded budgets. |
| `runner-fault.json` | The PostgreSQL-confirmed busy runner, killed container, distinct replacement, claimed work, live demand, and replacement time. |
| `docker-stats.jsonl` | Container CPU, memory, network, block-I/O, and process snapshots. |
| `control-plane.log`, `postgres.log`, `runner.log` | Timestamped component logs retained for diagnosis. |
| `container-states.jsonl` | Sanitized terminal container state and exit metadata; container environments are excluded. |
| `final-validation.json` | Every automated assertion and the overall pass/fail verdict. |
| `controller.log` | Detached controller stdout/stderr, captured by `qualification-status.sh`. |

Check progress or collect the terminal report with:

```sh
sh ./qualification-status.sh
```

Do not commit the result directory. Retain it as test evidence and attach only
an explicitly reviewed, redacted summary to a PR or issue.

## Interpretation

A passing local run means the shipped Favn images, API, durable queue, runner
lifecycle, PostgreSQL crash recovery, and evidence tooling survived the tested
load on one Docker host. It does not prove managed failover, PITR, cloud
networking, provider monitoring, or multi-week operational stability.

The qualification deliberately preserves Favn's unknown-outcome contract.
Killing a runner may leave that task `unknown`, and interrupting the control
plane or PostgreSQL after a runner-side write has completed may produce
`non_reusable_materialization_claim_succeeded`. These terminal errors are safer
than replaying a write that may already have happened. Any other failure reason,
an unbounded number of such errors, or non-terminal work fails the test.
