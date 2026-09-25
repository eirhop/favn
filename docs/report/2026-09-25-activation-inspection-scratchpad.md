# Activation inspection investigation — 2026-09-25

Point-in-time investigation notes for the activation blocker recorded in
[runner readiness qualification](2026-09-25-runner-readiness.md). This is evidence,
not an implementation plan or a claim that the root cause has been established.

## Baseline

Local `main` was fast-forwarded to `887e3b81`, which includes PRs #766 and #767.
Investigation branch: `codex/activation-physical-inspection`.
The separate root checkout was left unchanged.

The retained OrbStack environment still uses the older control-plane image
recorded in the qualification report. On resuming, its five runners were stopped
and its control-plane container was unhealthy. No containers, volumes, deployment
bindings, or workload data were replaced or reset during this inspection.

The umbrella development server was restarted from this implementation worktree.
Tidewave listens on port 4173 and uses the existing development PostgreSQL
container on port 5433, separate from the retained stress database.

## Evidence

- Read the latest 80 durable relation-inspection tasks from the stress database.
  All had succeeded status and no stored task error. Their inspection results had
  no warnings or result errors.
- The most recent 68 belong to the replacement runner release. The other 12 belong
  to the preceding release and must not be mistaken for replacement-release
  identity failures.
- Saved stress-target results include actual relation identities and columns;
  this was not merely a set of successful inspections of missing example tables.
- Using Tidewave on merged main, decoded the actual pinned manifest and supplied
  it to the production task codec. All 68 replacement-release results decoded,
  matched the required release, and produced a valid physical fingerprint (or an
  authoritative missing-relation result where applicable).
- All 68 request payloads decoded with that manifest, and every request/result
  pair passed `PersistenceSchema.completion/4`.
- A preliminary decode without the manifest failed. That is not evidence of a
  production codec bug: task decoding requires the validated manifest context.
- Existing control-plane logs confirm the two activation responses with 34 and 33
  unavailable inspections. They do not preserve the specific underlying error.

## Code paths and remaining uncertainty

`TargetCompatibilityPlanner` maps errors from relation selection and physical
inspection to the same `physical_inspection_unavailable` decision. It also maps
classifier exits to that decision after reconciling their tasks. Inspection
covers durable task creation/retrieval, waiting, result identity verification,
and fingerprinting, so a succeeded task does not prove that classification used
its result successfully.

`OperationRunnerTasks` can return task-data retrieval errors or waiter failures.
The storage read additionally validates manifest pins, orchestration context,
authorization, hashes, and task identity beyond the request/result checks above.
These paths and failures during live result delivery remain untested here.

No evidence currently justifies increasing inspection timeouts, weakening
identity checks, accepting unavailable inspections, or introducing a repair path.

## Next diagnostic experiment

1. Retain the current evidence and establish an aligned merged-main control-plane
   and runner baseline in OrbStack, preserving the stress database and data plane.
2. Reproduce activation with five runners and the existing 0.25-vCPU limit while
   capturing the precise relation-selection, task retrieval, waiter, or classifier
   failure before it is collapsed into the generic reason.
3. Turn the confirmed failure into a focused deterministic regression test, then
   select the smallest owning-layer fix and rerun activation plus the 35-asset
   workload. Defer the 100-asset and latency expansion until this blocker clears.
4. Assess change-record scope once the fix is known. The user's threshold requires
   a record for changes exceeding 300 code lines; small focused bug fixes are
   exempt. No implementation change or change record has been created yet.

## Root-cause investigation and controlled experiment

Further investigation on the same day established the failure path. The earlier
uncertainty above is retained as investigation history.

- Read an affected task through the retained orchestrator's actual
  `OperationRunnerTasks.fetch/2`: succeeded, data available, valid result. This
  rules out a permanently unreadable result for that sampled task.
- Repeated activation with all five existing healthy runners and 0.25 vCPU.
  A temporary return trace captured `Persistence.Error` with kind `unavailable`
  from `OperationRunnerTasks.await/3` and physical inspection. Revision 4 left
  24 unresolved inspections.
- A second bounded trace captured the underlying exception and the error return
  in the **same classifier PID**: `DBConnection.ConnectionError` with reason
  `queue_timeout` became retryable persistence unavailable, then an unavailable
  inspection decision. Revision 5 left 23 unresolved inspections.
- The configured database pool has 15 connections, queue target 50 ms and queue
  interval 1,000 ms. The global inspection admission limit was 32.
- Fifteen database samples during activation showed up to 15 sessions idle inside
  transactions waiting for the client. Several samples followed the manifest
  retirement check. One demand-row transaction lock wait was observed. This is
  evidence of connections held while the throttled control plane continues work,
  not proof that the database engine or that single lock is the dominant bottleneck.
- Temporarily changed only the existing in-memory admission limit from 32 to 4.
  Revision 6 completed with **zero unresolved inspections**. The database showed
  all 35 stress targets ready and 12 unused example targets uninitialized.
  The control image, five runner images, CPU quota, manifests, and volumes stayed
  unchanged. This one experiment proves pressure sensitivity, not full qualification
  of a concurrency default or a complete fix for transient reads.
- Restored the admission limit to 32 and removed temporary call tracing. No source
  implementation was edited. The successful activation's binding classifications
  remain persisted; earlier task and command evidence remains available.

The affected planner, operation-task, result-router, and runner-task-store files
have no diff between retained control commit `84248a5c74f4` and current main
`887e3b81`. Live evidence is from the retained image; an aligned main-image rerun
remains required during implementation qualification.

### Verified causal chain

Concurrent classification repeatedly reads and hydrates durable tasks. Read
transactions hold database connections while the control plane validates pinned
manifests, payloads, contexts, and results. Under the low CPU quota the pool's
adaptive queue sheds requests. An initial or follow-up task read returns an
explicitly retryable error. The activation planner immediately persists a generic
unavailable decision instead of continuing safe observation within its original
budget. Consequently a succeeded inspection can coexist with a blocked binding.

The root correctness defect is treating a transient observation failure as a
finished compatibility result. The high fan-out and repeated reads amplify it.
No evidence supports weakening inspection, generation, or release checks.

### Related defects confirmed by source review

1. Linked `Task.async_stream` classifiers can terminate the planner before its
   `{:exit, reason}` reconciliation branch runs. A Tidewave probe with the current
   runtime reproduced the caller exiting rather than receiving stream results.
2. Timeout reconciliation returns a timeout even when a fresh read finds that the
   inspection already succeeded. The new plan must define deadline and terminal
   evidence semantics explicitly.
3. Release selection compares whole manifest release maps and ignores the desired
   asset's runner pool. A target moved between existing pools can select the old
   pool despite an unchanged map. Compare effective per-asset bindings instead.
4. The planner drops failure stages and causes. Safe bounded diagnostics should
   preserve these without storing arbitrary errors or data.
5. The CLI discards unresolved-inspection diagnostics from activation responses.
   Its `reconciled` flag means uncertain HTTP outcome reconciliation, **not** that
   physical inspection succeeded. The four-worker success still prints false.
   Do not use that flag as the inspection verdict.

These findings and the resulting change record were independently reviewed by
Astra xhigh. The final plan was approved after tightening read/admission bounds,
permanent-error delivery, replay semantics, CLI unknown status, and the explicit
planner-error path for unconfirmed cleanup. No root
cause was established for a separate waiter-start timeout; that remains a source
risk, not an additional claimed production failure.

Local evidence: `/tmp/favn-activation-trace.log`,
`/tmp/favn-activation-trace-detail.log`, `/tmp/favn-activation-db-samples.json`,
`/tmp/favn-activation-reproduction.log`,
`/tmp/favn-activation-reproduction-detail.log`, and
`/tmp/favn-activation-reproduction-limit4.log`. These temporary files contain
local qualification evidence and are not durable repository artifacts.
