# Issue 763 investigation scratchpad — 2026-09-24

## Scope and evidence rules

Re-evaluate initial-registration recovery and inspect surrounding production risks,
especially orchestrator work amplification, storage pools, task supervision and
ownership. Produce a reviewed implementation plan; do not edit application code.
Confirmed code defects, local runtime experiments, incident observations and
unproven performance hypotheses are recorded separately.

## Baseline

- GitHub issue 763 and related issue 762 were reread; both are open with no comments.
- Current origin/main is `430a891d` (v0.5.0-rc.19 release merge).
- Planning checkout: isolated worktree based on origin/main.
- Tidewave: running umbrella server on localhost:4173. Its original source checkout
  includes issue 764 backfill-command changes (`95e132f4`); relevant recovery/storage
  sources must be checked for equivalence to main before attributing experiments.
- PostgreSQL is the existing disposable Favn Docker instance under OrbStack (5433).
  No native database will be created. No production environment is connected.

## Questions to resolve

1. Is failure inevitable only because of the 30-second budget, or because the
   ownership/domain model treats post-write registration as disposable work?
2. Which facts authorize replay of reads, marker initialization, and activation?
3. How can already terminal affected runs be repaired without restarting writes?
4. Does the coordinator pay repeated full-snapshot, manifest, task or projection
   costs for each asset/marker transition?
5. Which loops multiply pressure during overload, and which failures crash parents?
6. What is the smallest coherent architecture that removes paths instead of adding
   another independent retry/repair protocol?
7. What evidence would qualify a 0.5-vCPU orchestrator with three runners?

## Initial source-confirmed facts to recheck

- Registration has 30 seconds and eight slots, not one hard-coded attempt.
- Retry intent is persisted before dispatch; persistence can consume the window.
- Exhaustion fails the run and starts read-only cleanup; cleanup cannot create a
  missing marker. Recovery currently requires an existing marker.
- Existing safeguards correctly block new materialization on unresolved targets.
- Run ownership, external operation authority and run terminality are distinct.

## Experiment log

- Tidewave `tools/list`: successful, tools include project_eval, get_source_location,
  get_docs and get_logs.

## Confirmed observations (source plus Tidewave)

1. **Terminal dead end (P1):** `RegistrationRetry.next/5` saved slot 1, then returned
   exhaustion at +31 seconds. `Execution.resume_registration/5` emits scheduled
   retries before checking elapsed time. `RunServer` treats this specific reason
   as failure, stops sibling continuations and invokes `FailureCleanup.fail/2`.
   Cleanup only reads markers. Missing marker => unresolved target. Raising this
   budget alone does not fix the separate 30-second `PersistenceRetry.resolve/1`
   failure route or historical terminal runs.
2. **Checkout crash (P1, issue 762):** in an isolated one-connection Ecto pool using
   the existing Docker database URL, with its connection checked out, the actual
   Sequencer callback raised `DBConnection.ConnectionError`. The Projector under
   the same conditions returned retryable `Persistence.Error(kind: :unavailable)`.
   Temporary pool stopped afterward. `Operation.run/4` re-raises after telemetry;
   using it alone does not fix this. Backend `rest_for_one` couples the sequencer
   to projector/listener/maintenance restarts. Never change ordering where the
   Repo or authentication provider really is a prerequisite.
3. **Aggressive subscription retry (P2):** three temporary subscriptions to missing
   local tasks caused 111 store reads in 2 seconds. Source retries every failed
   subscription after 50 ms, including unavailable, overloaded and permanent
   not-found results; active checks are limited to 32, retry rate is not. All
   subscriptions and telemetry hooks removed. This proves a mechanism, not that
   production had missing tasks or this exact request rate.
4. **Heavy poll reads (P2):** router polls active waiters every second via full
   RunnerTasks.get. Each read opens repeatable-read transaction, checks retention,
   fetches retained manifest/package, verifies payload/context hashes and decodes
   payload and context even while task is still running. Multiple subscribers
   have separate checks. OperationRunnerTasks' 250-ms loop only polls rebuild
   cancellation (no DB call for initial registration); do not misattribute it.
5. **Manifest-sized decoding (measured):** 100 successful in-memory decode calls
   for synthetic valid manifests. Capability payload fixed at 1,076 bytes.

   | Assets | Capability decode us / reductions | Empty context us / reductions |
   | ---: | ---: | ---: |
   | 1 | 112.99 / 16,552 | 98.65 / 11,811 |
   | 35 | 187.13 / 41,066 | 175.39 / 36,357 |
   | 350 | 795.89 / 265,343 | 790.68 / 260,818 |

   `PersistenceData.atom_dictionary/3` reconstructs the fixed struct-field map
   and recursively traverses the whole version on every decode. ManifestCache
   avoids repeated deserialization, but does not eliminate this traversal or
   copying cached version terms. Package verification is additional work.
6. **Idle polling (measured):** empty local dev DB (0 runs/tasks/manifests) produced
   318 Repo query events in 10 seconds, including BEGIN/COMMIT, 61 database-clock
   reads and 55 transactions. Submission/rebuild/backfill/deployment dispatchers
   were the leading application processes by reductions. Dev SQL logging and 15
   online schedulers differ from a production 0.5-vCPU quota: no production CPU
   percentage or runner-capacity conclusion follows from these numbers.
7. **Timeout option mismatch (source confirmed):** registration worker supplies
   `timeout_ms`, but OperationRunnerTasks.await reads `timeout`. The external
   deadline timer still bounds retry workers; this is inconsistent propagation,
   not proof of an unbounded retry. The initial pass defaults to 300 seconds.

   Runtime confirmation: `await(context, missing_id, timeout: 0)` rejects an invalid
   timeout without storage; the same call with `timeout_ms: 0` ignores the option
   and returns storage's `:not_found`. A saved event with a 120-second deadline
   also fails the existing retry decoder, confirming a policy-format transition
   is required rather than changing one constant.

8. **Cleanup verification:** after all probes, the router had zero checks and
   zero waiter keys, the temporary pool was absent, and runs/tasks/manifests were
   still all zero. Tidewave's non-probe error log was empty. These local probes
   did not create persisted workload fixtures or change application source.

## Additional risks / not yet reproduced as incidents

- Initial binding reconciliation locks binding/generation and checks exact
  successful materialization/marker, but its command has no current run/target
  authority field. Unlike target recovery activation it cannot check a current
  operation fence. The new lifecycle must close this ownership gap; do not claim
  observed corruption or call evidence-based idempotent adoption inherently unsafe.
- Projector handles connection failures; maintenance uses supervised async work.
  They do not share the sequencer's exact unchecked exception bug.
- Projector has one global cursor. A poison event rolls back its whole batch;
  failure is recorded but the cursor cannot pass it. A combined-window
  materialization expands all logical windows and upserts individually inside
  that transaction. Potential head-of-line latency and cost need a scale test.
- Several dispatch/recovery loops collect all workspaces and process each
  synchronously, despite each page being bounded. Cost can grow across tenants.
- Run snapshots reference an immutable plan and the manifest cache is bounded;
  previous optimizations are present. Avoid claiming the full plan/SQL is always
  rewritten. Mutable snapshot hashing/encoding and process copying still need a
  workload profile before larger changes.

## Architecture options evaluated

- Increase timers: insufficient; there are multiple exhaustion routes, lost
  authority must still stop a worker, and historical runs remain terminal.
- Keep all registration inside RunServer, add another repair retry: smaller
  first diff but duplicates the same lifecycle across execution, cleanup and
  operator recovery. This is the coupling that caused the dead end.
- Put first marker in the original materialization transaction: attractive
  eventual simplification for qualified SQL adapters. Existing runtime catalog
  already records transactional publication receipts. However these receipts
  identify a relation by name, not a bound physical instance, so they cannot
  prove a historical table was never dropped/recreated. This option changes
  runner/adapter transaction protocols and still needs historical repair; do not
  make the urgent fix depend on that larger change.
- **Preferred:** one durable target-owned initial-registration state machine,
  shared by automatic completion and reviewed operator repair, reusing existing
  task queue and target-operation locks. Atomically hand off when successful
  materialization is recorded. Runs observe completion, not own marker mutation.
  Remove run-local registration timers/retry events as an execution mechanism;
  retain a bounded compatibility reader for existing rc19 evidence.

## Historical repair evidence boundary

Marker absence is not proof a previous writer stopped. Lease expiry alone is
also insufficient. Drain/reconcile exact original task and target write holds.
Never repeat the asset write. Matching bound marker permits evidence-based
completion; safe/not-started marker operations can use their original identity.
Without a pre-existing physical instance identity, a fingerprint or runtime
publication receipt cannot prove historical continuity. A supported missing-marker
repair therefore needs a narrowly scoped, audited administrator attestation about
that exact retained target, in addition to technical fencing/contract checks. It
must never present that attestation as machine-verified continuity or waive an
unresolved writer. Conflicting marker/identity remains blocked. Independent review
identified the remaining time-of-check gap: a same-schema unbound replacement
after approval is also undetectable. Historical adoption therefore requires an
administrator-controlled freeze on external writes/DDL from approval through
known marker commit (or definitive no-effect resolution); marker-free completion
requires the freeze through activation. This is a trusted operational precondition,
not a new claim that fingerprint comparison proves continuity.

## Experiment artifacts

`runtime_baseline.exs`, `idle_queries.exs`, `checkout_failure.exs`, `codec_cost.exs`,
`router_retries.exs`, `timeout_and_checks.exs` and matching .txt outputs were
kept locally under `/private/tmp/favn-763-audit` during the investigation. They
are temporary artifacts, not committed test fixtures; this document preserves
the sanitized results and reproduction methods. Implementation must turn the
relevant failure probes into permanent owning-layer regression tests.
The initial temporary-pool experiments used Repo.config(), which omitted the
runtime-supplied connection URL and failed during setup. Discarded as evidence.
The final successful probe used the same FAVN_DATABASE_URL as the server.

## Source index at the audited baseline

Links are pinned to `430a891d`; current code remains authoritative after this audit.

| Concern | Primary source |
| --- | --- |
| Durable retry policy and decoder | [RegistrationRetry](https://github.com/eirhop/favn/blob/430a891d/apps/favn_orchestrator/lib/favn_orchestrator/run_server/execution/registration_retry.ex#L10) |
| Retry intent, timer and timeout propagation | [Execution](https://github.com/eirhop/favn/blob/430a891d/apps/favn_orchestrator/lib/favn_orchestrator/run_server/execution.ex#L2044) |
| Exhaustion fails the run | [RunServer](https://github.com/eirhop/favn/blob/430a891d/apps/favn_orchestrator/lib/favn_orchestrator/run_server.ex#L792) |
| Independent persistence exhaustion path | [PersistenceRetry](https://github.com/eirhop/favn/blob/430a891d/apps/favn_orchestrator/lib/favn_orchestrator/run_server/persistence_retry.ex#L98) |
| Missing marker in cleanup | [InitialTargetGenerationReconciler](https://github.com/eirhop/favn/blob/430a891d/apps/favn_orchestrator/lib/favn_orchestrator/initial_target_generation_reconciler.ex#L231) |
| Target write protection | [TargetGenerations.Store](https://github.com/eirhop/favn/blob/430a891d/apps/favn_storage_postgres/lib/favn_storage_postgres/target_generations/store.ex#L317) |
| Sequencer callback | [Sequencer](https://github.com/eirhop/favn/blob/430a891d/apps/favn_storage_postgres/lib/favn_storage_postgres/outbox/sequencer.ex#L147) |
| Exception instrumentation re-raises | [Operation](https://github.com/eirhop/favn/blob/430a891d/apps/favn_storage_postgres/lib/favn_storage_postgres/operation.ex#L30) |
| Subscription retry and full-detail polling | [RunnerTaskResultRouter](https://github.com/eirhop/favn/blob/430a891d/apps/favn_orchestrator/lib/favn_orchestrator/runner_task_result_router.ex#L103) |
| Task hydration | [RunnerTasks.Store](https://github.com/eirhop/favn/blob/430a891d/apps/favn_storage_postgres/lib/favn_storage_postgres/runner_tasks/store.ex#L3358) |
| Repeated decoder inventory traversal | [PersistenceData](https://github.com/eirhop/favn/blob/430a891d/apps/favn_core/lib/favn/contracts/runner_task/persistence_data.ex#L668) |
| Instance identity installed during marker creation | [GenerationTransaction](https://github.com/eirhop/favn/blob/430a891d/apps/favn_sql_runtime/lib/favn/sql/generation_transaction.ex#L679) |
| Existing foreign instance fails closed | [DuckDB ADBC](https://github.com/eirhop/favn/blob/430a891d/apps/favn_duckdb_adbc/lib/favn/sql/adapter/duckdb/adbc.ex#L930) |
| Runtime publication stores a named relation | [RuntimeCatalog](https://github.com/eirhop/favn/blob/430a891d/apps/favn_duckdb_adbc/lib/favn/sql/adapter/duckdb/adbc/runtime_catalog.ex#L55) |
| Projection window expansion | [Projector](https://github.com/eirhop/favn/blob/430a891d/apps/favn_storage_postgres/lib/favn_storage_postgres/projections/projector.ex#L764) |

## Reproduction notes

Run probes through the umbrella server's Tidewave `project_eval`; do not use
production or change the application's configured backend. The manifest benchmark
uses the existing `FavnTestSupport.RunnerTaskPersistence.version/2` fixture, clones
1/35/350 assets with unique refs, applies `with_manifest_contract` and
`with_manifest_graph`, then calls `Version.new/1`. Encode one
`GenerationCapabilitiesRequest` and an empty `RunnerTaskContext`, perform 100
successful decodes of each, and measure process reductions and `:timer.tc`.
Payload bytes stay constant while unrelated manifest assets grow.

The checkout probe starts a separately named one-connection Repo using the running
development URL. Inside that Repo's `checkout/2`, a child sets the same dynamic
Repo and invokes the actual Sequencer callback and Projector batch. The child
observes checkout failure; an `after` block restores the previous dynamic Repo
and stops the temporary pool. This does not exhaust the application's real pool.

The router probe attaches a temporary counter to the existing persistence-operation
telemetry and starts three `RunnerTaskResultRouter.await/3` subscribers for known
nonexistent task IDs. After two seconds it stops those subscribers and detaches
the counter. The idle measurement separately counts Repo query events for ten
seconds, including BEGIN/COMMIT; it is not a count of domain commands.

## Verification limits and follow-up gates

No production access, customer data, database outage, deployment action or benchmark
under a 0.5-vCPU cgroup occurred. The local VM had 15 online schedulers, dev logging,
and no active runners. Timings are local microbenchmarks; reduction counts identify
algorithmic repeated work, not a throughput guarantee. No application fix or
schema change was made during this investigation.

Before release: reproduce the incident-shaped 35-target/70-attempt workload, both
before and after durable handoff; inject a 60–120-second outage and lost replies;
exercise cancelled/unknown marker cases; qualify three runners with declared
concurrency under a 0.5-vCPU/1-GiB limit. Keep broad projection/sharding or generic
workflow changes out of this fix unless that profile proves they are necessary.

## Independent review corrections

The reviewer independently read both issues, baseline source and probe outputs,
then requested five corrections before approval:

- Preserve the reconciler's valid unsupported-marker branch. A failed or malformed
  capability read must never become marker-free success.
- Specify renewable registration authority and separate its durable ID from the
  immutable marker mutation identity. The existing write barrier compares
  `lock.operation_id` with `task.write_operation_id` and otherwise creates a
  one-hour task-owned marker lock. New registration tasks must use explicit typed
  authority and fail closed instead of that fallback. Cancellation before handoff
  must be as effective as cancellation after it.
- Add the external-change freeze to historical attestation, without promising
  detection of indistinguishable unbound replacement.
- Retain existing recovery rows as approval/audit history linked to the single
  registration. Atomic approval acceptance and derived completion replace the
  current second execution path. A per-generation row remains justified because
  several immutable administrator plans may refer to the same generation.
- Declare offered task rates and p95 latency/drain limits in the 0.5-vCPU gate;
  CPU averages alone can reward throttling or idle time.

These corrections change the proposed design and qualification, not the observed
incident evidence. No application implementation was performed during review.
