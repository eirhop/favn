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


## Manual OrbStack qualification (in progress)

The user requested a local simulation, not a CI simulation: a 0.5-vCPU
orchestrator and five runner images, network fault injection, then the same test
after fixes. The independently reviewed qualification adjustment preserves
focused regression tests, durable fault triggers, before/after evidence and a
retained failing database for forward-repair testing. Four slots per runner in
the original plan is invalid: the current RunnerTask contract requires one slot.
The local qualification uses five distinct runner sessions and five admitted
independent targets instead.

The isolated Compose project is `favn-763-local`, using only OrbStack containers.
Its PostgreSQL is separate from the existing development and user databases.
The baseline control-plane image is published rc19, digest
`sha256:641d01af54cc11264b5a16e459d460ba48f2ac9b80709b81e935a4e7399a2beb`.
The generated CRM tutorial profile adds 35 independent SQL targets, a pool and
pipeline concurrency of five, and shared DuckLake metadata/data. A Toxiproxy
endpoint carries only the orchestrator's control-database connections; bootstrap,
evidence reads and DuckLake metadata use PostgreSQL directly. Directional
latency/jitter and measured round-trip overhead must be recorded separately.

Initial startup findings:

- The amd64 Erlang JIT failed under this ARM Mac's compatibility layer with
  `prim_tty:isatty/1`, `erlang:nif_error/1` and `nouser`. The OTP-maintainer
  workaround `+JMsingle true` allowed the unchanged release bootstrap to complete.
  This local setting is in the harness only. It means CPU measurements include
  emulation and cannot establish native production capacity. See
  [OTP issue 10355](https://github.com/erlang/otp/issues/10355).
- Docker's kernel cgroup reports `cpu.max = 50000 100000` for the orchestrator.
- **Additional confirmed production startup bug:** a valid resident pool is
  accepted by `RunnerPools.normalize/1`, producing `idle_grace_ms: :infinity`.
  `ProductionRuntimeConfig` installs that normalized value; `RuntimeConfig`
  normalizes it again and rejects the generated infinity as user-supplied grace.
  The actual rc19 container aborts with `resident_idle_grace_not_allowed`.
  A side-effect-free Tidewave call reproduced normalize(normalize(input)).
  Fix normalization idempotence while continuing to reject finite resident grace;
  add a production-config-through-runtime regression test. The unchanged baseline
  instead uses five fixed elastic runners with a one-hour idle grace and no
  autoscaler; candidate comparisons must retain that configuration.

No application fix or end-to-end workload result is claimed by these startup
checks. Image build corrections (explicit amd64 build platform and the supported
`query do ~SQL"..." end` fixture syntax) are harness setup corrections.


Additional setup evidence, before the first workload:

- `favn.build.manifest` reached public catalog export and crashed at
  `Favn.Catalog.Artifact.selector/1` on the tutorial's documented shorthand
  `assets([Engagement, ExecutiveOverview])`: `Snapshot.ref/1` only accepts tuples.
  The local profile selects only its stress pipeline and uses explicit
  `{Module, :asset}` refs to keep the release baseline unchanged. This is a
  separate authoring/export regression, not evidence for issue #763's cause.
- All five runner sessions registered durably. Their canonical image RPC health
  probes nevertheless fail with `:noconnection`. The release uses a dynamic
  `undefined@host` node; local EPMD reports no registered static name. Investigate
  the health probe's incompatibility with dynamic naming before production use;
  do not count a registered session as a passing image health check. Leave probes
  enabled equally in baseline/candidate resource measurements.
- The existing Compose operator helper captures `mix run` stdout as the manifest
  ID. A native dependency's compile output contaminated the capture and caused
  `invalid --manifest-version`. An explicit known ID activated successfully.
  The local harness reads the single publication directory name instead.
- The first shared-storage profile inherited the tutorial's `database_path`
  script parameter because Config merges nested keyword lists. Activation's
  relation-inspection tasks correctly failed with `unused_script_parameters`.
  Use an isolated generated compile profile rather than merge file-backed and
  DuckLake resource definitions. Discard these setup attempts from throughput
  evidence; the actual baseline requires a fresh Compose project.

Independent reviewer `review_763_plan` approved the resident normalization fix
on 2026-09-24 after reproducing it separately through Tidewave. Required cases:
resident/mixed normalization idempotence, production JSON validation followed by
runtime normalization, and rejection of finite/nil/string infinity resident grace
and infinity elastic grace. No broader architecture change is needed for this bug.


## Resumed manual qualification: short assets and live View

The successful unchanged-rc19 baseline used five fixed elastic runners and the
0.5-vCPU orchestrator. No generated SQL asset contains a sleep. The unused
`CRM_API_LATENCY_MS` setting was removed from the local overlay. The first
35-target run finished in 62.640 seconds, with median runner asset execution
498 ms (minimum 416 ms, p95 1,956 ms, maximum 2,979 ms). Its rerun finished in
26.459 seconds, median 485 ms (minimum 426 ms, p95 2,414 ms, maximum 3,274 ms).
Runner execution durations come from all 70 persisted successful task receipts,
not SQL-log timing alone. Both runs produced exactly 35 materializations; all
35 physical tables contained 1,000 distinct IDs and the expected sum 499,500.
The sampled first-run CPU usage consumed 89.64% of the 0.5-core budget and
86.02% of CPU periods were throttled. Emulation and image health probes are
included; these are local comparative measurements, not production capacity.

A fresh `favn-763-outage` case initially failed before any network fault fired:
manifest activation left unresolved inspections, admission permanently rejected
the run, and two runners exited with status 0. No Run row, SQL attempt, generation
or materialization was created. Keep this separate from issue #763 reproduction.
The no-trigger fault watcher exited without disabling the proxy. After explicitly
restarting the two stopped runners, a third activation completed all 47 relation
inspections successfully. Evidence is retained in the ignored
`.favn/registration-stress/evidence/` directory; `/tmp` did not survive reboot.

**Additional confirmed claim-protocol bug:** `RunnerTasks.finish_claim_error/2`
finishes a failed store claim in `RunnerRegistry` as successful `NoWork(wait_ms: 0)`.
`RunnerAgent` retains the command ID when retrying a storage failure. The registry
therefore replays a successful zero-wait response to that retry. For an elastic
runner, this schedules immediate `idle_expired`, sets `final_claim?`, and a further
empty response stops the runner even with a one-hour configured idle grace. A
side-effect-free Tidewave invocation of the actual registry callbacks reproduced
the erroneous cached response. The stopped containers' logs show storage claim
errors followed by clean draining; the protocol bug explains that sequence, but
the original transient storage failure's cause is not yet established.

The user additionally requested a production View with `/runners` held open so
LiveView subscriptions contribute load. The image and HTTPS proxy are running.
Browser connection is pending user handling of the local certificate warning;
setting the isolated Entra-seeded simulation administrator's password also needs
explicit approval after automatic approval review rejected that recovery action.
Do not label the existing baseline as including a connected runners page.


## First durable-trigger outage outcome

At 15:25:58.470 UTC, the observer found Target01's authoritative success receipt
and materialization while its generation remained building and no marker task
existed. It disabled the orchestrator-only database proxy at 15:25:58.484 and
restored it at 15:27:28.489 (90.005 seconds). The initial HTTP attempt could not
connect because the Compose internal network did not publish the API port; after
adding the explicit observation bridge, the exact same idempotency key and body
were submitted. No new key masked the uncertain attempt.

The outage reproduced the Sequencer checkout crash at 15:26:29.188. Its restart
then triggered NotificationListener initialization failures: the actual Postgrex
listen result was `{:eventually, ref}`, a documented successful deferred
subscription, but the listener accepts only `{:ok, ref}`. Repeated bad init returns
exhausted supervision and emptied the orchestrator runner registry. Three idle
runner containers remained alive without registry presence, and two runners with
expired assignments later rejected re-registration and stopped. The one-hour
elastic idle grace used by this harness makes the missing idle reconnection more
visible; it must not be mistaken for the production default grace.

After recording those states and confirming no active assigned/running tasks,
the five runners were explicitly restarted at 15:30:59. Recovery then completed
34 proven successful materializations and activated all 34 corresponding
generations. The run ended `error` at 15:31:52.471; the remaining task/claim had an
unknown effect and its generation remained building. **This case reproduced the
supervision/reconnection cascade, but did not strand a proven successful
materialization after runner replacement.** Keep the unknown outcome protected.
This is not yet a positive reproduction of the missing-marker production case.
Its retained volume is useful for forward-upgrade unknown-outcome qualification.

Independent review also rejected the first reservation-release-only proposal for
the false-NoWork bug: replaying an earlier empty subattempt can hide a later
committed assignment. The revised plan checks exact-session existing assignment
on empty-receipt replay and fences local reservations with fresh tokens, without
new task acquisition, lease renewal, receipt rewriting or old-session adoption.


## First implementation slices and evidence bookkeeping

The resident-pool normalization correction passed 26 focused tests and independent
implementation review. Storage-consumer containment passed five focused tests
against a separate `favn_test_763` database in the existing OrbStack PostgreSQL
container. No native database was installed. Tests exercise a held one-connection
checkout, a real statement timeout, invariant-error rollback, consumer sibling
PID preservation, and a Postgrex notification connection initially pointed at an
unavailable endpoint before reconnecting and delivering a real notification.
Independent review approved this slice without actionable findings.

Claim recovery now uses fresh local reservation tokens and releases a failed
reservation without manufacturing NoWork. Fifteen registry/facade tests pass,
including a real elastic RunnerAgent retrying the identical command and retaining
60 seconds of idle grace. Ninety PostgreSQL task-store tests pass (two excluded),
including ordinary and final-subattempt empty-receipt recovery with unchanged
task/lease/fence/receipt/demand, queued-only empty replay and old-session or
incompatible assignment rejection. Independent review is pending. The durable
registration ownership change is still outstanding.

A bookkeeping bug in the local load driver was found: API idempotency keys are
hashed in `run_submissions`, so filtering that column with the raw key prefix
missed accepted runs. The earlier driver therefore timed out even though its run
had already reached a terminal error. It now tracks returned run IDs and records
a local exclusive key-prefix reservation plus fsynced intent before submission.
Do not treat the earlier timeout as proof that a run remained nonterminal. This
is a bounded-backlog driver, not a fixed offered-rate benchmark.


Independent review found one more claim-owner ambiguity: active rebuild-validation
work can be hidden when its owner row is locked, because candidate validation
uses SKIP LOCKED. This affects both the existing fresh claim and the new empty
receipt replay. The approved shared-helper refinement returns an explicit
retryable conflict while ownership cannot be confirmed and leaves the existing
assignment for fenced recovery. A locked-owner regression is being added before
claim implementation review can be accepted.


The shared owner-validation correction now passes the real row-lock test with
both fresh and empty-receipt claims. All 91 owning task-store tests pass (two
excluded). The rerun exposed one existing fixture's fixed platform-global
`native-claim` command ID; making it workspace-unique allows repeatable runs
against retained test data. Independent re-review approved the corrected claim
implementation and canonical documentation. Test-environment compilation with
warnings as errors, the CI tag-tier guard and diff whitespace checks passed.
Candidate image comparison and the remaining registration/performance slices
are still outstanding.


## Candidate warm-run latency and credential decision

Candidate d67779259be37a2913190814b8d81189acfdd252 used the same five baseline
runner images and 0.5-CPU control-plane limit. Its cold no-fault run completed in
61.411 seconds; the following warm run took 24.468 seconds. Both produced all 35
successful materializations. Data audit found all 35 tables with 1,000 distinct
rows and the expected sum; this does not independently prove no repeated writes.

Adding 10 ms latency with 3 ms jitter in each control-database direction made
warm run `run_api_90e4e01bd988e45415a30467efa60777` take 456.294 seconds
(16:13:43.678502 to 16:21:19.972544 UTC). The external driver timed out at 180
seconds, but did not cancel or resubmit the accepted run. It subsequently ended
`ok`, with 35 successful receipts/materializations and all five runners still
registered. Intermittent samples with all runners idle were not a permanent lost
wake: work continued. This is a performance reproduction, not evidence of the
missing-marker correctness defect. Individual SQL operations remained short.

After explicitly clearing the latency, two further runs completed in 25.168 and
24.069 seconds without restarting runners. This reversible approximately 19x
warm-run slowdown points to database-latency amplification; the dominant query
paths still need measurement. Observation itself adds reads, and these amd64
images run under ARM emulation. Do not extrapolate native production capacity.

The user approved adding a local password, but the documented release command
returned `password_input_failed` / `stdin_unavailable` before changing credentials.
The user then explicitly chose to skip login setup and continue testing. Do not
retry credential recovery. If reproduction remains inconclusive, the authorized
fallback is a fresh password-authentication deployment with the View open, with
the user starting runners personally. Existing tests still have no authenticated
LiveView browser connected. Preserve current evidence and volumes when switching.

Independent harness review requested exact proxy ownership/namespace checks,
restoration-independent watcher cleanup, archived revision in build.env, and
current-assignment receipt joins. These are applied; re-review is pending. A live
wrong-port test was rejected before mutation; the correct project's latency was
then cleared. The support-budget variance was accepted as justified.


## Candidate warm-generation 90-second outage

A separate bounded diagnostic cut the same verified proxy after the first exact
success receipt of `run_api_6170cfba47715b341b88b2843ef9c701`. This was a warm
generation test, explicitly not the original first-marker trigger. Connectivity
was disabled at 16:27:14.149 and restored at 16:28:44.201 UTC. No runner was
manually restarted. The run ended `error` at 16:29:34.126 with 34 successful asset
tasks and one protected unknown outcome. Four runners remained registered/idle;
the runner with the expired assignment rejected registration and exited. The
registry-wide loss seen with rc19 did not recur in this test. An unknown write
requires reconciliation; neither a table count nor runner replacement permits
blind retry. Do not report this as all-success recovery or proof the original
missing-marker bug is fixed.

The reviewed harness corrections were independently approved. The current
scope is manual-only and the supporting-code variance was accepted. Baseline
and candidate volumes and raw evidence remain retained locally.


## Fresh 0.25-vCPU case (ongoing)

On user request, the recovery case quota was changed in place to `25000 100000`
without restarting it. A later fresh `favn-763-quarter` case uses the same candidate
control image and five fixed runner images, with a Compose override preserving
0.25 CPU. All prior volumes are retained. Password-mode workspace bootstrap
succeeded using a private local file; no credential is included in this record.
The user explicitly authorized starting all five runners. View is healthy but the
browser still refuses the local certificate, so there is no authenticated browser
subscription load.

The control-plane image's 10-second RPC health probe repeatedly exceeds its
5-second deadline at this quota, while the authenticated API returns 200 and the
application reports accepting. The original health probe remains enabled. Setup
continued using explicit `--no-deps` commands after verifying API readiness;
its initial `--wait` attempt failed. Report this readiness-probe defect together
with amd64 emulation, rather than assuming the service is down.

Before any injected network fault, first activation produced two safe/retryable
`runner_task_manifest_unavailable` inspection failures. Repeating activation
completed additional inspection tasks, but 37 of 47 bindings still became
`operator_decision` / `physical_inspection_unavailable`. The first 35-asset
submission `run_api_aa91007b9ad1ea832fc24ab43ce349f0` was accepted into the
submission queue then permanently rejected during admission; it created no Run
or asset write. This is a genuine pre-run failure, not missing initial registration.
A bounded 60-second trace of inspection error returns is diagnosing a third
safe inspection attempt. No network fault or 100-asset workload has been started.


The activation trace captured 18 error returns: 15 retryable persistence
`:unavailable` errors ("database connection unavailable") and three safe/retryable
runner manifest-preparation failures. `TargetCompatibilityPlanner` collapses such
errors into persistent operator-decision bindings. This explains why a transient
capacity/storage problem can block later admission; the underlying database
exception is redacted by the persistence mapper and is not yet attributed to a
specific checkout, timeout or connection failure.

To reach the separately requested 35-asset execution test, activation alone was
given 1 CPU. That attempt produced 47 uninitialized/usable bindings. CPU was then
restored and verified as `25000 100000` before submission. The resulting run is
`run_api_06349c257d264cbe32675d0a77756f27`, submitted at 16:46:14.687 UTC and
started at 16:46:15.326657. There is no injected fault, no extra latency, and no
100-asset workload. Do not present this as a successful all-quarter-CPU deployment:
its pre-run failure and setup-only CPU intervention are separate evidence.


## Positive missing-registration reproduction: 35 assets at 0.25 CPU

Run `run_api_06349c257d264cbe32675d0a77756f27` started at
16:46:15.326657 UTC and ended `error` at 16:48:02.096971 (106.770 seconds).
The public run error is `recovery_exhausted` with phase/reason code
`registration_retry_exhausted`. Logs show retryable persistence unavailability
and scheduled generation-registration retries before terminal failure. Source
`RegistrationRetry` has a 30-second/eight-slot budget, and RunServer routes its
exhaustion into failure cleanup, which cancels run-owned registration tasks.

The durable state continued settling after terminal failure: the immediate
snapshot had 31 materializations and five active generations; later it had 34
materializations. At 16:50:03.282 UTC, all 35 current-assignment asset success
receipts had 35 successful/resolved claims and 35 materializations. Only six
generations were active; **29 remained building**. Six marker initialization
tasks succeeded and nine were cancelled. Exact joins show examples of both
materialized building targets with no marker task and materialized building
targets whose marker task was cancelled. No asset task has an unknown outcome.
All five runners are still registered and idle, with no outstanding demand.

Physical read-only audit verified every one of the 35 tables: 1,000 rows, 1,000
distinct IDs and sum 499,500. DuckDB emits the sum as a JSON string; the audit
normalizes its numeric type. This supports data correctness, but is not an
independent proof that no replacement write ever repeated.

**This positively reproduces the central issue-763 symptom:** proven successful
materializations outlive a failed run without completing initial registration.
No network delay/outage was injected. No 100-asset case was run. The earlier
setup-only CPU boost remains a documented qualification deviation; all execution
and initial registration occurred after restoring the 0.25-CPU limit. View ran
without an authenticated browser session because of its local certificate.

During the 103.488-second measurement segment, the orchestrator used 24.837 CPU
seconds, about 96.0% of its 0.25-CPU budget; 95.3% of CPU periods were throttled.
These numbers include image RPC health probes and observation, under amd64
emulation. They demonstrate local pressure, not native production capacity.

Preserve this exact case and its volumes for forward-fix qualification. Do not
rerun asset writes, reset bindings, or fabricate marker success. The reviewed
plan's durable target-owned registration coordinator is directly supported by
this result: registration must survive run failure independently of the already
completed write. A larger timeout alone leaves the ownership/lifetime defect.
Additional reproduced defects are retryable inspection failures becoming durable
operator-decision bindings and RPC health probes exceeding their timeout at low
CPU. Underlying storage-unavailable causes and CPU hot paths still need attribution.

Ignored evidence: `quarter-35-ready-load.jsonl`, `quarter-run-api.json`,
`quarter-stranded-proof.json`, `quarter-35-settled.json`, `quarter-data-audit.log`,
`quarter-timeline.jsonl`, `quarter-cpu-summary.json`, and activation trace/intervention
files under `.favn/registration-stress/evidence/`. Source image remains d6777925.
