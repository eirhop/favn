# Run ownership and recovery

A run's coordinator may save progress only while PostgreSQL recognizes its owner
and fencing generation. This prevents an old process from making new decisions
after recovery has taken over. Runner task identities and unresolved external
write holds remain authoritative across every local process restart.

## Independent renewal

`RunServer` owns execution state. A separate `RunLeaseKeeper` renews its run
lease through a storage-owned pool of two connections. Slow execution callbacks,
materialization maintenance and ordinary connection-pool contention do not queue
renewal behind the coordinator's mailbox.

| Policy | Default or bound |
| --- | --- |
| Run lease | 120 seconds; configurable 120–600 seconds |
| Renewal interval | 10 seconds, initially staggered |
| Renewal operation | 2 seconds including checkout and reply |
| Contention retry | 250–1,000 ms with the original renewal identity |
| Responsiveness | A challenge every 5 seconds; revoke 45 seconds after the last response |
| Admission pause | Less than 30 seconds of lease headroom or more than 20 seconds without a response |
| Admission reopening | Two consecutive healthy checks |
| Final local safety margin | Revoke 10 seconds before conservative expiry |
| Active local runs | 64; configurable 1–512 |
| Concurrent preparation | 4, within the active-run limit |

The receipt includes expiry and a fresh database observation timestamp. The
keeper derives its local deadline from monotonic request start plus the remaining
database lease. Checkout and transport consume that budget. Replaying a renewal
preserves expiry and gets a new observation timestamp; it cannot restore elapsed
time. A locally revoked keeper never accepts late success as restored authority.

A long-running runner task is healthy if its coordinator can answer challenges.
An entirely starved VM cannot renew or answer; after service returns its old
processes must stop and reconcile under fresh authority. Increasing the lease
therefore tolerates transient delays but does not make unlimited CPU starvation
safe. It also increases the wait before recovery after a hard crash.

## Process ownership and admission

The manager and run process supervisors share one `one_for_all` subtree. Run
processes are temporary: no supervisor restarts a worker with an old receipt.
Keeper failure makes the manager stop that generation's coordinator, preparer,
maintenance process and registered helpers. Manager failure stops the complete
subtree before a replacement manager starts. Helpers register before doing work.

Recovery reserves a local slot before claiming. It attaches renewal before loading
the manifest. A newer generation waits for every old local process to go down
before loading a fresh snapshot for execution. If the old generation cannot
confirm shutdown, the replacement persists terminal failure with cleanup intent and
stops. Cleanup cannot start until the previous local generation stops. Repeated adoption of the same generation is
idempotent; an older handoff cannot replace a newer owner.

Fresh permission is required for each new task admission. Already submitted
commands can still be reconciled by their original identities while admission is
paused; reconciliation cannot create missing work. The database checks the owner,
generation, expiry and claim purpose in the authoritative transaction.

Combined-window asset target-lock maintenance is independent of run renewal.
It resolves the exact persisted target/operation/owner/fence from the task
continuation, including after restart with no in-memory watch. Marker operations
retain their separate operation-owned lease policy. Tasks register before
acquisition. Maintenance reads their exact durable lock identities and does not
renew terminal tasks or release unknown-write holds. Expired unresolved target
leases route recovery to diagnosis; they are never silently reacquired. Diagnosis
and cancellation cleanup do not require renewing expired execution locks first.

## Persistent recovery policy

Ownership rows retain the claim purpose, automatic/attention disposition, retry
count and next eligible recovery time. Three automatic execution recoveries are
allowed, with 5, 15 and 60 second backoffs plus bounded jitter. Further claims are
for diagnosis only; the diagnostic owner persists terminal failure and durable
cleanup intent, then stops. Renewal moves the eligibility time forward; restarting the
orchestrator does not remove the backoff. Newly committed node settlement or a
terminal outcome resets the count. Replaying an old event does not.

Saving `run_recovery_required` atomically writes the event, attention revision and
attention disposition under the current fence. Saved attention is excluded from
automatic execution and diagnostic claims. If the diagnostic cannot be saved but
storage accepts a narrow ownership update, the run becomes diagnosis-only. If
storage is unavailable, the failure remains explicit and the remaining bounded
attempts must re-evaluate the same durable evidence.

An authorized resume names the displayed attention revision. It reserves a local
hold, checks for a prior successful resume or conflict, stops prior local work,
then revalidates and commits the resume. A replay or stale revision must not stop
a healthy replacement. The transaction expires the old claim, clears active
attention and resets recovery pacing. Original runner tasks are reconciled before
further dispatch. Durable cancellation takes precedence over resume.

## Uncertain run transitions

Run-start, task-start notifications and terminal persistence retain the original
command while its database outcome is uncertain. One sequence-changing operation
runs at a time. Registered helpers perform database work so the coordinator can
answer lease challenges and retain cancellation intent, including during sequential
execution and cancellation dispatch.

Transient transition errors use a 30-second budget from the first failed attempt,
with one-second retry scheduling. Permanent rejections stop command replay
immediately. Final reconciliation compares canonical saved snapshot and event
content; decoded in-memory hashes are not durable receipts. A confirmed original
receipt allows its matching continuation, even after the retry budget expires.
An already-saved terminal outcome retains its result, error and timestamp.
Observing that outcome does not authorize another bulk capacity release. If the
original terminal write succeeded but its capacity release failed, the outcome
is adopted unchanged; existing admission reconciliation and lease expiry handle
remaining capacity. A normal coordinator exit alone is not proof that every
capacity release succeeded.

If the saved run is still nonterminal and current authority permits it, failure
and cleanup intent are written atomically. Unavailable storage or lost authority
stops the local attempt with task and target evidence retained for paced recovery.
Cancellation keeps its existing authority rules and shares the original retry
budget. The 30-second clock is per live ownership generation; repeated crashes
without settlement progress spend the persisted recovery-attempt limit.

## Registration retries and failed-run cleanup

A successful asset result stays accepted while generation registration retries
explicitly retryable PostgreSQL conflicts, timeouts, and unavailability. The first
failure starts a 30-second budget with at most eight scheduled retries, using
1/2/4/5-second backoff and bounded jitter. Each slot is persisted before dispatch;
restarts retain its deadline and count. Recovery checks existing terminal helper
evidence before spending another slot. Failed reads never mean missing work.

Exhaustion persists `error` and versioned `failure_cleanup` intent atomically.
The original failed outcome stays immutable. The ordinary recovery sweep selects
pending cleanup separately using cleanup ownership, including after restart.
Cleanup first inventories the exact run's tasks in pages and drains all siblings.
Only complete, contiguous event history permits result settlement. A permanent
history gap records a run-level reason, skips settlement from that history, and
retains affected target protection. A missing individual outcome or detail records
its task/sequence reason while independently proven siblings continue. Reasons are
saved in versioned cleanup progress before advancing and survive restart.
Previously saved results remain retained by node and attempt when a later detail
read fails. Sequential cleanup also saves its accepted results before recording
settlement. Cleanup advances diagnostic timestamps while the execution finish time
continues to come from the original terminal row; cleanup neither consumes nor
resets the execution recovery-attempt budget.
Confirmed results and safe resources are settled under the current fence. Failure and
new task claim/start serialize under the history lock: queued or assigned assets
and mutating helpers cannot start after failure. Already-started work may still
report its terminal outcome. No asset task
or generation-marker write is created. Read-only inspection helpers may use a
cleanup-generation identity when original terminal evidence is unavailable; a
restart drains old helpers before admitting new reads. Their normal bounded wait
runs in a registered helper while the coordinator answers lease challenges.

Cleanup ends only after PostgreSQL confirms no active exact-run tasks and no
active execution leases or admission waiters. Temporary persistence failures leave
cleanup pending. Unknown external writes retain their target claims and locks;
after other tasks drain, cleanup reports attention with bounded reason codes.
Future work on unrelated targets can proceed. Two cleanup lifecycles have reserved local slots, in addition to the ordinary
active-run limit and still subject to PlanCapacity memory limits. Cleanup discovery
continues while ordinary admission is paused.
Pending and attention cleanup retain their execution history and task evidence.

The coordinator queues recovered task callbacks one at a time. Reads and step
settlement run in registered helpers with one sequence mutation outstanding.
PlanCapacity accounts for retained state and temporary operation copies. Lost
helper replies stop the owner and restore durable phases; they never repeat a
multi-write settlement blindly. Timers, cancellation hints, and sibling results
remain coordinator-owned and wait behind the matching receipt.

Existing attention runs retain the explicit resume procedure. Failed cleanup
never offers execution resume or changes the failed outcome to successful.

## PostgreSQL and deployment

Renewal takes the shared execution-history guard and a `NOWAIT` ownership-row
lock. It does not take the broad run advisory lock or load the run snapshot.
Run transitions lock ownership before the run row and use `FOR NO KEY UPDATE`
for state changes, allowing foreign-key parent checks. Renewal updates expiry and
recovery pacing in one statement with one database timestamp.
Authoritative run transactions have a PostgreSQL 18 total transaction timeout of
15 seconds, including nested work. An interrupted client does not prove whether a
commit happened: retry only the original command identity and reconcile its receipt.

Stop the old orchestrator before applying the ownership migration and starting the
new binary. Mixed old/new ownership protocols are unsupported. Schema fingerprints
and migration versions prevent an old binary from accepting the upgraded schema;
rollback requires a reviewed forward repair that preserves recovery state.

See the [PostgreSQL operator runbook](../production/postgresql_operator_runbook.md)
for connection budgeting and the recovery resume procedure, and the
[environment reference](../production/control_plane_environment.md) for settings.

Do not roll back to a binary without this cleanup protocol while failed cleanup
is pending/in attention or registration retry events remain active. There is no
schema migration to enforce this application-level compatibility boundary; finish
cleanup with a compatible binary or apply a forward fix, preserving target holds.
