# PostgreSQL retention

Retention removes eligible control-plane history in bounded transactions. It does
not delete external datasets. Optional families default to `retain_forever`, with
optional cleanup disabled. Receipt cleanup remains scheduled even when optional
cleanup is disabled; command expiry never depends on physical deletion.

## Policy and progress

One platform policy selects periods in seconds, workspace holds, interval, and
budgets. See the [operator runbook](../../production/postgresql_operator_runbook.md#retention)
for configuration commands. Default budgets are 250 deleted rows, 1,000 preview
candidates, 100 ms lock waits, one-second statements and a five-second transaction.
The minimum row budget is five so the final run/event envelope can be removed.
Rows hidden by cascades must not exceed the budget: children are removed explicitly.
Database statement/transaction deadlines also bound expensive reference scans;
`scan_limit` bounds returned preview candidates, not PostgreSQL physical page reads.

A single timer starts one supervised task. Replicas and explicit commands share a
nonblocking transaction advisory lock and one `maintenance_jobs` row. Deletion,
phase progress, counters and version commit together. Each command requires the
last observed version. After a lost acknowledgement, read status before resuming;
repeating an old version cannot accidentally execute the next batch. Boot policy
must match persisted policy. Configuration changes serialize with cleanup.

Families rotate between transactions. Bounded phases allow a large owner to make
progress without a large delete. A protected or locked row is revisited later;
an empty batch does not prove the database has no eligible history. Optional
cleanup can be paused without affecting command expiry. Workspace holds also
pause physical receipt deletion. A hold cannot restore already deleted history.
Shared platform records are conservatively protected when any workspace is held.

## Ownership and replay

Execution age starts at terminal settlement, including required descendants.
Current dataset evidence, active work, unknown effects, retry lineage, operator
results, and unconsumed projection source block retirement. Materialization claim
keys are reusable across runs: their fencing counters are retained, even after a
claim is released. Keeping these references can intentionally retain old runs.

Run groups (including their backfills), rebuilds, standalone tasks, deployments
and manifests become retiring before any child disappears. Historical detail returns explicit expired history during
retirement and not-found after deletion. Publication pages check a durable replay
floor in the same snapshot as their query. Source required to repair a retained
projection stays; missing-row repair shares the retention lock.

Logs use `(publication_id, batch_offset)` cursors in both directions. Event time
remains a display/filter value; delayed old-timestamp logs sort by publication.
The feed combines stored diagnostics with lifecycle messages derived from run
events. Diagnostics follow the logs period; lifecycle messages follow execution
history. Cursor checks use both replay floors. A fresh history request can still
show older retained lifecycle messages, but an expired cursor cannot resume across
a gap. Keep the initial page replay cursor separately when paging older history.
Unpublished logs are not yet in historical pages. Log delivery older than seven
days is rejected; physical cleanup also allows five minutes of clock skew.
Sparse cleanup can expire a cursor even when some older protected rows remain.
An expired run-event SSE cursor returns HTTP 410 before streaming, or closes an
already connected stream. Restart from a current snapshot instead of treating expiry as
successful complete replay.

Registry references are checked by explicit PostgreSQL triggers under owner row
locks. This covers both foreign keys and logical references. A matching cached
manifest cannot make a retired or deleted identity readable again. Registry
periods use original publication/insertion age; references still protect content
while in use. Global packages remain protected by every manifest link and pin.

## Table inventory

This exact inventory is checked against a freshly migrated database. “Retained”
means retention does not delete the table; its ordinary lifecycle may update it.
Eligibility always includes workspace holds and the relevant replay protections.

| Table | Family | Rule |
| --- | --- | --- |
| `admission_waiters` | retained | Current coordination, identity, or reusable fencing state; lifecycle commands own settlement. |
| `asset_attempt_overviews` | execution_history | Terminal execution group, expired replay, settled descendants, no retained external reference; children first. |
| `asset_evidence_bindings` | retained | Dataset provenance or current projection; exact source history stays referenced. |
| `asset_freshness_states` | retained | Dataset provenance or current projection; exact source history stays referenced. |
| `asset_target_bindings` | retained | Dataset provenance or current projection; exact source history stays referenced. |
| `asset_target_generations` | retained | Dataset provenance or current projection; exact source history stays referenced. |
| `asset_window_states` | retained | Dataset provenance or current projection; exact source history stays referenced. |
| `auth_actors` | retained | Current coordination, identity, or reusable fencing state; lifecycle commands own settlement. |
| `auth_audit_entries` | retained | Permanent audit or reconciliation identity. |
| `auth_credentials` | retained | Current coordination, identity, or reusable fencing state; lifecycle commands own settlement. |
| `auth_external_identities` | retained | Current coordination, identity, or reusable fencing state; lifecycle commands own settlement. |
| `auth_operator_commands` | sessions | Expired sessions and settled intents; unresolved attribution and replay remain protected. |
| `auth_platform_audit_entries` | retained | Permanent audit or reconciliation identity. |
| `auth_platform_grants` | retained | Current coordination, identity, or reusable fencing state; lifecycle commands own settlement. |
| `auth_sessions` | sessions | Expired sessions and settled intents; unresolved attribution and replay remain protected. |
| `auth_workspace_memberships` | retained | Current coordination, identity, or reusable fencing state; lifecycle commands own settlement. |
| `backfill_overviews` | execution_history | Terminal backfill and settled descendants retire with the execution group; one root marker protects all group reads. |
| `backfill_plan_batches` | execution_history | Terminal backfill and settled descendants retire with the execution group; one root marker protects all group reads. |
| `backfill_windows` | execution_history | Terminal backfill and settled descendants retire with the execution group; one root marker protects all group reads. |
| `backfills` | execution_history | Terminal backfill and settled descendants retire with the execution group; one root marker protects all group reads. |
| `capacity_scopes` | retained | Current coordination, identity, or reusable fencing state; lifecycle commands own settlement. |
| `coverage_baselines` | retained | Dataset provenance or current projection; exact source history stays referenced. |
| `execution_group_overviews` | execution_history | Terminal execution group, expired replay, settled descendants, no retained external reference; children first. |
| `execution_lease_scopes` | operations | Released lifecycle state or superseded diagnostic; current ownership remains protected. |
| `execution_leases` | operations | Released lifecycle state or superseded diagnostic; current ownership remains protected. |
| `execution_packages` | registry | Unreferenced package after publication grace, including formerly linked packages; pins and shared manifests protect it. |
| `idempotency_records` | idempotency | Expired committed result, with no retained operator intent. |
| `log_batches` | logs | Ingestion and publication age, delivery replay horizon, holds and active runs; publication-position floor. |
| `log_entries` | logs | Ingestion and publication age, delivery replay horizon, holds and active runs; publication-position floor. |
| `maintenance_jobs` | maintenance | Completed historical jobs; singleton scheduler and active repair progress stay. |
| `manifest_activation_leases` | retained | Current coordination, identity, or reusable fencing state; lifecycle commands own settlement. |
| `manifest_deployment_operations` | retained | Permanent audit or reconciliation identity. |
| `manifest_deployment_upload_leases` | retained | Current coordination, identity, or reusable fencing state; lifecycle commands own settlement. |
| `manifest_execution_packages` | registry | Unreferenced inactive deployment or manifest; reject new references while retiring children. |
| `manifest_versions` | registry | Unreferenced inactive deployment or manifest; reject new references while retiring children. |
| `materialization_claims` | retained | Current coordination, identity, or reusable fencing state; lifecycle commands own settlement. |
| `materializations` | retained | Dataset provenance or current projection; exact source history stays referenced. |
| `outbox_events` | owner history | Published and consumed source removed with its eligible owner; retain all other source. |
| `outbox_publication_state` | retained | Sequence, replay boundary, schema, projection progress, or key inventory authority. |
| `projection_cursors` | retained | Sequence, replay boundary, schema, projection progress, or key inventory authority. |
| `projection_failures` | operations | Released lifecycle state or superseded diagnostic; current ownership remains protected. |
| `rebuild_operations` | operations | Unreferenced settled operation; unresolved effects, retained generations and child operations protect it. |
| `rebuild_plan_actions` | operations | Unreferenced settled operation; unresolved effects, retained generations and child operations protect it. |
| `rebuild_windows` | operations | Unreferenced settled operation; unresolved effects, retained generations and child operations protect it. |
| `resource_circuit_outcomes` | execution_history | Terminal execution group, expired replay, settled descendants, no retained external reference; children first. |
| `resource_circuits` | retained | Current coordination, identity, or reusable fencing state; lifecycle commands own settlement. |
| `resource_recovery_candidates` | operations | Terminal scheduling/recovery history after replay and retained source dependencies end. |
| `retention_floors` | retained | Sequence, replay boundary, schema, projection progress, or key inventory authority. |
| `run_events` | execution_history | Terminal execution group, expired replay, settled descendants, no retained external reference; children first. |
| `run_execution_checkpoints` | execution_history | Terminal execution group, expired replay, settled descendants, no retained external reference; children first. |
| `run_ownerships` | execution_history | Terminal execution group, expired replay, settled descendants, no retained external reference; children first. |
| `run_plans` | execution_history | Terminal execution group, expired replay, settled descendants, no retained external reference; children first. |
| `run_submission_commands` | receipts | Seven-day replay plus clock-skew safety; explicit child budgets and workspace holds. |
| `run_submissions` | execution_history | Terminal execution group, expired replay, settled descendants, no retained external reference; children first. |
| `run_targets` | execution_history | Terminal execution group, expired replay, settled descendants, no retained external reference; children first. |
| `runner_capacity_demands` | retained | Current coordination, identity, or reusable fencing state; lifecycle commands own settlement. |
| `runner_sessions` | sessions | Expired sessions and settled intents; unresolved attribution and replay remain protected. |
| `runner_task_command_tasks` | receipts | Seven-day replay plus clock-skew safety; explicit child budgets and workspace holds. |
| `runner_task_commands` | receipts | Seven-day replay plus clock-skew safety; explicit child budgets and workspace holds. |
| `runner_task_log_batches` | execution_history | Terminal execution group, expired replay, settled descendants, no retained external reference; children first. |
| `runner_task_outcomes` | receipts/history | Obsolete versions after receipt references expire, or with an eligible execution group. |
| `runner_task_runtime_input_errors` | receipts/history | Obsolete versions after receipt references expire, or with an eligible execution group. |
| `runner_tasks` | execution_history/operations | Retire with a group or rebuild, or as an unreferenced standalone terminal task; target-recovery evidence remains protected. |
| `runs` | execution_history | Terminal execution group, expired replay, settled descendants, no retained external reference; children first. |
| `runtime_input_key_versions` | retained | Sequence, replay boundary, schema, projection progress, or key inventory authority. |
| `runtime_input_pins` | execution_history | Terminal execution group, expired replay, settled descendants, no retained external reference; children first. |
| `schedule_activation_commands` | retained | Current coordination, identity, or reusable fencing state; lifecycle commands own settlement. |
| `schedule_activations` | retained | Current coordination, identity, or reusable fencing state; lifecycle commands own settlement. |
| `schedule_cursors` | retained | Current coordination, identity, or reusable fencing state; lifecycle commands own settlement. |
| `schedule_occurrences` | operations | Terminal scheduling/recovery history after replay and retained source dependencies end. |
| `schema_migrations` | retained | Sequence, replay boundary, schema, projection progress, or key inventory authority. |
| `target_operation_locks` | retained | Current coordination, identity, or reusable fencing state; lifecycle commands own settlement. |
| `target_recovery_operations` | retained | Required materialization and generation links preserve recovery evidence, including its tasks. |
| `target_statuses` | retained | Dataset provenance or current projection; exact source history stays referenced. |
| `workspace_deployment_targets` | registry | Unreferenced inactive deployment or manifest; reject new references while retiring children. |
| `workspace_deployments` | registry | Unreferenced inactive deployment or manifest; reject new references while retiring children. |
| `workspace_provisioning_operations` | retained | Permanent audit or reconciliation identity. |
| `workspace_runtime_state` | retained | Current coordination, identity, or reusable fencing state; lifecycle commands own settlement. |
| `workspaces` | retained | Current coordination, identity, or reusable fencing state; lifecycle commands own settlement. |

## Logical references

Execution retirement checks all members of the root group, retained materializations
and claims, leases/waiters, backfill windows, rebuild children, current target/window
state, schedule occurrences, recovery source/results, task receipt snapshots,
external parent/rerun/cancellation links, submission retry lineage, and operator
result identities. Current task outcome and runtime-input error versions remain
until their owning task can retire. Pins retire with their execution owner.

Groups containing submission retry or supersession chains remain protected.
Standalone terminal tasks retire in bounded child phases under the execution-history
policy. Target-recovery operations always reference a materialization and generation;
those records and their tasks remain protected as recovery evidence. This can also
retain their manifest and deployment references.

Registry predicates enumerate exact manifest/deployment columns across runs,
submissions, tasks, operations, target/evidence state, scheduling and workspace
activation. Deployment targets and package links retire with their registry owner.
Permanent deployment operation identities can keep their registry content forever.

## Operational limits

This is not a fixed database-size guarantee. Audit, current state, fencing and
provenance can grow permanently. Measure eligible backlog separately from protected
history. Deletion creates dead tuples; autovacuum reclaims reusable space and need
not shrink allocated files. Monitor table, index and TOAST bytes, WAL and vacuum
progress. Ordinary retention never runs `VACUUM FULL`.

Preview reports a bounded sample of eligible rows or owners and the observed job
version. An incomplete result is a lower bound, not a total. Owner previews also
report aggregate referenced/unsettled counts; they do not classify every blocking
reference separately. The first version has no preview continuation or byte
estimate. Failures are reported through rate-limited warnings and telemetry;
failed transactions leave durable progress unchanged.
