# favn_orchestrator

Purpose: control plane and system of record for manifests, runs, schedules,
events, auth/authz/audit, command idempotency, private HTTP API, backfill state,
asset freshness state, and storage contracts.

Code:
- `apps/favn_orchestrator/lib/favn_orchestrator.ex`
- `apps/favn_orchestrator/lib/favn_orchestrator/`
- Auth/session/service-token helpers under `apps/favn_orchestrator/lib/favn_orchestrator/auth/`
- Run submission, cancellation, execution admission, materialization-claim
  admission, and recovery in
  `apps/favn_orchestrator/lib/favn_orchestrator/run_manager.ex`,
  `apps/favn_orchestrator/lib/favn_orchestrator/run_execution_ownership.ex`,
  `apps/favn_orchestrator/lib/favn_orchestrator/execution_admission.ex`,
  `apps/favn_orchestrator/lib/favn_orchestrator/materialization_claim.ex`, and
  `apps/favn_orchestrator/lib/favn_orchestrator/run_recovery.ex`
- Durable runner execution ownership is stored behind the storage adapter boundary
  before runner dispatch and updated through submitted, started,
  finish-persist-pending, completed, and cancellation-outcome states. Crash
  cleanup reads this ledger instead of trusting run metadata alone. Ownership
  records carry `ownership_id`, `dispatch_id`, deadline, runner reference, and
  cancellation outcome fields; runner work metadata includes the ownership and
  dispatch identifiers before dispatch.
- Run process internals under `apps/favn_orchestrator/lib/favn_orchestrator/run_server/`
- `FavnOrchestrator.RetryPolicyResolver` freezes operator → asset → pipeline →
  default retry policy on planned nodes. Run-server execution schedules only
  typed known-safe failures, preserves successful siblings, persists absolute
  retry wake times, and restores safe retry waits after restart.
- `FavnOrchestrator.RuntimeInputPins` owns resolve/pin/execute, fresh/pinned/
  inherit replay modes, atomic conflicts, and safe lineage. The dedicated
  storage codec protects sensitive payloads; generic run metadata contains no
  raw resolved parameters.
- Runner cancellation envelope and best-effort dispatch under
  `apps/favn_orchestrator/lib/favn_orchestrator/run_server/cancellation.ex` and
  normalized cancellation DTOs in
  `apps/favn_orchestrator/lib/favn_orchestrator/cancellation_outcome.ex`.
  `:cancelled` is persisted only for acknowledged cancellation; already-completed
  and unknown/failed runner outcomes remain explicit non-cancelled cleanup states.
- Run read models in `apps/favn_orchestrator/lib/favn_orchestrator/run_read_model.ex`,
  including bounded operator run detail, execution-group summaries/details,
  asset attempts, windows, event cursors, and timeline entries for thin operator
  clients.
- Target status projection in
  `apps/favn_orchestrator/lib/favn_orchestrator/target_status.ex` and
  `apps/favn_orchestrator/lib/favn_orchestrator/target_status/projector.ex`,
  including persisted current asset/pipeline catalogue status, run/latest-freshness
  projection updates, target-scoped bounded detail history reads, module-submitted
  pipeline attribution for named pipeline targets, and explicit rebuild from
  authoritative history.
- Schedule inspection/read models in `apps/favn_orchestrator/lib/favn_orchestrator/scheduler_entry.ex`
  and `apps/favn_orchestrator/lib/favn_orchestrator/schedule_list_entry.ex`,
  including operator activation state, effective-enabled scheduling semantics,
  runtime state, fingerprint review, next-due calculation, occurrence preview,
  compact scheduler errors, and public enable/disable commands for thin operator
  clients.
- Schedule list/detail reads may bootstrap missing persisted scheduler state from
  the active manifest when the scheduler runtime is not running; storage read or
  write failures are returned to callers rather than converted to default state.
- Scheduler ticks keep failed state writes in an explicit dirty set, report them
  in scheduler diagnostics, and retry dirty writes before evaluating schedules on
  later ticks.
- Freshness execution/query helpers under `apps/favn_orchestrator/lib/favn_orchestrator/freshness/`
- Reusable runtime-state repair passes under `apps/favn_orchestrator/lib/favn_orchestrator/repair/`
- Refresh policy normalization and forced-run selection in
  `apps/favn_orchestrator/lib/favn_orchestrator/refresh_policy.ex`
- Operator run/backfill command DTOs under
  `apps/favn_orchestrator/lib/favn_orchestrator/operator_commands/`, used by the
  public facade to translate browser/API/CLI intent into runtime submit options.
- Operational pipeline and asset backfill range expansion, parent/child run
  grouping, ledger rows, and partial-submission compensation in
  `apps/favn_orchestrator/lib/favn_orchestrator/backfill_manager.ex`
- Storage boundary codecs and JSON-safe DTO normalization under `apps/favn_orchestrator/lib/favn_orchestrator/storage/`, including full-row operational-backfill read-model codecs under `apps/favn_orchestrator/lib/favn_orchestrator/storage/backfill/`
- preserved public contracts under `apps/favn_orchestrator/lib/favn/`
- Private API router and DTO boundary under `apps/favn_orchestrator/lib/favn_orchestrator/api/`
- HTTP contract schemas for private API JSON-safe DTOs under `apps/favn_orchestrator/priv/http_contract/v1/`
- Production runtime config, normalized runtime dependency config, readiness,
  diagnostics, projection degradation diagnostics, telemetry, redaction, safe
  operator error DTOs, and operational event modules under
  `apps/favn_orchestrator/lib/favn_orchestrator/`
- Public facade readiness and diagnostics entrypoints on `FavnOrchestrator`, used
  by same-BEAM web readiness and operator tooling.
  Storage adapters without readiness support report `:unknown` instead of healthy;
  SQLite and Postgres expose explicit schema/migration readiness diagnostics.
- Command idempotency helpers under `apps/favn_orchestrator/lib/favn_orchestrator/idempotency.ex`

Tests:
- `apps/favn_orchestrator/test/`
- API contract tests under `apps/favn_orchestrator/test/api/`
- HTTP schema shape tests under `apps/favn_orchestrator/test/http_contract/`
- Auth storage/facade tests under `apps/favn_orchestrator/test/auth/`
- Operator command DTO tests under `apps/favn_orchestrator/test/operator_commands/`
- Production runtime config/runtime dependency config/readiness/diagnostics tests under `apps/favn_orchestrator/test/production_runtime_config_test.exs`, `apps/favn_orchestrator/test/runtime_config_test.exs`, `apps/favn_orchestrator/test/readiness_test.exs`, and `apps/favn_orchestrator/test/diagnostics_test.exs`
- storage contract/codec tests under `apps/favn_orchestrator/test/storage/`
- Freshness decision/query tests under `apps/favn_orchestrator/test/freshness/`
- cross-app runner integration coverage under `apps/favn_orchestrator/test/orchestrator_runner_integration_test.exs`
- execution-group run read-model coverage in `apps/favn_orchestrator/test/run_read_model_test.exs`
- optional adapter contract smoke coverage under `apps/favn_orchestrator/test/integration/`; adapter-specific suites own full adapter coverage

Use when changing run lifecycle, durable runner ownership, run cancellation
semantics, retry policy/recovery, runtime-input pinning/replay, freshness
decisions/queries, scheduling, private API behavior, SSE/events, auth, command
idempotency, bootstrap service-token/runner-registration endpoints, same-BEAM
readiness facade behavior, backfill orchestration, execution leases,
materialization claims, runtime-state repair, control-plane
concurrency/admission, queue reasons, or storage contract semantics.

SQL check results, `quality_status`, and `write_outcome` persist in the owning
node attempt's JSON-safe output metadata. Successful warnings/no-ops keep normal
success, freshness, and downstream gating; rolled-back attempts retain the same
bounded diagnostics under an error attempt and are not presented as committed
quality results.

Asset detail assurance is an orchestrator-owned read model. It joins the active
manifest's typed SQL contract and check definitions with latest persisted run
metadata, including candidate schema observations and structural differences.
The public catalogue facade returns that complete read model; thin clients must
not inspect manifests, runner state, or SQL adapters to reconstruct assurance.
