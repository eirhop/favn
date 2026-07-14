# Orchestrator cleanup ledger

This is a working review record, not a public architecture contract.

## Scope and baseline

- Base: `origin/main` at `58eea670`.
- Initial orchestrator source: 127 Elixir files and 42,127 lines.
- Initial largest modules:
  - `FavnOrchestrator`: 4,187 lines.
  - `FavnOrchestrator.API.Router`: 2,860 lines.
  - `FavnOrchestrator.Storage.Adapter.Memory`: 3,208 lines.
  - `FavnOrchestrator.RunServer.Execution`: 2,819 lines.
  - `FavnOrchestrator.RunReadModel`: 2,306 lines.
- Review target: production code in `apps/favn_orchestrator`, plus adapter changes
  required to preserve its storage contract.

## Completed architecture work

- Kept `FavnOrchestrator` as the documented cross-app facade while moving
  authorization, catalogue, lineage, schedules, commands, window selection, and
  manifest-target behavior into focused internal services. The facade is now
  about 1,500 lines.
- Reduced the root Plug router to a roughly 250-line transport boundary with
  resource routers and explicit authentication, filters, response, audit,
  idempotency, command-error, and SSE modules.
- Reduced `RunServer.Execution` to about 1,000 lines and extracted sequential execution,
  stage admission/classification/settlement, freshness context, result building,
  result sanitization, and pool selection. Removed the unused 186-line
  `AwaitTasks` implementation.
- Reduced `RunReadModel` to 1,244 lines and moved node/step reconstruction into a
  dedicated projection module with pre-built indexes and shared event/window
  loads.
- Reduced the memory adapter process to a 1,312-line callback coordinator backed
  by focused state-family modules and explicit indexes.
- Split scheduler evaluation, diagnostics, and state persistence out of the
  runtime process; split lineage request, model, projection, and query behavior;
  and consolidated run submission/admission/cleanup contracts.
- Removed deprecated and unused paths instead of retaining compatibility shims;
  Favn remains private pre-v1.

## Correctness and performance work

- Fixed stale mailbox events deleting newer await/admission state, early or
  duplicate lease release, completed ownership remaining active, and admission
  cleanup failures crashing after durable terminal persistence.
- Made scheduled occurrence identity deterministic across restarts and tied it
  to the pipeline and schedule fingerprint, preventing duplicate submission or
  accidental adoption after schedule edits.
- Added non-blocking retries for transient run-start, step-result, timeout,
  failure, and terminal persistence errors. Execution does not progress until
  the matching transition is durable.
- Persisted completed node and asset results in append-only step events, with
  indexed event type and step identity columns in SQLite and PostgreSQL.
  Active-run detail combines at most 200 recent context events per run with one
  indexed current-state event per step, while the full aggregate run blob is
  written only at terminal settlement.
- Made materialization producer/freshness identities fixed-size fingerprints
  instead of embedding complete encoded node terms.
- Added memory indexes for manifest hashes, execution groups, run relationships,
  backfill windows, logs, auth identities/sessions, and execution leases.
- Removed repeated collection scans and N+1 reads in catalogue, lineage,
  execution-group, run-detail, and backfill projection paths.
- Made log batching, stage accumulation, lineage construction, and summary
  counting linear; bounded pagination and diagnostic result sizes.
- Replaced `Node.start/2` in LocalNode unit tests with the supported local
  `:erpc` self-node path. The focused file fell from repeated 60-second ExUnit
  timeouts to 0.1 seconds while retaining remote-dispatch coverage.
- Centralized duplicated runner-client validation and backfill persisted
  status/window vocabulary.

## Security and persistence work

- Removed all production `binary_to_term/2` paths. Execution leases, admission
  waiters, materialization claims, group summaries, events, logs, idempotency
  responses, and run snapshots use explicit versioned JSON DTOs.
- Persisted input uses allowlists and structural validation. Unknown status,
  event, role, result, timestamp, duration, attempt, pool, and response fields
  do not create atoms or silently broaden queries.
- Durable consumer module identities use strict Elixir-module syntax and a
  255-byte limit. Restoration accepts existing atoms only; decoding never
  creates an atom.
- Hardened service-token parsing, password/session handling, login throttling,
  local actor/session creation, bootstrap config, log subscriptions, SSE fields,
  idempotency failure replay, and manifest-scoped module resolution.
- Redacted and bounded nested errors and diagnostics, including invalid UTF-8,
  before JSON, logging, telemetry, or persistence boundaries.
- Production runtime values now have explicit upper bounds for session TTL,
  SQLite busy timeout, scheduler tick, and missed-occurrence count. Bootstrap
  credentials and roles are validated before application environment mutation.
- Backfill, freshness, target-status, materialization-claim, log, event, auth
  audit, and execution-group records reject malformed identities, timestamps,
  collections, counts, and ranges instead of coercing or failing later.

## Verification

- Focused tests were kept at each owning layer throughout the refactor.
- `MIX_ENV=test mix compile --warnings-as-errors`: passing.
- Strict Credo across all 196 orchestrator production files: passing with no
  findings.
- Sobelow across the orchestrator root: passing with no unreviewed findings.
  The only suppressions are function-scoped and reviewed: line-safe JSON SSE
  framing and bounded durable module-identity restoration.
- `mix deps.audit`: no known dependency vulnerabilities.
- Tidewave runtime initialization and orchestrator evaluation: passing against
  the locally running Phoenix endpoint.
- Final app-scoped fast orchestrator test gate: 752 tests passing in 12.4
  seconds, excluding the explicit acceptance/slow/browser tiers.
- Focused SQLite adapter and storage gate: 53 tests passing.
- Full SQLite and `favn_view` suites: 82 and 165 tests passing after correcting
  stale expectations exposed by the full umbrella run.
- Every umbrella app was exercised under a 45-minute watchdog. The orchestrator
  passed; two `favn_local` production-acceptance tests and one environment-sensitive
  DuckDB permission test fail with the configured `/mnt/c` temp directory. The
  DuckDB test and one `favn_local` test pass under native Linux `/tmp`; the
  packaged-runtime restart failure remains reproducible outside this refactor's
  scope.

## Deliberate non-changes and follow-up

- The 616-line catalogue timeline projection and other cohesive large
  projections were not split mechanically. They have focused ownership and
  tests; a new abstraction should wait for an independent contract.
- The full umbrella command previously took about 11 minutes, including an
  approximately 6.5-minute `favn_local` acceptance path plus environment-sensitive
  DuckDB/generated-server checks. LocalNode's two repeated 60-second delays are
  fixed here; broader cross-app tiering/profile work remains a separate task.
- The root `test` alias does not forward ExUnit arguments. The complete run used
  child-level `mix test --timeout 2700000` commands so slow tests were not killed
  by ExUnit's default 60-second timeout.
