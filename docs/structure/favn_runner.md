# favn_runner

Purpose: execution runtime for pinned manifest work, runner server, worker
execution, plugin loading, runtime context construction, and safe relation
inspection.

Code:
- `apps/favn_runner/lib/favn_runner.ex`
- `apps/favn_runner/lib/favn_runner/`
- `apps/favn_runner/lib/favn_runner/production_runtime_config.ex` owns
  runner-side production env validation for the first local single-node setup
- `apps/favn_runner/lib/favn_runner/plugin_loader.ex` consumes the public
  `Favn.Runner.Plugin` contract with explicit packaged-application startup,
  bounded callback and child-spec expansion, plugin/child counts, duplicate-id
  rejection, and redacted errors
- `apps/favn_runner/lib/favn_runner/extension_supervisor.ex` owns the dedicated
  plugin child subtree that starts before connection and execution services
- `apps/favn_runner/lib/favn_runner/sql_runtime_preflight.ex` validates the complete
  planned SQL connection scope once when an orchestrated run acquires its manifest
  lease. Individual work submissions carry only their own asset identity and never
  rescan the plan. Direct standalone `FavnRunner.run/2` calls preflight their explicit
  work scope before execution.
- `apps/favn_runner/lib/favn_runner/execution_lifecycle.ex` owns runner execution
  lifecycle state, worker/waiter/subscriber monitor bookkeeping, bounded
  completed-execution retention, bounded log/event buffers, and lifecycle
  diagnostics counts. Completed entries retain a deterministic replay fingerprint,
  not the execution package or full work item. Retention enforces per-result,
  per-log/event-buffer, and aggregate completed-execution byte budgets in addition
  to count limits. Workers compact oversized results before sending them to the
  central runner process. Production retention is configured with
  `config :favn_runner, :execution_retention, ...`.
- `apps/favn_runner/lib/favn_runner/manifest_store.ex` owns the immutable pinned
  manifest cache. It is bounded by both entry count and estimated BEAM bytes,
  rejects a manifest larger than its byte budget, and evicts only unleased entries.
  Active runs acquire expiring leases, and execution resolves the handle, selected
  asset, and package relation map in one atomic cache operation. The store fails
  closed when all capacity is protected and exposes active-lease and cache-pressure
  diagnostics and telemetry. Production limits are
  configured with `config :favn_runner, :manifest_cache, ...`.
- `apps/favn_runner/lib/favn_runner/runtime_config_diagnostic.ex` normalizes
  runner runtime-config failures into stable redacted run diagnostics
- `apps/favn_runner/lib/favn_runner/execution_admission.ex` owns runner-local
  active-worker admission and overload diagnostics. Durable queueing remains an
  orchestrator concern; the runner rejects exhausted capacity with a typed,
  retryable `:runner_overloaded` boundary error. Submit and cancel calls are
  bounded and normalize call timeouts into typed runner boundary errors.
- Runner cancellation outcomes distinguish BEAM worker acknowledgement from
  native data-plane certainty. A stopped BEAM worker reports
  `native_status: :native_cancel_unknown` unless an adapter-specific native
  cancellation path proves a stronger outcome.
- `apps/favn_runner/lib/favn_runner/sql/materialization_planner.ex` owns runner SQL
  asset materialization planning and emits shared `%Favn.SQL.WritePlan{}` values
- runner-owned execution modules under `apps/favn_runner/lib/favn/`

Tests:
- `apps/favn_runner/test/`
- `apps/favn_runner/test/sql/materialization_planner_test.exs` covers the explicit
  runner-owned planner namespace and write-plan contract handoff
- `apps/favn_runner/test/sql_runtime_preflight_test.exs` covers planned SQL
  connection runtime config preflight and redacted diagnostics
- App-local tests use manifest-shaped fixtures and fake SQL adapters instead of
  authoring DSL fixtures or concrete runner plugins from sibling apps.
- Connection loader tests declare the authoring app as a test-only dependency so
  local connection fixtures can use the public `Favn.Connection` behaviour.

Use when changing asset execution, runner protocol behavior, cancellation,
timeouts, manifest registration/resolution, plugin config, SQL asset execution,
SQL asset materialization planning, runner production config validation,
runner-owned inspection, or runner-side normalization into shared work/result,
error, and cancellation contracts.

`FavnRunner.ContextBuilder` constructs the typed `Favn.Run.Context`. Static
values are exposed only as `ctx.asset.settings` and `ctx.pipeline.settings`,
submitted values as `ctx.params`, and resolved environment values/secrets as
`ctx.runtime_config`. `RunnerWork` carries the typed pipeline context and
absolute deadline explicitly rather than duplicating them inside metadata.

Runner registration stores immutable pinned manifests only in the bounded runner
manifest cache. Before one
selected SQL asset is admitted, the orchestrator loads that asset's immutable
execution package and attaches it to `%Favn.Contracts.RunnerWork{}`. The runner
verifies both package hash and asset ref before resolving runtime inputs or
opening a SQL session. It has no inline-SQL or compiled-module fallback.
Exact execution-id replay is checked before manifest resolution. An active run's
lease prevents eviction; after release, retained completed work remains replayable
even if the manifest is later evicted.
Manifest acquire, renew, and release are mandatory runner-client callbacks. The
orchestrator rejects clients that cannot uphold this lease contract; there is no
unleased registration fallback.

Consumer and integration packages implement `Favn.Runner.Plugin` from
`favn_core`; they never depend on this internal app merely to extend the runner
lifecycle. The root uses `:rest_for_one` ordering with the extension supervisor
first. A plugin subtree failure therefore cannot leave later runner services
operating without their required lifecycle dependencies.

Plugin state is runner-local and disposable. It may cache credentials, sessions,
pools, rate limits, or other rebuildable operational state, but cannot be a
durable or cross-run correctness channel.

Checked SQL assets are coordinated by `Favn.SQLAsset.Runtime`: target existence,
optional candidate staging, ordered before checks, the write plan, ordered after
checks, and stage cleanup all run inside one admitted adapter transaction.
Contracted assets always stage their candidate. The runtime inspects candidate
columns and applies `%Favn.SQL.ContractValidation{}` before target mutation,
then executes generated data claims through the same check engine as authored
checks. Warnings and no-op writes remain successful; a no-op also records
`quality_status: :warning`. Failures return bounded contract/check metadata so
the worker persists failed-attempt diagnostics.

The SQL renderer binds Favn-owned `@favn_run_id` and
`@favn_run_started_at` from `%Favn.Run.Context{}`. Parameterized exact
row-count claims reuse normal settings/runtime params and are type-checked before
rendering or opening an adapter session; values never become SQL source or error
details.

`FavnRunner.RuntimeInputResolver` owns behaviour-based SQL runtime input
resolution. The public runner boundary can perform a bounded selection-only
phase, returning a typed resolution to the orchestrator without opening a SQL
session. Normal execution requires the orchestrator-persisted pin and validates
that it matches the manifest resolver before rendering. Resolver timeout is 30
seconds and additionally bounded by the node deadline. Cancellation kills
resolver code; failures never open or mutate a connection. The runner does not
write orchestrator storage. Parameters stay out of generic metadata and
sensitive names drive result/error redaction.

Runner failures use `%Favn.Contracts.RunnerError{}`. `retryable?: true` plus
`outcome: :safe_failure` may permit an orchestrator node retry when policy has
an attempt left. Boundary timeouts and unknown write/materialization outcomes
remain terminal; runner code never decides attempt count.

Runner results and errors may also carry bounded
`%Favn.Contracts.ResourceOutcome{}` entries. SQL connection/bootstrap failures
identify the named connection, classification, and whether repeating is proven
safe; successful SQL use emits connection success. The orchestrator alone owns
circuit state and recovery policy. Generic exceptions and unknown-outcome SQL
writes never become inferred resource failures.

DuckDB/ADBC session pooling is default-on for poolable adapters unless disabled
with `pool: [enabled: false]`. It is runner-local and per BEAM. A pooled SQL
session may be checked out by only one asset execution at a time, and the runner
must still honor existing catalog/write concurrency for active work and new
session/bootstrap. SQL client operations are process-affine to the checkout
owner. Same-key fresh session creation may run in parallel up to the selected
finite admission/catalog limit, but the pool does not coordinate across runner
nodes, does not increase catalog/write concurrency, and does not replace finite
DuckLake catalog `write_concurrency`, especially on low-tier Azure PostgreSQL
metadata stores.
SQL asset execution packages carry versioned stable session resource names. The
runner combines those names with rendered catalog requirements and passes both
sets to the SQL client before physical-session creation; it never embeds script
files or resolved secret values in the manifest.
For DuckLake with PostgreSQL metadata, one concurrent DuckLake writer can use
multiple PostgreSQL backend connections; observed deployments used about three,
so size `write_concurrency` with that multiplier and leave operational headroom.
