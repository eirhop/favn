# favn_runner

Purpose: execution runtime for pinned manifest work, runner server, worker
execution, plugin loading, runtime context construction, and safe relation
inspection.

Code:
- `apps/favn_runner/lib/favn_runner.ex`
- `apps/favn_runner/lib/favn_runner/`
- `apps/favn_runner/lib/favn_runner/production_runtime_config.ex` owns
  runner-side production env validation for the first local single-node setup
- `apps/favn_runner/lib/favn_runner/sql_runtime_preflight.ex` validates planned SQL
  connection runtime config from explicit runner work planned scope before worker
  execution begins
- `apps/favn_runner/lib/favn_runner/execution_lifecycle.ex` owns runner execution
  lifecycle state, worker/waiter/subscriber monitor bookkeeping, bounded
  completed-execution retention, bounded log/event buffers, and lifecycle
  diagnostics counts. Production retention is configured with
  `config :favn_runner, :execution_retention, ...`.
- `apps/favn_runner/lib/favn_runner/runtime_config_diagnostic.ex` normalizes
  runner runtime-config failures into stable redacted run diagnostics
- `apps/favn_runner/lib/favn_runner/execution_admission.ex` owns runner-local
  active-worker admission, bounded submit queue configuration, queue timeout,
  and overload diagnostics. The runner either queues within `max_queue_size` or
  rejects exhausted capacity with a typed retryable `:runner_overloaded` boundary
  error instead of growing unbounded worker processes. Submit and cancel calls
  are bounded and normalize call timeouts into typed runner boundary errors.
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

Checked SQL assets are coordinated by `Favn.SQLAsset.Runtime`: target existence,
optional candidate staging, ordered before checks, the write plan, ordered after
checks, and stage cleanup all run inside one admitted adapter transaction.
Warnings and no-op writes remain successful; failures return bounded check
metadata so the worker persists failed-attempt diagnostics.

`FavnRunner.RuntimeInputResolver` owns behaviour-based SQL runtime input
execution. The worker first builds the normal final `Favn.Run.Context`; the SQL
runtime finalizes any incremental window, resolves inputs in an owned child
process, validates budgets/types/collisions, and merges parameters before the
first render or SQL session. Resolver timeout is 30 seconds and additionally
bounded by the node deadline. Cancellation kills resolver code, failures never
open or mutate a connection, and only module, identity, JSON-safe metadata, and
duration enter attempt metadata. Parameter payloads remain runner-local and
sensitive names drive result/error redaction.

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
For DuckLake with PostgreSQL metadata, one concurrent DuckLake writer can use
multiple PostgreSQL backend connections; observed deployments used about three,
so size `write_concurrency` with that multiplier and leave operational headroom.
