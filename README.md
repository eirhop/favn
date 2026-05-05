<p align="center">
  <img src="docs/images/favn-logo-transparent.png" alt="Favn logo" width="220" />
</p>

<h1 align="center">Favn</h1>

<p align="center">
  Define business-oriented data assets in Elixir, model how they depend on each other, and turn them into predictable runs.
</p>

<p align="center">
  <a href="#status"><strong>Status</strong></a>
  ·
  <a href="#what-favn-gives-you"><strong>Features</strong></a>
  ·
  <a href="#core-concepts"><strong>Concepts</strong></a>
  ·
  <a href="#quickstart"><strong>Quickstart</strong></a>
  ·
  <a href="#local-development"><strong>Local Dev</strong></a>
  ·
  <a href="#common-public-api-entry-points"><strong>API</strong></a>
  ·
  <a href="#documentation"><strong>Docs</strong></a>
</p>

Favn is an Elixir library for defining business-oriented data assets, understanding how they depend on each other, and turning those definitions into predictable runs.

It is aimed at teams who want data and workflow logic to live in normal Elixir modules instead of being spread across ad hoc scripts, scheduler config, and undocumented conventions.

## Status

Favn is in private development.

Current release work is focused on hardening the authoring, manifest, local
tooling, and single-node runtime support boundaries for a stable `v1`.

- breaking changes are still allowed before `v1.0`
- `{:favn, ...}` is the primary public dependency for consumer projects
- `{:favn_duckdb, ...}` is optional and only needed when the project executes
  DuckDB-backed SQL assets or uses DuckDB through `Favn.SQLClient`
- storage, orchestrator, runner, local tooling, and web apps are internal runtime
  or product components, not ordinary user dependencies
- local development tooling is available today through `mix favn.init`, `mix favn.doctor`, `mix favn.install`, `mix favn.dev`, `mix favn.run`, `mix favn.backfill`, `mix favn.diagnostics`, `mix favn.reload`, `mix favn.status`, and `mix favn.stop`
- local development startup uses HTTP-level orchestrator readiness checks, validated Distributed Erlang node/cookie inputs, explicit service wrapper pid/log-write failures, structured local API failure diagnostics, and normalized runner RPC dispatch failures at the orchestrator boundary
- the local web UI now includes a run inspector at `/runs`, an asset catalog at `/assets`, and an operational backfill area at `/backfills` for submitting explicit active-manifest pipeline backfills, inspecting parent windows, rerunning failed windows, and browsing coverage-baseline and asset/window state projections
- local development registers one pinned manifest version across runner and orchestrator so scheduled runs execute against the same manifest identity
- run snapshot and run event persistence stores explicit JSON-safe storage records, not BEAM terms or reconstructed exception structs; runner failures are normalized, bounded, and redacted before durable storage
- run-event storage treats exact duplicate writes as idempotent success and rejects duplicate sequences with different event content
- run live updates expose documented global and run-scoped SSE streams with persisted cursors, Last-Event-ID replay, retry hints, heartbeats, and SvelteKit BFF relays that keep orchestrator service tokens server-side
- production mutating orchestrator command APIs require `Idempotency-Key` for run submit/rerun/cancel, manifest activation, backfill submit, and backfill-window rerun; duplicate retries replay the original logical result, conflicting input returns `409`, and SQLite persists only key/request fingerprints plus redacted normalized responses
- the first `v1` production target is documented as a single backend node with SQLite control-plane persistence on durable attached storage and runner-owned DuckDB data-plane execution; Phase 1 runtime config validation covers SQLite storage, orchestrator API/service auth, scheduler, local runner mode, and web-to-orchestrator/public-origin config, and the orchestrator now exposes service-authenticated operator diagnostics for storage/schema readiness, active manifest, scheduler, runner availability, in-flight runs, and recent failed runs; backup automation, full web production startup, and the operator runbook remain follow-up; see `docs/production/single_node_contract.md`
- local documentation lookup is available through `mix favn.read_doc ModuleName` and `mix favn.read_doc ModuleName function_name`
- initial packaging tooling now includes `mix favn.build.runner` for project-local runner artifact output under `.favn/dist/runner/<build_id>/`
- split-target packaging now also includes `mix favn.build.web` and `mix favn.build.orchestrator` with honest metadata-oriented outputs under `.favn/dist/web/<build_id>/` and `.favn/dist/orchestrator/<build_id>/`
- single-node packaging now includes `mix favn.build.single` with a verified project-local backend-only SQLite launcher under `.favn/dist/single/<build_id>/`; it still depends on the installed runtime source root and is not a self-contained or relocatable release artifact (see `OPERATOR_NOTES.md` in each artifact)
- backend bootstrap tooling now includes `mix favn.bootstrap.single`, which verifies an orchestrator service token through `/api/orchestrator/v1/bootstrap/service-token`, validates a manifest JSON file, registers and activates the manifest by default, asks the orchestrator to register that persisted manifest with the local runner, and reports service-auth active-manifest verification status; it does not make browser login/session/audit durable
- operational backfill foundations are implemented in the control plane for resolving ranges, submitting parent/child pipeline backfills, tracking per-window state, exposing private orchestrator HTTP reads/commands, and driving those endpoints from `mix favn.backfill` in local dev; operational backfill does not accept lookback-policy input until concrete runtime semantics exist

## What Favn Gives You

- Elixir DSLs for single assets, multi-assets, SQL assets, namespaces, schedules, pipeline window policies, and pipelines
- explicit dependency modeling between assets
- compile-time manifest generation for authored business code
- deterministic planning from selected targets and dependency rules
- pipeline definitions for scheduled or operator-triggered runs
- local runtime workflow support for authoring, safe landed-data inspection, and orchestration
- SQL-aware asset authoring with reusable SQL definitions and relation references
- public SQL client access for named Favn connections via `Favn.SQLClient`
- DuckDB connection bootstrap for DuckLake sessions, including extension install/load, Azure credential-chain secrets, DuckLake attach, and catalog selection

## Core Concepts

### Assets

Assets are the units of business work in Favn. An asset can be written as normal Elixir (`use Favn.Asset`) or as a SQL-backed asset (`use Favn.SQLAsset`).

### Pipelines

Pipelines select one or more target assets and describe how they should run, for example whether dependencies should be included and whether the pipeline should have a schedule.
Cron schedules support both standard 5-field expressions and 6-field expressions with a leading seconds field, so production schedules can be defined down to seconds when needed.
When `missed: :all` is used, scheduler catch-up is capped per schedule entry per tick to avoid unbounded backlog submission for high-frequency schedules. The orchestrator default cap is 1,000 occurrences per schedule entry per tick and can be adjusted with `config :favn_orchestrator, :scheduler, max_missed_all_occurrences: ...`.

### Manifests

Favn compiles your authored modules into a manifest so planning and runtime behavior can operate from a stable, explicit description of the graph.

## Quickstart

### 1. Add the dependency

Favn is not published on Hex yet. For private local development with the Favn
monorepo checked out next to your project, use path dependencies:

```elixir
def deps do
  [
    {:favn, path: "../favn/apps/favn"}
  ]
end
```

If your project executes DuckDB-backed SQL assets directly, add the DuckDB
plugin from the same local checkout:

```elixir
def deps do
  [
    {:favn, path: "../favn/apps/favn"},
    {:favn_duckdb, path: "../favn/apps/favn_duckdb"}
  ]
end
```

Do not add internal runtime apps as ordinary consumer dependencies. Storage
adapters such as `favn_storage_sqlite` and `favn_storage_postgres`, runtime apps
such as `favn_orchestrator`, `favn_runner`, and `favn_local`, and the web app are
owned by Favn's runtime/package tooling rather than by authored business code.
Local SQLite control-plane storage is selected with `config :favn, :local` or
`mix favn.dev --sqlite`, not by adding `:favn_storage_sqlite` to the consumer
project.

Multiple `git` dependencies with different `subdir` values from this monorepo
are not the supported plugin-consumption model before Hex packaging because Mix
checks them out as separate dependency projects. For real external consumption,
Favn packages will be published as normal package dependencies.
If a private git/subdir setup currently needs direct `override: true` entries for
internal apps to satisfy Mix, treat that as a temporary private-development
workaround only. It is not the intended consumer dependency model.

### 2. Define an asset

```elixir
defmodule MyApp.Warehouse do
  use Favn.Namespace, relation: [connection: :warehouse]
end

defmodule MyApp.Warehouse.Raw do
  use Favn.Namespace, relation: [catalog: "raw"]
end

defmodule MyApp.Warehouse.Raw.Orders do
  @moduledoc """
  Raw commerce orders as received from the source platform.

  One row represents one source order. Cancelled orders are retained so
  downstream models can decide how to treat them.
  """

  use Favn.Asset

  @doc "Fetch, normalize, and write raw orders."
  @meta owner: "data-platform", category: :sales, tags: [:raw]
  source_config :source_system,
    segment_id: env!("SOURCE_SYSTEM_SEGMENT_ID"),
    token: secret_env!("SOURCE_SYSTEM_TOKEN")
  @relation true
  def asset(ctx) do
    _segment_id = ctx.config.source_system.segment_id
    :ok
  end
end
```

Namespace defaults are inherited from parent modules. The recommended default is
`warehouse.ex` for the connection namespace, `warehouse/raw.ex` or
`warehouse/mart.ex` for layer catalogs, and leaf files for assets. `@relation
true` is the normal leaf-module path, while `@relation [name: "..."]` overrides
only the relation name. SQL asset namespace inheritance is finalized during
explicit asset/manifest compilation, so parent namespace modules do not need to
compile before child SQL asset modules in the same parallel compiler batch.
Use your own layer names, such as bronze/silver/gold or raw/intermediate/mart,
and use schemas instead of catalogs if that is your platform convention.
Keep asset-specific logic near the asset; move code to `integrations/` or `sql/`
only when it is transport-specific or genuinely reusable.

Assets and generated multi-assets can declare required runtime configuration with
`source_config/2`, `env!/1`, and `secret_env!/1`. Manifests record the required
environment keys and secret flags, but never embed resolved runtime values. The
runner resolves the values before asset execution and exposes them through
`ctx.config`, failing the run early with diagnostics such as
`missing_env SOURCE_SYSTEM_TOKEN` when a required value is absent.

For source-system raw landing assets, keep the source client outside the asset,
read source IDs/tokens through `ctx.config`, write raw rows through
`Favn.SQLClient`, and return structured metadata such as row counts, mode,
relation, load timestamp, and hashed source identity. The standalone tutorial in
`examples/basic-workflow-tutorial` shows this pattern with a full-refresh raw
orders asset followed by SQL transformations.

### 3. Define a downstream SQL asset

```elixir
defmodule MyApp.Warehouse.Mart do
  use Favn.Namespace, relation: [catalog: "mart"]
end

defmodule MyApp.Warehouse.Mart.OrderSummary do
  @moduledoc """
  Sales order mart used by business reporting.

  One row represents one reportable order after raw source fields have been
  normalized for analytics.
  """

  use Favn.SQLAsset

  @meta owner: "analytics", category: :sales, tags: [:mart]
  @depends MyApp.Warehouse.Raw.Orders
  @materialized :view

  query do
    ~SQL"""
    select *
    from raw.orders
    """
  end
end
```

Relation-style SQL references are the primary authoring path. Two-part SQL names
are read as `schema.table`; three-part names are read as
`catalog.schema.table`. When a reference resolves to an owned asset relation on
the same connection, Favn infers the dependency automatically. Use `@depends`
when the dependency is not visible in SQL, cannot be resolved from owned
relations, or the project intentionally uses catalog-only layer names.
DuckDB-backed SQL materialization creates the owned target schema when needed
before creating the table or view. DuckDB appender materialization treats a
successful appender close as consuming the handle; if close fails, the handle
remains retryable or explicitly releasable, and adapter-owned materialization
releases it as part of failure cleanup. Runner-side SQL asset materialization
planning emits the shared `%Favn.SQL.WritePlan{}` adapter contract consumed by
SQL runtime adapters.
For longer queries, place the SQL file next to the SQL asset module, for example
`warehouse/mart/order_summary.ex` plus `warehouse/mart/order_summary.sql`, and
use `query file: "order_summary.sql"`.

### 4. Define a pipeline

```elixir
defmodule MyApp.Pipelines.DailySales do
  use Favn.Pipeline

  pipeline :daily_sales do
    asset MyApp.Warehouse.Mart.OrderSummary
    deps :all
    schedule cron: "0 2 * * *", timezone: "Etc/UTC"
    window :daily
  end
end
```

Windowed pipelines support `:hourly`, `:daily`, `:monthly`, and `:yearly`
policies. Manual local runs pass the concrete requested window, while scheduled
windowed runs resolve the previous complete period in the schedule timezone.
Invalid scheduled window policy data is isolated to that scheduler entry instead
of crashing the scheduler runtime. Pipelines without a `window` clause run
without an anchor window.

Operational backfill foundations can resolve explicit or relative ranges into
the same concrete window anchors and submit one child pipeline run per resolved
window through the orchestrator. Local development can submit explicit
operational backfills and inspect their control-plane state with
`mix favn.backfill` or the web operator flow at `/backfills`.

### 5. Configure authored modules

```elixir
import Config

config :favn,
  asset_modules: [
    MyApp.Warehouse.Raw.Orders,
    MyApp.Warehouse.Mart.OrderSummary
  ],
  pipeline_modules: [
    MyApp.Pipelines.DailySales
  ]
```

### 6. Inspect and compile from IEx

```elixir
{:ok, assets} = Favn.list_assets()
{:ok, pipeline} = Favn.get_pipeline(MyApp.Pipelines.DailySales)
{:ok, manifest} = Favn.generate_manifest()
```

### 7. Query a configured SQL connection from Elixir

```elixir
{:ok, session} = Favn.SQLClient.connect(:warehouse)
{:ok, result} = Favn.SQLClient.query(session, "select 1")
:ok = Favn.SQLClient.disconnect(session)
```

For this flow, configure `:connection_modules` and runtime `:connections` under
`config :favn` using the `Favn.Connection` contract.

Connection runtime values can also use `Favn.RuntimeConfig.Ref.env!/1` and
`Favn.RuntimeConfig.Ref.secret_env!/1` when values should be resolved from the
runner environment just before adapter connection.

DuckDB local-file connections are serialized by default inside the SQL runtime so
local sessions and materializations do not run unsafe concurrent catalog writes
against the same database file. Backends that support parallel writes can opt out
with `write_concurrency: :unlimited` in their runtime connection config, while
local DuckDB files can keep the default or set `write_concurrency: 1` explicitly.
Production local DuckDB database paths should be explicit, durable attached-storage
paths owned by the runner/plugin data plane. Add
`Favn.SQL.Adapter.DuckDB.production_storage_schema_fields/0` to production DuckDB
connection schemas to reject `:memory:`, relative paths, missing parents, and
unwritable parents before opening DuckDB. SQLite control-plane backups do not
include those DuckDB files.

DuckDB connections can also declare `duckdb_bootstrap` runtime config when a
session needs setup before SQLClient or SQL asset execution. This is the
recommended path for local DuckLake dogfooding with Azure Data Lake Storage and a
PostgreSQL metadata catalog. Add `Favn.SQL.Adapter.DuckDB.bootstrap_schema_field/0`
to the connection module schema, then configure extension install/load, Azure
credential-chain secret creation, DuckLake attach, and `USE` under the named
connection. Bootstrap extension names are allow-listed by the adapter today to
`ducklake`, `postgres`, and `azure`; secret runtime refs are resolved on the
runner side and redacted from diagnostics. DuckDB worker unavailability,
worker-call timeouts, bootstrap failures, materialization failures, and appender
failures are normalized into structured SQL errors suitable for logs, API/UI
payloads, and run diagnostics without exposing configured secrets. Worker-call
timeouts are reported as unknown-outcome failures rather than blindly retryable
errors, because the DuckDB operation may still be running in the worker.

## Local Development

Favn includes a local developer loop for running the current project with the Favn runtime stack.

```bash
mix favn.init --duckdb --sample
mix deps.get
mix favn.doctor
mix favn.install
mix favn.dev
mix favn.run MyApp.Pipelines.DailySales --window day:2026-04-27 --timezone Europe/Oslo
mix favn.backfill submit MyApp.Pipelines.DailySales --from 2026-04-01 --to 2026-04-07 --kind day
mix favn.backfill windows RUN_ID --limit 100 --offset 0
mix favn.backfill repair --pipeline-module MyApp.Pipelines.DailySales --apply
mix favn.logs
mix favn.status
mix favn.reload
mix favn.stop
mix favn.reset
mix favn.read_doc Favn
mix favn.read_doc Favn generate_manifest
```

These supported `mix favn.*` commands are the stable public entrypoints for
local iteration.

Security-sensitive Elixir changes should also run the repo-local audit tooling:
`mix deps.audit` for known dependency vulnerabilities, `mix hex.audit` when Hex
package freshness or metadata is relevant, and Sobelow for HTTP/Plug-facing code.
Treat findings as review input: fix high/critical issues and document any
intentional false positives.

For a fresh standalone Mix consumer project with local path dependencies back to
this checkout, `mix favn.init --duckdb --sample` generates the first dogfooding
path: a DuckDB connection module, raw and gold namespace modules, one Elixir raw
load asset, one SQL business output asset, a `deps(:all)` pipeline, local Favn
config, and `.env.example` web-login credentials. The task also adds a local
`favn_duckdb` path dependency when it can recognize the project's `defp deps/0`
shape. Rerun `mix deps.get`, then use `mix favn.doctor` before starting the
stack.

The generated pipeline is `MyApp.Pipelines.LocalSmoke` for a project whose OTP
app is `:my_app`:

```bash
mix favn.install
mix favn.dev
mix favn.run MyApp.Pipelines.LocalSmoke --wait
```

After the run, inspect `/runs` and `/assets` in the local web UI. Asset detail
pages show latest materialization metadata when available and can load a safe
read-only preview for manifest-owned SQL relations: columns, row count, and up to
20 sample rows. The generated DuckDB output can also be queried from the
consumer project:

```elixir
{:ok, session} = Favn.SQLClient.connect(:warehouse)
{:ok, result} = Favn.SQLClient.query(session, "select * from gold.order_summary")
:ok = Favn.SQLClient.disconnect(session)
```

`mix favn.dev` starts with the local scheduler disabled by default. This keeps
one-time local ETL and DuckDB-backed dogfooding on the manual path unless you
explicitly opt into active schedules. Use `mix favn.run PipelineModule` for the
normal local execution loop, and use `mix favn.dev --scheduler` only when you
want scheduled pipelines to run locally. `mix favn.dev --no-scheduler` is
available when you want to make the disabled setting explicit or override local
config.

For a consumer-style authoring and DuckDB execution tutorial, see
`examples/basic-workflow-tutorial`. It lives outside the umbrella apps, uses
local path dependencies back to `apps/favn` and `apps/favn_duckdb`, and has its
own compile/test workflow. The embedded tutorial also exercises the
`mix favn.install` and `mix favn.dev` local-tooling loop from a consumer-style
project. Its raw orders asset is the canonical source-system landing example:
it resolves a source segment and token through `ctx.config`, calls a small source
client, lands JSON rows into DuckDB through `Favn.SQLClient`, and returns
structured run metadata for inspection.

`mix favn.install` resolves and materializes a runtime workspace under
`.favn/install/runtime_root`. The install fingerprint includes a deterministic
hash of the copied runtime source tree, excluding generated dependency/build
directories, so source-only Favn updates refresh the installed runtime. `mix
favn.dev` validates that fingerprint and compiles the installed runtime
workspace before startup so live runner/orchestrator processes do not boot stale
internal runtime beams. During foreground startup it prints concise terminal
progress lines before slow phases such as runtime compile, project compile,
service startup, manifest publication, and HTTP readiness checks. `--root-dir`
remains an install/runtime-source override for split-root workflows.

Local tooling HTTP calls are plain HTTP loopback calls to Favn-managed local
services. They intentionally do not support remote or HTTPS URLs in the local
developer loop. The local stack enables an explicit trusted local-dev context
while the web UI and orchestrator API are bound to loopback, so `mix favn.dev`,
the bundled local web UI, `mix favn.run`, and local backfill commands do not
require usernames or passwords and do not persist local service tokens,
passwords, RPC cookies, or session secrets under `.favn/`.

Production/server startup is separate from local dev. When production runtime
configuration is enabled, the orchestrator requires explicit
`FAVN_ORCHESTRATOR_BOOTSTRAP_USERNAME` and
`FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD` values in the runtime environment and
refuses startup if they are missing.
Password login now uses Argon2id password hashes, returns an opaque session token
once, and applies an explicit absolute session TTL. Passwords must be nonblank,
15 to 1,024 characters, and are not subject to arbitrary composition rules.
Clients must forward the session token to the private orchestrator API as
`x-favn-session-token`. SQLite storage keeps only encoded Argon2 password hashes,
deterministic session-token hashes, revocation timestamps, actor metadata, and
redacted audit entries, with auth roles, credential records, and audit entries
stored as explicit JSON-safe DTO records rather than generic BEAM-term payloads;
password changes revoke active sessions for that actor.

Local numeric config values, including dev ports and Postgres port/pool size,
are accepted only as positive integers or strings that contain exactly a
positive integer after trimming whitespace. Malformed strings use the documented
local defaults.

`mix favn.run PipelineModule` submits a manifest-scoped pipeline run to the
currently running local stack through the loopback-only local-dev context, so
tutorial and local smoke runs do not require hand-written private orchestrator
API requests or local passwords. This manual run path is the recommended default
for one-time local ETL. Each invocation uses a fresh idempotency key by default;
pass `--idempotency-key KEY` only when you intentionally want deterministic
orchestrator replay behavior for a local submission.

`mix favn.backfill` exposes the local operational-backfill workflow for running
local stacks. Use `submit` for explicit `--from`/`--to`/`--kind` pipeline ranges,
`windows RUN_ID` to inspect child windows, `coverage-baselines` and
`asset-window-states` to inspect projected backfill state, and `rerun-window
RUN_ID --window-key KEY` for failed window reruns. Use `repair` to dry-run or,
with `--apply`, rebuild derived coverage-baseline, backfill-window, and latest
asset/window projections from authoritative run snapshots after projection drift
or read-model deletion. `repair --backfill-run-id RUN_ID` rebuilds that parent
window ledger only; use `--pipeline-module` or no scope when latest
asset/window state must also be recomputed. Operational backfill submit does not accept
lookback-policy input; asset window lookback remains part of normal windowed
execution only.

The local web UI exposes the same operator workflow through `/backfills`,
including active-manifest pipeline selection, explicit range submission,
optional coverage-baseline selection, parent backfill detail pages with child
window rows, one-window failed reruns, `/backfills/coverage-baselines`, and
`/assets/window-states`.

Backfill read commands are bounded. They default to `--limit 100 --offset 0`, reject
`--limit` values above `500`, only accept known status values and manifest-owned
pipeline or asset filters, and print a next-page hint when more rows are available.

For `submit`, `--wait-timeout-ms` controls local CLI polling only, while
`--run-timeout-ms` controls the child run execution timeout sent to the
orchestrator.

The local runner receives only the explicitly supported consumer `:favn` config
needed for local execution: `:connection_modules`, `:connections`,
`:runner_plugins`, and `:duckdb_in_process_client`. This transport is local-dev
plumbing only; it uses a tagged payload rather than arbitrary app-env forwarding,
maps top-level keys explicitly, validates transported module/local atoms before
recreating them, normalizes relative connection database paths from the consumer
project root, and redacts connection/plugin secrets from diagnostics.

If you are upgrading from earlier pre-closeout local SQL storage state, run
`mix favn.reset` once so local persisted payloads are recreated in the current
canonical format.

Pre-closeout SQL payload compatibility is intentionally not supported. Existing
SQLite/Postgres rows that were persisted as BEAM term blobs must be reset or
recreated before running with the closeout adapters.

Pre-DTO SQLite auth rows are also intentionally unsupported. If a private-dev
database contains auth roles, credentials, or audit entries persisted before the
explicit auth DTO boundary, reset or recreate that SQLite control-plane state.

### favn_local configuration

`favn_local` reads local tooling config from `config :favn, :local` (with
`config :favn, :dev` still supported for backward compatibility).

```elixir
config :favn, :local,
  storage: :memory,
  scheduler: false,
  sqlite_path: ".favn/data/orchestrator.sqlite3",
  postgres: [
    hostname: "127.0.0.1",
    port: 5432,
    username: "postgres",
    password: "postgres",
    database: "favn",
    ssl: false,
    pool_size: 10
  ]
```

Storage modes:

- `mix favn.dev` uses configured storage (default `:memory`)
- `mix favn.dev --sqlite` forces SQLite
- `mix favn.dev --postgres` forces Postgres
- `mix favn.dev --scheduler` enables local scheduled runs
- `mix favn.dev --no-scheduler` disables local scheduled runs and overrides config

Do not add `:favn_storage_sqlite` to a consumer `mix.exs` for local SQLite
control-plane storage. The SQLite storage adapter is owned by Favn's local
runtime/package setup under `.favn/`; consumer projects select it through the
local config above or `mix favn.dev --sqlite`.

`mix favn.build.single` emits a verified project-local backend-only SQLite launcher under
`.favn/dist/single/<build_id>/`. Configure it by copying
`env/backend.env.example` to `env/backend.env` or setting `FAVN_ENV_FILE`, then
use the generated `bin/start` and `bin/stop` scripts. The launcher starts the
runner, SQLite adapter, and orchestrator in one backend BEAM runtime. It remains
project-local and non-relocatable because it depends on the installed runtime
source root. Postgres production mode is not included, and the web service is
still deployed as a separate explicit process.

Production orchestrator service tokens are configured as comma-separated
`service_identity:token` entries in `FAVN_ORCHESTRATOR_API_SERVICE_TOKENS`.
Each token must be at least 32 characters and must not look like a placeholder,
example, password, secret, test value, todo, or token-label string; identities
must be nonblank and unique. Rotation is supported by configuring multiple active
identities. The orchestrator stores only token hashes from production runtime
config and records the matched `service_identity` in audit logs, diagnostics, and
bootstrap auth responses without exposing raw token values.

`web/favn_web` has an explicit SvelteKit Node production path. Build it with
`npm run build` and start it with `npm run start` from `web/favn_web`; the start
script runs `node build`. Required production env is
`FAVN_WEB_ORCHESTRATOR_BASE_URL`, `FAVN_WEB_ORCHESTRATOR_SERVICE_TOKEN`, and
`FAVN_WEB_PUBLIC_ORIGIN`, with optional `FAVN_WEB_ORCHESTRATOR_TIMEOUT_MS`
defaulting to `2000`. The web process stores raw orchestrator session tokens in a
process-local server-side web-session store and gives browsers only opaque
`__Host-favn_web_session` ids. It exposes `/api/web/v1/health/live` without an orchestrator check and
`/api/web/v1/health/ready` with a bounded orchestrator readiness check. See
`docs/production/web_service.md` and `web/favn_web/README.md`.

`mix favn.bootstrap.single` bootstraps the backend control-plane side of the
single-node shape through orchestrator APIs. Required inputs can be passed as
`--manifest`, `--orchestrator-url`, and `--service-token`, or by the supported
environment defaults documented by the task. Bootstrap uses
`FAVN_BOOTSTRAP_ORCHESTRATOR_SERVICE_TOKEN` or
`FAVN_WEB_ORCHESTRATOR_SERVICE_TOKEN` for service auth, with
`FAVN_ORCHESTRATOR_SERVICE_TOKEN` accepted only as a legacy fallback.
The command verifies service-token auth, reads and verifies the manifest JSON,
registers the manifest, activates it by default, and calls
`/api/orchestrator/v1/manifests/:manifest_version_id/runner/register` so the
orchestrator registers the persisted manifest with the local runner. Repeating
the command with the same manifest and runner registration is safe and
repeatable, and the command prints manifest registration, runner registration,
and active-manifest verification status. Active-manifest verification uses the
service-auth-only `/api/orchestrator/v1/bootstrap/active-manifest` endpoint. The
single-node integration path now verifies first-admin login, manifest-pinned run
submission, run history, auth/session state, diagnostics, and restart survival
against the generated backend artifact with fresh SQLite storage.

Storage adapter startup reports recoverable configuration failures as
`{:error, reason}`. Scheduler state keys are exact across built-in adapters:
`{pipeline_module, nil}` addresses the nil schedule id and does not fall back to
the latest concrete schedule id.

SQLite adapter startup validates the configured database path and can classify
the schema as empty, ready, missing, upgrade-required, newer than the running
release, or inconsistent. Local/default SQLite startup still auto-runs
migrations, while manual startup can reject non-ready existing databases and can
initialize empty databases when explicitly enabled.
SQLite is also the first durable backend for orchestrator auth/session/audit
state, storing auth roles, credential records, and audit entries as explicit
JSON-safe DTO records; Postgres auth persistence remains deferred.

Detailed operator diagnostics are available through the private orchestrator
HTTP endpoint `GET /api/orchestrator/v1/diagnostics` with service auth and the
local `mix favn.diagnostics` wrapper. Each check returns `check`, `status`,
`summary`, `details`, and optional `reason`, and all diagnostics are redacted
before being returned, logged, or passed to the optional
`:favn_orchestrator, :metrics_hook` callback. Runner diagnostics include
redacted data-plane connection summaries when the runner can expose them through
the adapter boundary.

### favn_local implementation notes

- public `mix favn.*` tasks are owned by `apps/favn`
- public `mix favn.*` tasks reject unsupported options and unexpected positional
  arguments before performing work
- implementation is owned by `apps/favn_local`
- `mix favn.build.runner` is rooted in the current Mix project; `--root-dir`
  must match the current project root
- runner artifacts include the pinned manifest and compiled beams from the
  current project app so helper modules and connection modules are available at
  execution time
- install now materializes a runnable runtime workspace under
  `.favn/install/runtime_root`, records runtime metadata in
  `.favn/install/runtime.json`, and caches npm data under
  `.favn/install/cache/npm`

## Common Public API Entry Points

The stable `v1` API is intended to cover authoring, manifest compilation and
registration inputs, `Favn.SQLClient`, and supported local commands. Runtime
helper functions exposed on `Favn` are runtime-dependent conveniences and are not
part of the stable `v1` contract unless they are documented here or in
`docs/production/public_api_boundary.md`.

- `Favn.list_assets/0,1`
- `Favn.asset_module?/1`
- `Favn.get_asset/1`
- `Favn.get_pipeline/1`
- `Favn.resolve_pipeline/2`
- `Favn.generate_manifest/0,1`
- `Favn.build_manifest/0,1`
- `Favn.plan_asset_run/2`
- `Favn.SQLClient.connect/1,2`, `query/2,3`, `execute/2,3`, `transaction/2,3`,
  `capabilities/1`, `relation/2`, `columns/2`, `with_connection/2,3`, and
  `disconnect/1`

`Favn.Assets` remains available as a compatibility-only authoring DSL for compact
multi-asset modules. Prefer `Favn.Asset` for new single assets and
`Favn.MultiAsset` for generated or repetitive multi-asset modules. Multi-assets
support module-level `source_config/2`; each generated asset carries the same
runtime config declarations and shared runtime code reads resolved values from
`ctx.config`. At runtime, Favn rehydrates `ctx.asset.config.rest` structural keys
and known Favn enum fields for idiomatic access. Arbitrary adapter-specific
`rest.extra` payload keys and static `rest.params` entries remain
manifest/JSON-shaped unless Favn explicitly supports that key.

## Documentation

- `docs/FEATURES.md` tracks the implemented feature set today
- `docs/ROADMAP.md` tracks planned next work and later ideas
- `docs/production/public_api_boundary.md` defines the intended package and
  stable public API boundary for `v1`
- `docs/production/single_node_contract.md` defines the first `v1` production deployment contract
- `docs/structure/` maps current ownership, code layout, and test layout by app
- `examples/basic-workflow-tutorial` is the first end-to-end tutorial project

## AI Doc Entry Point

Favn includes a compiled-doc reader intended for AI-assisted workflows.

Use Favn when you are defining assets and pipelines in Elixir, compiling them
into a manifest, planning runs, or using the local dev/runtime tooling.

Read `Favn.AI` for the task map and the next module to inspect.

```bash
mix favn.read_doc Favn.AI
```

Common follow-up reads:

```bash
mix favn.read_doc Favn generate_manifest
mix favn.read_doc Favn.Asset
mix favn.read_doc Favn.SQLAsset
mix favn.read_doc Favn.Pipeline
mix favn.read_doc Favn.Backfill.RangeRequest
mix favn.read_doc Favn.Dev
```

Suggested section for a consumer project's `AGENTS.md`:

```md
## Favn

Favn is used to define business-oriented assets and pipelines in Elixir,
compile them into a manifest, and run or inspect them locally.

Before guessing about Favn APIs, read `mix favn.read_doc Favn.AI` and follow
the module pointers there. Prefer the recommended consumer shape unless this
project documents a stronger local convention.
```

## Current Direction

Current release work is focused on product hardening, explicit support
boundaries, and operator/developer experience. The user-facing goal remains:
define assets and pipelines in Elixir, compile them into an explicit graph, and
run them with predictable behavior.
