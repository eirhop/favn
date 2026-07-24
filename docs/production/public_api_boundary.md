# Public API And Package Boundary

This document records the intended `v1` public API and package boundary. It is a
documentation contract only; runtime API moves, dependency graph changes, and
module-level enforcement remain separate implementation work.

## Package Boundary

Consumer projects should treat `favn` as the primary public dependency.

Use `favn` for:

- asset, SQL asset, source, namespace, connection, schedule, pipeline, and window
  authoring
- manifest compilation, serialization, compatibility checks, and manifest-version
  inputs
- `Favn.SQLClient` access to named Favn connections from authored Elixir code
- supported `mix favn.*` local commands

Add `favn_duckdb` when the project needs bundled local/in-memory DuckDB
execution, or `favn_duckdb_adbc` when the project needs ADBC-backed DuckDB
execution with explicit shared-library/driver control:

- DuckDB-backed SQL asset materialization
- DuckDB named connections used through `Favn.SQLClient`
- DuckDB bootstrap features such as extension load, keyed DuckLake/DuckDB
  attaches, catalog selection, and catalog-level write admission

Both DuckDB plugins are supported optional dependencies. Do not add internal
runtime apps directly for either path.

Add `favn_azure` when runner code or DuckDB session setup needs shared Azure CLI
or managed-identity tokens. Its supported public surface is
`Favn.Azure.RunnerPlugin`, `Favn.Azure.Credentials`, and the token/error/request
types returned by that API. It uses the public runner lifecycle and does not
make `favn_runner` a consumer dependency.

The other umbrella apps are not ordinary user dependencies:

- `favn_authoring` owns authoring implementation behind `favn`.
- `favn_core` owns shared manifests, planning, and runtime contracts.
- `favn_orchestrator` owns the private control plane and is not a public external
  API dependency.
- `favn_runner` owns execution internals and runner plugin dispatch.
- `favn_storage_postgres` is the internal control-plane persistence
  implementation selected by runtime/package tooling.
- `favn_local` owns local lifecycle and packaging implementation behind public
  `mix favn.*` tasks.
- `favn_view` is the Phoenix/LiveView UI boundary app, not a dependency for authored business code.

Before Hex publishing, local private consumer projects may use path dependencies
from one checkout. The supported shape is `favn` plus optional plugins such as
`favn_duckdb_adbc`, `favn_duckdb`, or `favn_azure`, not manually listing
internal runtime apps.

## Stable V1 API Focus

The stable `v1` API should focus on the parts users build authored projects on:

- authoring modules and macros: `Favn.Asset`, `Favn.MultiAsset`, `Favn.SQLAsset`,
  `Favn.Source`, `Favn.Namespace`, `Favn.Connection`, `Favn.Pipeline`,
  `Favn.Triggers.Schedules`, and `Favn.Window`
- manifest compilation and registration inputs exposed through `Favn` and the
  documented manifest modules
- tooling and planning helpers used before submitting local work, such as
  `Favn.asset_module?/1` and `Favn.plan_asset_run/2`
- `Favn.SQLClient` connect, query, execute, transaction, capabilities, relation,
  columns, with-connection, and disconnect functions against named connections
- `Favn.Runner.Plugin` and `Favn.Runner.SupervisedChildren` for consumer-owned
  runner lifecycle services, plus `Favn.RuntimeValue` for integration boundaries
  that explicitly document deferred-value support
- supported local commands: `mix favn.init`, `mix favn.doctor`,
  `mix favn.dev`, `mix favn.run`, `mix favn.backfill`,
  `mix favn.runs`, `mix favn.reload`,
  `mix favn.inspect`, `mix favn.query`, `mix favn.diagnostics`, `mix favn.stop`,
  and `mix favn.read_doc`

`mix favn.inspect` and `mix favn.query` are direct local command boundaries: they
use the caller's environment and start only the SQL runtime before connecting.
They do not start the consumer app or configured plugins.

The public deployment command surface is `mix favn.init`,
`mix favn.build.manifest`, `mix favn.publish`, and `mix favn.activate`. Favn
publishes the control-plane image; the customer owns and publishes the runner
image. Local tooling does not invoke deployment Dockerfiles.
The supported production topology and artifact ownership are documented in
[`deployment_topology.md`](deployment_topology.md) and
[`runner_releases.md`](runner_releases.md).

## Authoring APIs

Use `Favn.Asset`, `Favn.MultiAsset`, `Favn.SQLAsset`, or `Favn.Source`.
Non-secret static values use the common `settings` declaration and custom DSL
declarations do not use `@`. The former `Favn.Assets` compatibility DSL is
removed before v1.

## Runtime-Dependent Helpers

Some functions exposed on `Favn` are thin runtime delegation helpers. They depend
on runtime apps such as the orchestrator, runner, or scheduler being available
and may return `:runtime_not_available` when those apps are absent.

Those helpers are internal/runtime-dependent conveniences, not stable `v1` API.
Examples include runtime calls such as pipeline submission, run lookup,
scheduler reload/tick/list helpers, and other functions whose behavior depends
on a live runtime process rather than authored project compilation.

For `v1`, ordinary user code should prefer documented authoring APIs,
`Favn.SQLClient`, and supported `mix favn.*` commands. Runtime helper placement
can be tightened later without treating the current `Favn` helpers as a stable
external contract.

## Related Contracts

- `docs/architecture/postgresql-control-plane-storage-v2.md` defines production
  persistence and the multi-node runtime foundation.
- `docs/production/README.md` routes the PostgreSQL-backed production topology,
  image, environment, and operator contracts.
- `docs/FEATURES.md` tracks implemented behavior and maturity labels.
- `docs/ROADMAP.md` tracks remaining implementation work to align code, tests,
  examples, and publishing with this boundary.
