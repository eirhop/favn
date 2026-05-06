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
- DuckDB bootstrap features such as extension install/load, DuckLake attach, and
  catalog selection

Both DuckDB plugins are supported optional dependencies. Do not add internal
runtime apps directly for either path.

The other umbrella apps are not ordinary user dependencies:

- `favn_authoring` owns authoring implementation behind `favn`.
- `favn_core` owns shared manifests, planning, and runtime contracts.
- `favn_orchestrator` owns the private control plane and is not a public external
  API dependency.
- `favn_runner` owns execution internals and runner plugin dispatch.
- `favn_storage_sqlite` and `favn_storage_postgres` are control-plane storage
  adapters selected by runtime/package tooling.
- `favn_local` owns local lifecycle and packaging implementation behind public
  `mix favn.*` tasks.
- `favn_web` is the web product edge, not a dependency for authored business code.

Before Hex publishing, local private consumer projects may use path dependencies
from one checkout. The supported shape is `favn` plus optional plugins such as
`favn_duckdb_adbc` or `favn_duckdb`, not manually listing internal runtime apps.

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
- supported local commands: `mix favn.init`, `mix favn.doctor`,
  `mix favn.install`, `mix favn.dev`, `mix favn.run`, `mix favn.backfill`,
  `mix favn.reload`, `mix favn.status`, `mix favn.logs`, `mix favn.stop`,
  `mix favn.reset`, and `mix favn.read_doc`

Packaging commands such as `mix favn.build.runner`, `mix favn.build.web`,
`mix favn.build.orchestrator`, and `mix favn.build.single` are public command
entrypoints, but their production artifact behavior is still being hardened and
should follow `docs/production/single_node_contract.md` before being described as
fully production-stable.

## Compatibility-Only Authoring APIs

`Favn.Assets` remains supported for existing compact multi-asset modules, but it
is not the preferred new-project authoring path. New code should prefer
`Favn.Asset` for single assets and `Favn.MultiAsset` when generated or repetitive
multi-asset structure is clearer.

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

- `docs/production/single_node_contract.md` defines the first production runtime
  topology.
- `docs/FEATURES.md` tracks implemented behavior and maturity labels.
- `docs/ROADMAP.md` tracks remaining implementation work to align code, tests,
  examples, and publishing with this boundary.
