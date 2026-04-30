# Phase 7 DuckDB Runner Plan

## Status

Implemented on branch `feature/phase-7-duckdb-runner`.

Phase 6 intentionally stopped at durable orchestrator persistence and left three SQL/plugin follow-ups for this slice:

- SQL assets still do not execute from pinned manifest payloads in `favn_runner`
- DuckDB still lives inside `favn_runner` instead of the dedicated `favn_duckdb` plugin app
- `apps/favn/lib` still carries temporary runtime seams created during the Phase 2 -> Phase 4 ownership handoff

Phase 7 should close those together as one bounded runner/plugin refactor instead of scattering them across smaller opportunistic cleanups.

## Recommendation Summary

Phase 7 should make seven architectural moves together:

1. Carry canonical SQL execution payload in `favn_core` manifest data so pinned manifests contain everything the runner needs to render and materialize SQL assets.
2. Refactor runner SQL execution around manifest-backed runtime definitions rather than compiled module definitions.
3. Move DuckDB code and the `duckdbex` dependency out of `favn_runner` and into `favn_duckdb` as the first real runner plugin.
4. Add a minimal DuckDB placement seam with exactly two runtime modes: `:in_process` and `:separate_process`.
5. Generalize plugin loading/configuration enough that DuckDB is the first SQL execution plugin, not a special case in `favn`.
6. Re-center public SQL helper/input handling back into `favn`, leaving `favn_runner` focused on runtime execution and allowing it to drop the temporary dependency on `favn`.
7. Keep the current manifest/version constants unchanged while evolving the refactor in place.

The most important recommendation is to keep the boundary line sharp:

- `favn_core` owns canonical manifest payload and shared value contracts
- `favn_runner` owns execution orchestration against pinned manifests
- `favn_duckdb` owns DuckDB adapter implementation and any backend-specific runtime support
- `favn` owns public authoring DSL and public helper APIs, but not runner implementation details

Scope guard for this phase:

- support exactly two DuckDB execution placements: `:in_process` and `:separate_process`
- do not introduce more runtime variants, worker pools, multiple DuckDB plugin packages, or generalized compute scheduling abstractions

## Current Reality On `main`

The Phase 7 plan should fit what already exists today:

- `Favn.Manifest.Asset` in `apps/favn_core/lib/favn/manifest/asset.ex` stores generic execution metadata, but no SQL query/template payload
- `FavnRunner.Worker` explicitly rejects `:sql` assets with `:sql_manifest_execution_not_supported`
- `Favn.SQLAsset.Runtime` and `Favn.SQLAsset.Renderer` still fetch compiled module definitions and resolve deferred asset refs through `Favn.Assets.Compiler`, which is not manifest-pinned runtime behavior
- `apps/favn_runner/lib/favn/sql/adapter/duckdb*.ex` still owns DuckDB runtime code, and `apps/favn_runner/mix.exs` still pulls `{:duckdbex, ...}` directly
- `apps/favn_runner/mix.exs` still depends on `{:favn, in_umbrella: true}`, which does not match the locked `favn_runner -> favn_core` dependency direction
- `apps/favn/lib/favn/sql_asset.ex` still generates `asset/1` as a deterministic `{:error, :runtime_not_available}` stub even though the moduledoc says SQL assets call into runtime automatically
- public SQL helper APIs (`Favn.render/2`, `Favn.preview/2`, `Favn.explain/2`, `Favn.materialize/2`) are still missing from `apps/favn/lib/favn.ex`, while the old normalization helper lives only in legacy `Favn.Submission`
- `apps/favn/lib/favn/sql.ex` still mixes public authoring DSL with temporary runtime bridge wrapper functions such as `connect/2`, `query/3`, and `materialize/3`
- the current DuckDB adapter API is written as a direct adapter module, so without care it could drift into a chatty process-routing layer instead of a narrow placement seam

That means Phase 7 is not just "move files into `favn_duckdb`". It must finish the runner SQL contract so DuckDB extraction lands on the final manifest-backed path instead of moving the current temporary seams around.

## Phase 7 Architecture Decisions

### 1. Evolve the current manifest contract in place

Phase 7 changes the canonical manifest payload, but it does not need a schema-version or runner-contract-version bump during this refactor.

Recommended rule:

- keep `Favn.Manifest.Compatibility.current_schema_version/0` unchanged
- keep `Favn.Manifest.Compatibility.current_runner_contract_version/0` unchanged
- extend the current manifest payload shape in place
- do not add compatibility shims or dual-shape runtime handling unless a concrete break in the current codebase forces it

This is consistent with the repo rules:

- private development only
- breaking changes are allowed
- no need to preserve transitional compatibility layers before `v1.0`

Operational implication:

- local dev and tests should rebuild and re-register manifests as Phase 7 lands
- do not spend Phase 7 scope on multi-shape persisted-manifest compatibility machinery

### 2. Add a canonical SQL execution payload to the manifest/core contract

The runner cannot execute SQL assets from pinned manifests until the manifest carries the SQL runtime payload directly.

Recommended new manifest-side data split in `favn_core`:

- keep `Favn.Manifest.Asset` as the canonical runtime descriptor for all assets
- add a dedicated optional SQL execution payload field on manifest assets, for example `execution_payload` or `sql_execution`
- store that payload only for `type: :sql` assets

Recommended SQL payload contents:

1. the canonical root SQL string for metadata/debug output
2. the compiled SQL template IR for the asset query
3. the manifest-safe catalog of reusable SQL definitions needed during expansion
4. any runtime metadata that is actually required for render/materialize execution

Recommended design rule:

- do not persist compile-only raw asset metadata such as `raw_asset`, module source file paths, or compile diagnostics
- do reuse existing core value types where they are already canonical and runtime-safe
- if an existing struct carries compile-only fields, create a manifest-specific stripped struct instead of persisting the whole compile-time struct blindly

Recommended new modules in `apps/favn_core/lib/favn/manifest/`:

- `sql_execution.ex`
- `sql_definition.ex`

Recommended generator flow:

- `Favn.Manifest.Generator` should keep compiling SQL assets from authored modules exactly once
- during `ManifestAsset.from_asset/1`, SQL assets should attach a manifest-safe execution payload built from `Favn.SQLAsset.Definition`
- non-SQL assets should keep `nil`/empty execution payloads

Recommended direct-asset-ref rule:

- do not resolve direct asset refs at runtime by recompiling referenced modules
- keep symbolic asset refs in the SQL template payload
- let the runtime resolve them from the pinned manifest asset catalog

That is the core contract change that unlocks the rest of Phase 7.

### 3. Introduce a runner-side runtime definition that can be built from either compiled SQL definitions or manifest payload

Current runner SQL runtime code is mostly useful, but it is wired to the wrong input shape.

Recommended refactor:

- keep the SQL render/materialize engine in `favn_runner`
- stop making it depend directly on compiled `Favn.SQLAsset.Definition` values
- introduce one runtime-focused internal struct that contains only what render/materialize needs

Recommended internal split:

- `compiled definition -> runtime definition` adapter for public helper paths
- `manifest SQL payload -> runtime definition` adapter for runner execution paths
- shared renderer/materializer logic works only on the runtime definition

Recommended new internal module shape in `apps/favn_runner/lib/favn/sql_asset/`:

- `runtime_definition.ex`
- `definition_builder.ex`

This keeps the refactor small:

- the renderer and materialization planner do not need two separate implementations
- manifest-backed runner execution and public helper APIs can share one low-level runtime engine
- `favn_runner` can stop depending on `%Favn.Asset{}` and `Favn.get_asset/1`

### 4. Make `favn_runner` SQL execution truly manifest-pinned

Once manifest assets carry SQL payload, the worker should execute SQL assets the same way it executes other pinned work: by resolving one target asset out of the registered manifest version and running that pinned descriptor.

Recommended runner behavior:

- `FavnRunner.Worker` should stop rejecting `:sql` assets
- on `:sql`, build a runtime definition from `%Favn.Manifest.Asset{}` plus the registered manifest version
- render/materialize through runner-owned SQL runtime modules
- resolve direct asset refs from `version.manifest.assets`, not from authored modules
- keep connection lookup at the runner edge through `Favn.Connection.Registry`

Recommended same-node rule:

- same-node mode still registers a full `%Favn.Manifest.Version{}` with the runner
- SQL execution must use the registered manifest payload even when runner and authoring code live in the same BEAM node
- the runner should not special-case same-node SQL execution back to module compilation

Recommended direct asset ref resolution path:

- build a manifest asset index keyed by `asset.ref`
- when SQL render encounters a direct asset ref, look up the referenced manifest asset
- use its manifest-carried relation metadata
- fail deterministically if the manifest target asset is missing or does not own a relation

This is the actual "manifest-pinned SQL asset execution" milestone, not just enabling a branch in the worker.

### 5. Move DuckDB into `favn_duckdb` as the first runner plugin

DuckDB is the proof case for the plugin model, so it should become optional in packaging and dependency terms.

Recommended extraction:

- move `Favn.SQL.Adapter.DuckDB` into `apps/favn_duckdb/lib/`
- move `Favn.SQL.Adapter.DuckDB.Client`
- move `Favn.SQL.Adapter.DuckDB.Client.Duckdbex`
- move `Favn.SQL.Adapter.DuckDB.ErrorMapper`
- move the `duckdbex` Mix dependency from `favn_runner` to `favn_duckdb`

Recommended naming rule:

- keep the existing adapter module name `Favn.SQL.Adapter.DuckDB` for the first extraction cut
- move physical ownership to the `favn_duckdb` app instead of renaming the adapter module at the same time

This is the smaller correct move because it proves the plugin boundary without introducing an unrelated public adapter rename while SQL manifest work is landing.

Recommended plugin contract:

- add a small `FavnRunner.Plugin` behaviour in `favn_runner`
- configure plugins through one generic runner-plugin config path such as `config :favn, :runner_plugins, [...]`
- each plugin declares the adapter modules and optional runtime children it provides
- each plugin may receive plugin-local options from the generic plugin config entry
- keep the plugin contract boring and local to runner ownership; it is not a generalized workload-routing framework

Recommended generic shape:

- DuckDB is the first SQL execution plugin
- future SQL execution plugins may include Snowflake or Databricks
- `favn` must not contain DuckDB-specific branches, config structs, or helper code
- the plugin system should be generic enough for additional SQL execution plugins, but not broaden into a general platform framework in Phase 7

Recommended `favn_duckdb` plugin module:

- `FavnDuckdb` or `FavnDuckdb.Plugin` implements `FavnRunner.Plugin`
- returns `sql_adapters: [Favn.SQL.Adapter.DuckDB]`
- returns either no runtime children for `:in_process` or one long-lived worker child for `:separate_process`
- interprets its own placement option from the generic plugin configuration entry

Recommended `FavnRunner.Application` behavior:

- read configured plugins from the generic runner-plugin config path
- validate plugin modules at startup
- start any plugin child specs before the runner server if needed
- pass plugin-local options through to the plugin module without adding DuckDB-specific code paths in runner startup
- keep connection resolution boring: connection definitions may still point directly at the adapter module they need

Important non-goal for the first plugin cut:

- do not redesign the public connection definition DSL around symbolic plugin ids yet

The first plugin proof only needs:

- optional dependency packaging
- explicit plugin registration/loading
- extracted backend code ownership

### 6. Add a minimal DuckDB execution-placement seam

Phase 7 should add one narrow placement configuration seam and stop there.

Supported modes:

- `:in_process`
- `:separate_process`

Meaning:

- `:in_process` means DuckDB executes in the main runner process path
- `:separate_process` means DuckDB executes in a dedicated long-lived BEAM worker owned by the runner/plugin path

Recommended rule:

- placement is runtime/plugin configuration only
- placement must not appear in manifest payloads
- placement must not appear in public authoring DSL or user-authored asset semantics
- placement must not leak into orchestrator behavior or `favn_core` contracts
- implement placement through generic plugin configuration plus plugin-owned interpretation, not DuckDB-specific branches in `favn`

Recommended runtime shape in `favn_duckdb`:

- one shared logical DuckDB runtime contract used by both modes
- one inline implementation for `:in_process`
- one dedicated long-lived worker implementation for `:separate_process`
- one thin dispatcher chosen from plugin/runtime config

Recommended module split:

- `FavnDuckdb` or `FavnDuckdb.Plugin` for plugin registration/config
- `FavnDuckdb.Runtime` for the shared logical runtime contract
- `FavnDuckdb.Runtime.InProcess` for inline execution
- `FavnDuckdb.Runtime.SeparateProcess` for the long-lived worker client/server path
- `FavnDuckdb.Worker` for the dedicated worker process in separate-process mode

Important constraints:

- the separate worker must be long-lived, not spawn-per-query
- do not add worker pools
- do not add distributed placement logic
- do not add generalized workload routing abstractions unless strictly required by the two-mode seam
- keep cross-process calls coarse-grained so the worker boundary does not dominate execution cost

Recommended execution-unit rule:

- cross-process calls should wrap meaningful execution units such as query, materialize, or statement groups
- do not redesign the API around many tiny back-and-forth calls

### 7. Remove temporary runtime seams from `favn`

The manifest-backed runner SQL path should let `favn` return to a thinner public role.

Recommended cleanup in `apps/favn/lib/`:

1. Add the missing public SQL helper APIs to `Favn`:
   - `render/2`
   - `preview/2`
   - `explain/2`
   - `materialize/2`
2. Recreate the old `Favn.Submission` normalization behavior inside `favn`, not `favn_legacy`.
3. Make generated `Favn.SQLAsset.asset/1` delegate through a small dynamic runtime call instead of returning a hardcoded `{:error, :runtime_not_available}` stub.
4. Stop using `apps/favn/lib/favn/sql.ex` as a mixed DSL/runtime wrapper once runner-owned runtime modules no longer need it.

Recommended ownership split after cleanup:

- `Favn.SQL` in `apps/favn` returns to authoring DSL only (`defsql`, `~SQL`)
- `Favn` owns public input normalization and public helper entrypoints
- runner-owned SQL runtime modules expose low-level execution functions that operate on core contracts, not public DSL structs

Recommended public helper call shape:

- `favn` resolves a SQL asset module/ref/input into a compiled SQL definition using the public compiler surface
- `favn` dynamically calls the runner-owned runtime module with that compiled definition
- if the runner runtime is not available, return deterministic `{:error, :runtime_not_available}` just like the current orchestrator/scheduler wrappers

This removes the current accidental coupling where runner SQL code depends on the public `favn` package only because helper/input logic and temporary runtime wrapper functions sit on the wrong side of the boundary.

### 8. Drop the temporary `favn_runner -> favn` dependency

Phase 7 should close with `favn_runner` depending only on `favn_core` again.

Recommended rule:

- `apps/favn_runner/mix.exs` should keep `{:favn_core, in_umbrella: true}`
- remove `{:favn, in_umbrella: true}` once SQL helper/input responsibilities are moved back to `favn`
- remove `{:duckdbex, ...}` once DuckDB code is extracted into `favn_duckdb`

This is an important architecture checkpoint, not just a cosmetic dependency cleanup. It proves that:

- `favn` is the public authoring package
- `favn_runner` is the execution runtime
- plugins attach to the runner without dragging public authoring implementation back into the runtime app

## File Plan

### In `apps/favn_core/lib/favn/manifest/`

- `sql_execution.ex`
- `sql_definition.ex`
- update `asset.ex`
- update `generator.ex`
- update `compatibility.ex`

### In `apps/favn_runner/lib/favn/sql_asset/`

- `runtime_definition.ex`
- `definition_builder.ex`
- update `runtime.ex`
- update `renderer.ex`

### In `apps/favn_runner/lib/favn_runner/`

- `plugin.ex`
- `plugin_config.ex` if a tiny generic config-normalization helper is needed
- update `application.ex`
- update `worker.ex`

### Move out of `apps/favn_runner/lib/favn/sql/adapter/duckdb*`

- `adapter/duckdb.ex`
- `adapter/duckdb/client.ex`
- `adapter/duckdb/client/duckdbex.ex`
- `adapter/duckdb/error_mapper.ex`

### Into `apps/favn_duckdb/lib/`

- `favn_duckdb.ex`
- `favn_duckdb/runtime.ex`
- `favn_duckdb/runtime/in_process.ex`
- `favn_duckdb/runtime/separate_process.ex`
- `favn_duckdb/worker.ex`
- moved DuckDB adapter/runtime files under the preserved `Favn.SQL.Adapter.DuckDB*` namespaces

### In `apps/favn/lib/`

- update `favn.ex` with public SQL helpers
- update `favn/sql.ex` to remove temporary runtime wrapper responsibilities
- update `favn/sql_asset.ex` generated `asset/1` behavior
- add a small favn-owned SQL input/helper bridge module if needed

The exact helper module count can stay smaller than this, but the ownership split should stay the same.

## Implementation Order

Recommended Phase 7 work order:

1. Add the manifest-side SQL payload structs and generator wiring in `favn_core`.
2. Update serializer/version/compatibility coverage without changing the current version constants.
3. Refactor runner SQL runtime code around internal runtime definitions that can be built from compiled definitions or manifest payload.
4. Enable runner SQL execution from registered manifest versions and replace the current worker rejection path.
5. Add the generic plugin behaviour/loading path in `favn_runner` with plugin-local options.
6. Add the two-mode DuckDB placement seam in `favn_duckdb` with a shared runtime contract and one long-lived worker path for `:separate_process`.
7. Move DuckDB code and `duckdbex` into `favn_duckdb`.
8. Recreate public SQL helper/input behavior in `favn`, add `Favn.render/2`-style APIs, and remove the current hardcoded `Favn.SQLAsset.asset/1` stub behavior.
9. Stop runner-owned SQL code from depending on public `Favn.SQL` wrapper functions or `%Favn.Asset{}` inputs.
10. Remove the temporary `favn_runner -> favn` dependency and document the generic plugin config path.

## Testing Plan

### `apps/favn_core/test`

- add manifest payload coverage for SQL assets carrying execution payload
- add serializer/version tests that prove SQL payload survives canonical serialization and hashing
- add compatibility/version tests proving the current manifest constants still hold after the SQL payload expansion
- add manifest contract lock coverage for the new SQL payload keys if needed

### `apps/favn_runner/test`

- replace the current `sql assets are rejected` test with manifest-backed SQL execution coverage
- add tests for manifest-native direct asset ref resolution
- add tests that runner SQL execution does not consult compiled module definitions
- add plugin loader tests for configured/unconfigured plugins
- add same-node runner integration coverage that registers a manifest with SQL payload and successfully materializes through DuckDB
- add focused coverage that manifest-pinned SQL execution works through both `:in_process` and `:separate_process` modes

### `apps/favn/test`

- add public facade tests for `Favn.render/2`, `Favn.preview/2`, `Favn.explain/2`, and `Favn.materialize/2`
- add runtime-not-available tests for those helper APIs when runner runtime is not started
- add coverage for generated `Favn.SQLAsset.asset/1` delegating through the runtime bridge rather than returning a static stub

### `apps/favn_duckdb/test`

- move or recreate the current DuckDB adapter behavior coverage from `apps/favn_legacy/test/sql_duckdb_adapter_test.exs`
- move or recreate the current DuckDB hardening coverage from `apps/favn_legacy/test/sql_duckdb_adapter_hardening_test.exs`
- add a focused plugin module test that proves the declared adapter/runtime metadata matches the runner plugin contract
- add worker lifecycle and cleanup tests for `:separate_process`
- add focused failure-path tests for both modes
- add cancellation-behavior tests for both modes if the current runner cancellation contract already reaches the execution boundary in a testable way

## Minimal Config Shape

Phase 7 should add only the smallest runtime configuration needed for placement.

Recommended config shape:

```elixir
config :favn,
  runner_plugins: [
    {FavnDuckdb, execution_mode: :in_process}
  ]
```

```elixir
config :favn,
  runner_plugins: [
    {FavnDuckdb, execution_mode: :separate_process, worker_name: FavnDuckdb.Worker}
  ]
```

Recommended validation rules:

- `execution_mode` accepts only `:in_process | :separate_process`
- default to `:in_process`
- keep worker naming/config minimal and plugin-owned through generic plugin opts
- do not add manifest options, asset DSL options, or orchestrator settings for placement
- do not add DuckDB-specific config branches in `favn`

## DuckDB Runtime Rules To Enforce In Code

These are explicit implementation rules for this phase, not optional style notes.

### Bulk write rule

- keep bulk write paths appender-oriented or otherwise bulk-oriented
- do not redesign large-ingest paths around repeated prepared inserts
- keep a short implementation note or moduledoc comment near the owned write path stating that DuckDB recommends the Appender for large inserts

### Long-lived worker rule

- in `:separate_process`, the DuckDB worker must be long-lived and coarse-grained
- do not spawn one process per query or per statement
- prefer reuse of the same worker/session path for substantial workloads

### Avoid chatty cross-process APIs

- for `:separate_process`, keep cross-process messaging coarse enough that the worker boundary does not dominate cost
- prefer query/materialize execution units over fine-grained call chains
- do not add speculative request-routing layers or multi-hop adapters

### Placement stays out of authoring contracts

- do not add placement controls to manifest payloads
- do not add placement controls to `Favn.SQLAsset`, `Favn.Asset`, or connection authoring DSLs in Phase 7
- keep placement as runner/plugin configuration only

### Keep failure boundaries boring

- lifecycle ownership for the separate DuckDB worker must stay local to runner/plugin ownership
- shutdown and cleanup behavior must be explicit
- do not turn the separate worker into a second orchestrator or scheduler

## Boundary Rules That Must Remain True

- `favn_duckdb -> favn_runner` only
- no `favn_duckdb -> favn`
- no `favn_duckdb -> favn_orchestrator`
- no `favn_duckdb -> favn_legacy`
- no DuckDB-specific branches in `favn` code
- no orchestrator-owned DuckDB runtime behavior
- no DuckDB-specific runtime handles in `favn_core`
- no process-placement fields in manifest payloads
- no generalized distributed compute abstractions introduced under the excuse of placement

### Legacy test reduction goal

- after the new app-owned tests land, DuckDB adapter correctness should no longer depend on legacy-owned test files
- the legacy SQL runtime test suite can remain as reference coverage until final cutover, but it should stop being the only place that exercises these behaviors

## Documentation Updates Required In Phase 7

Phase 7 implementation should update at least:

- `README.md`
- `docs/REFACTOR.md`
- `docs/FEATURES.md`
- `docs/structure/`
- `apps/favn_duckdb/README.md`

The docs should make these points explicit:

- SQL asset runner execution is now manifest-backed
- DuckDB now lives in the optional `favn_duckdb` plugin app
- `favn_runner` no longer bundles DuckDB directly
- the supported user dependency shape is `favn` plus optional plugin packages such as `favn_duckdb`
- public SQL helper APIs live on `Favn`, while `Favn.SQL` stays focused on authoring DSL concerns

## Explicit Out-Of-Scope List

Do not implement these as part of Phase 7:

- new SQL backends beyond DuckDB
- multiple DuckDB plugin packages
- worker pools
- autoscaling
- resource-aware placement
- generalized compute scheduling or workload routing
- a symbolic plugin-id redesign for connection definitions
- remote runner transport work
- view/UI work
- developer tooling and packaging flows beyond the initial plugin dependency/config path
- final legacy deletion and storage payload cleanup
- broad SQL feature expansion unrelated to manifest-backed execution/plugin extraction

## Later Roadmap Items To Add Separately

These should be tracked later and not implemented in this phase:

- worker pooling and concurrency-control improvements for DuckDB `:separate_process` mode
- stronger cancellation, isolation, and recovery semantics for the DuckDB worker path
- resource-aware placement for heavy DuckDB workloads after real workload validation
- observability and tuning for DuckDB worker memory/runtime behavior
- packaging and deployment refinements for different DuckDB runner shapes
- richer execution plugin ecosystem
- advanced queueing and admission control
- broader runtime placement controls after real workload validation

## Current Overreach Risks On `main`

The current codebase has a few places where implementation could accidentally exceed the intended scope if not kept tight:

1. Plugin loading could drift into a generalized backend framework.
Keep `FavnRunner.Plugin` minimal and DuckDB-driven; do not add speculative workload routing APIs.

2. Separate-process DuckDB could drift into a pool/scheduler model.
Keep one long-lived worker path first. Do not add pool sizing, balancing, or placement heuristics.

3. SQL runtime refactors could leak placement into manifest/core contracts.
Keep manifest payload focused on executable SQL content only, never runtime placement policy.

4. Generic plugin config could accidentally become DuckDB-specific code in `favn`.
Keep `favn` free of DuckDB branches; plugin-local opts should be interpreted by the plugin or by generic runner plugin-loading code only.

5. Cross-process worker APIs could become too chatty.
Avoid fine-grained message protocols that mimic a database driver over BEAM messages; use coarse execution units.

Phase 7 is successful when SQL assets execute from pinned manifest payloads in `favn_runner`, DuckDB code and dependencies live in `favn_duckdb`, DuckDB placement is configurable only as `:in_process | :separate_process`, the plugin architecture is generic enough for future SQL execution plugins without adding DuckDB-specific logic to `favn`, and the runner app returns to the intended dependency shape centered on `favn_core` plus optional plugins.
