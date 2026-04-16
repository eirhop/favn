# Phase 7 TODO

## Status

Checklist for implementing the Phase 7 plan defined in `docs/refactor/PHASE_7_DUCKDB_RUNNER_PLAN.md`.

This checklist covers the SQL manifest contract, manifest-pinned runner SQL execution, DuckDB plugin extraction, and removal of the temporary runtime seams still sitting in `favn`.

## Manifest / Core Contract

- [x] Add a manifest-side SQL execution payload struct in `favn_core`.
- [ ] Add a manifest-side reusable SQL definition struct in `favn_core`.
- [x] Extend `Favn.Manifest.Asset` so SQL assets carry execution payload.
- [x] Teach `Favn.Manifest.Generator` to emit SQL payload for `type: :sql` assets.
- [x] Keep compile-only raw asset metadata out of the canonical manifest payload.
- [x] Keep the current manifest schema and runner-contract version constants unchanged.

## Runner SQL Execution

- [x] Introduce a runner-side runtime definition that the SQL runtime can execute.
- [x] Add a builder for `compiled definition -> runtime definition`.
- [x] Add a builder for `manifest SQL payload -> runtime definition`.
- [x] Update runner SQL render/materialize code to operate on the runtime definition instead of compiled module definitions directly.
- [x] Resolve direct SQL asset refs from the pinned manifest asset catalog.
- [x] Remove the explicit `:sql_manifest_execution_not_supported` worker rejection path.
- [x] Add manifest-backed SQL execution coverage in `apps/favn_runner/test`.

## Public `favn` Cleanup

- [x] Add `Favn.render/2`.
- [x] Add `Favn.preview/2`.
- [x] Add `Favn.explain/2`.
- [x] Add `Favn.materialize/2`.
- [x] Recreate SQL asset input normalization in `favn` instead of relying on legacy `Favn.Submission`.
- [x] Make generated `Favn.SQLAsset.asset/1` delegate through runtime rather than returning a hardcoded stub.
- [x] Remove temporary runtime-wrapper responsibilities from `apps/favn/lib/favn/sql.ex` once the new helper path is in place.

## DuckDB Plugin Extraction

- [x] Add a small `FavnRunner.Plugin` behaviour.
- [x] Load configured plugins from `config :favn, :runner_plugins, [...]`.
- [x] Pass plugin-local options through the generic plugin config path instead of adding DuckDB-specific config handling in `favn`.
- [x] Move DuckDB adapter files from `favn_runner` into `favn_duckdb`.
- [x] Move `duckdbex` from `apps/favn_runner/mix.exs` to `apps/favn_duckdb/mix.exs`.
- [x] Replace the scaffold `FavnDuckdb.hello/0` module with real plugin/docs entrypoints.
- [x] Add DuckDB runtime config validation for exactly `:in_process | :separate_process`.
- [x] Add one shared logical DuckDB runtime contract used by both modes.
- [x] Implement the `:in_process` DuckDB execution path.
- [x] Implement the `:separate_process` DuckDB execution path with one long-lived worker.
- [x] Keep placement in runner/plugin config only, not manifest or authoring DSL.
- [x] Keep bulk write paths appender-oriented and document the write-path rule near owned DuckDB execution code.
- [x] Recreate DuckDB adapter and hardening tests under `apps/favn_duckdb/test`.

## Placement-Focused Tests

- [x] Add focused tests for DuckDB `:in_process` execution.
- [x] Add focused tests for DuckDB `:separate_process` execution.
- [x] Add worker lifecycle and cleanup tests for separate-process mode.
- [x] Add failure-path tests for both placement modes.
- [ ] Add cancellation-behavior tests for both modes if the current runner contract exposes a testable cancellation boundary.
- [x] Verify manifest-pinned SQL execution still works through plugin integration in both modes.

## Dependency Cleanup

- [x] Remove the temporary `{:favn, in_umbrella: true}` dependency from `apps/favn_runner/mix.exs`.
- [x] Remove the direct `duckdbex` dependency from `apps/favn_runner/mix.exs`.
- [ ] Verify `favn_runner` depends only on `favn_core` plus runtime-level external deps, with DuckDB coming only through `favn_duckdb`.
- [x] Verify `favn` contains no DuckDB-specific runtime branches or config-handling code.

## Docs Updates

- [x] Update `README.md` for Phase 7 focus and plugin packaging notes.
- [x] Update `docs/REFACTOR.md` to point at the Phase 7 plan and checklist.
- [x] Update `docs/FEATURES.md` as Phase 7 planning and implementation slices land.
- [x] Update `docs/lib_structure.md` for the DuckDB ownership move.
- [x] Update `docs/test_structure.md` for new `favn_duckdb` and public SQL helper coverage.
- [ ] Update `apps/favn_duckdb/README.md` with plugin usage/config examples.

## Verification

- [x] Run `mix format`.
- [x] Run `mix compile --warnings-as-errors`.
- [x] Run `mix test`.
- [x] Run `mix credo --strict`.
- [x] Run `mix dialyzer`.
- [x] Run `mix xref graph --format stats --label compile-connected`.

## Explicit Out Of Scope For Later Phases

- [ ] Additional SQL plugins beyond DuckDB remain later-phase work.
- [ ] Multiple DuckDB plugin packages remain out of scope.
- [ ] Worker pools and autoscaling remain later-phase work.
- [ ] Resource-aware placement remains later-phase work.
- [ ] Generalized compute scheduling/workload routing remains later-phase work.
- [ ] Remote/distributed runner transport remains later-phase work.
- [ ] View runtime work remains Phase 8.
- [ ] Packaging/install/dev tooling remains Phase 9.
- [ ] Final storage payload cleanup and legacy deletion remain later phases.
