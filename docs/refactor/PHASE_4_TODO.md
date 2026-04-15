# Phase 4 TODO

## Status

Checklist for implementing the Phase 4 runner boundary plan defined in `docs/refactor/PHASE_4_RUNNER_BOUNDARY_PLAN.md`.

This checklist is intentionally scoped to execution ownership only. Scheduling, persistence, and control-plane lifecycle work stay in Phase 5.

Current implementation status: core Phase 4 runner execution boundary is implemented (manifest registration, runner server/worker path, Elixir/source/SQL execution, and runner-side connection runtime ownership). Remaining unchecked items are either optional cleanup splits or explicit out-of-scope guardrails.

## Shared Contract Preparation

- [x] Move `Favn.Run.Context` from `favn_legacy` to `favn_core`.
- [x] Move `Favn.Run.AssetResult` from `favn_legacy` to `favn_core`.
- [x] Update shared types/docs so runner result payloads use the new owner modules.
- [x] Add `apps/favn_core/test/run/context_test.exs`.
- [x] Add `apps/favn_core/test/run/asset_result_test.exs`.

## Runner App Skeleton

- [x] Replace scaffold `FavnRunner.hello/0` API with runner entrypoints.
- [x] Add `FavnRunner.Server`.
- [x] Add supervised manifest store.
- [x] Add execution registry/subscription support if needed for same-node await/cancel flow.
- [x] Add dynamic supervisor for runner workers.
- [x] Keep `FavnRunner.Application` limited to execution-owned children only.

## Manifest Registration And Resolution

- [x] Implement `register_manifest/1` around `%Favn.Manifest.Version{}`.
- [x] Make registration idempotent by `manifest_version_id` and `content_hash`.
- [x] Reject incompatible manifest schema or runner contract versions during registration.
- [x] Implement manifest lookup by `manifest_version_id` during work execution.
- [x] Verify `manifest_content_hash` from `%Favn.Contracts.RunnerWork{}` before execution.
- [x] Reject work that references missing manifests or unknown asset refs.

## Runner Work Protocol

- [x] Implement `submit_work/2` for `%Favn.Contracts.RunnerWork{}`.
- [x] Require exactly one target asset for Phase 4 execution.
- [x] Return an opaque runner execution id for in-flight work.
- [x] Implement `await_result/2` for same-node integration tests/dev flows.
- [x] Implement `cancel_work/2` for active runner work only.
- [x] Add `run/2` convenience wrapper that goes through the same server path.

## Elixir And Source Asset Execution

- [x] Recreate the local executor boundary in `favn_runner` without depending on `favn_legacy`.
- [x] Build runtime `%Favn.Run.Context{}` values from work request + manifest asset descriptor.
- [x] Execute Elixir assets using manifest `module` + `entrypoint` metadata.
- [x] Preserve strict return-shape normalization for `:ok | {:ok, map()} | {:error, reason}`.
- [x] Execute source assets as observe/no-op results instead of calling user code.
- [x] Emit `Favn.Contracts.RunnerEvent` values for start/success/failure/cancel/timeout.

## Connection Runtime Ownership

- [x] Move `Favn.Connection.Loader` into the runner owner app.
- [x] Move `Favn.Connection.Registry` into the runner owner app.
- [x] Move `Favn.Connection.Resolved` into the runner owner app.
- [x] Move `Favn.Connection.Validator`, `Error`, `Sanitizer`, and `Info` as needed for runtime resolution.
- [x] Keep `Favn.Connection.Definition` in `favn_core`.
- [x] Resolve runtime connection config at the runner edge rather than inside core planning code.

## SQL Runtime Slice

- [x] Move `Favn.SQLAsset.Runtime` out of legacy into the runner owner app.
- [x] Move the smallest backend-neutral SQL runtime module set required for SQL asset execution.
- [x] Keep plugin extraction out of scope; do not introduce a `favn_legacy` dependency.
- [x] Ensure SQL assets execute through the runner boundary, not through legacy coordinator paths.
- [x] Keep public direct SQL helper API expansion out of scope unless needed to support runner execution.

## Tests

- [x] Add `apps/favn_runner/test/server_test.exs`.
- [x] Add `apps/favn_runner/test/manifest_store_test.exs`.
- [x] Add `apps/favn_runner/test/manifest_resolver_test.exs`.
- [x] Add `apps/favn_runner/test/worker_test.exs`.
- [x] Add `apps/favn_runner/test/execution/elixir_asset_test.exs`. (covered in `apps/favn_runner/test/favn_runner_test.exs`)
- [x] Add `apps/favn_runner/test/execution/source_asset_test.exs`. (covered in `apps/favn_runner/test/favn_runner_test.exs`)
- [x] Add `apps/favn_runner/test/execution/sql_asset_test.exs`.
- [x] Add same-node integration coverage that registers a manifest version and submits runner work.
- [x] Keep scheduler/storage/orchestrator tests in legacy until Phase 5/6.

## Docs Updates

- [x] Update `README.md` once `favn_runner` owns real execution paths.
- [x] Update `docs/REFACTOR.md` Phase 4 status as slices land.
- [x] Update `docs/FEATURES.md` checkboxes as runner slices complete.
- [x] Update `docs/lib_structure.md` with new runner-owned modules.
- [x] Update `docs/test_structure.md` with new runner test layout.

## Verification

- [x] Run `mix format`.
- [x] Run `mix compile --warnings-as-errors`.
- [x] Run `mix test`.
- [x] Run `mix credo --strict`.
- [x] Run `mix dialyzer`.
- [x] Run `mix xref graph --format stats --label compile-connected`.

## Explicit Out Of Scope For Later Phases

- [ ] Do not migrate orchestrator run lifecycle state transitions in Phase 4.
- [ ] Do not migrate scheduler persistence/runtime in Phase 4.
- [ ] Do not build storage adapters in Phase 4.
- [ ] Do not add the view/UI layer in Phase 4.
- [ ] Do not extract DuckDB into `favn_duckdb` in Phase 4.
- [ ] Do not turn the runner into a control-plane service.
