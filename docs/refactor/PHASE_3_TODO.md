# Phase 3 TODO

## Status

Checklist for implementing the Phase 3 manifest/versioning plan defined in `docs/refactor/PHASE_3_MANIFEST_VERSIONING_PLAN.md`.

Phase 3 implementation scope is complete. Remaining unchecked items are intentional out-of-scope guardrails for later phases.

## Schema Definition

- [x] Create `Favn.Manifest.Build` and split compile/build output from canonical runtime manifest payload.
- [x] Rewrite `Favn.Manifest` as the canonical runtime-required manifest struct with no diagnostics or timestamps in the hashed payload.
- [x] Define canonical `Favn.Manifest.Asset` fields for Elixir, SQL, and source assets.
- [x] Define canonical `Favn.Manifest.Pipeline` fields for persisted pipeline descriptors.
- [x] Define canonical `Favn.Manifest.Schedule` fields for persisted schedule descriptors.
- [x] Define `Favn.Manifest.Graph` for normalized dependency edges and deterministic topo order.
- [x] Document which current `%Favn.Asset{}` and `%Favn.Pipeline.Definition{}` fields remain runtime-required and which become build-only.

## Hashing And Version Identity

- [x] Create `Favn.Manifest.Version` as the immutable pinned manifest-version envelope.
- [x] Create `Favn.Manifest.Identity` for canonical content hashing.
- [x] Choose and document the hash algorithm and encoding.
- [x] Define canonical list ordering and map-key ordering rules that feed hashing.
- [x] Ensure timestamps, diagnostics, and source locations are excluded from the hash input.
- [x] Define the relationship between `manifest_version_id` and `content_hash`.
- [x] Document dedup/integrity expectations for orchestrator persistence.

## Serialization

- [x] Create `Favn.Manifest.Serializer` in `favn_core`.
- [x] Define the canonical serialized format identifier, recommended `json-v1`.
- [x] Implement canonical encode rules for manifest structs.
- [x] Implement canonical decode rules for persisted manifest-version payloads.
- [x] Add round-trip tests for manifest and manifest-version serialization.
- [x] Ensure canonical bytes are stable for semantically identical input.

## Compatibility Checks

- [x] Create `Favn.Manifest.Compatibility`.
- [x] Define `schema_version` validation behavior.
- [x] Define `runner_contract_version` validation behavior.
- [x] Define strict failure behavior for unsupported versions.
- [x] Define the compatibility checks orchestrator must perform before persisting a new manifest version.
- [x] Define the compatibility checks runner must perform before executing pinned work.

## `favn` Vs `favn_core` Ownership Cleanup

- [x] Move pure value objects from `apps/favn/lib/favn/*.ex` into `apps/favn_core/lib/favn/*.ex`.
- [x] Move compile-time helper modules from `apps/favn/lib/favn/**` into `apps/favn_core/lib/favn/**`.
  - [x] Move planning modules (`Favn.Plan`, `Favn.Assets.Planner`) into `favn_core`.
  - [x] Move asset compiler/inference modules (`Favn.Assets.Compiler`, `Favn.Assets.DependencyInference`) into `favn_core`.
  - [x] Move pipeline value modules (`Favn.Pipeline.Definition`, `Favn.Pipeline.Resolution`) into `favn_core`.
  - [x] Move pipeline resolver (`Favn.Pipeline.Resolver`) into `favn_core`.
  - [x] Move SQL helper internals (`Favn.SQL.*`, `Favn.SQLAsset.*`) into `favn_core` while keeping DSL entrypoints in `favn`.
- [x] Rewrite the current `apps/favn/lib/favn/manifest*.ex` files under `apps/favn_core/lib/favn/manifest*.ex`.
- [x] Rewrite `apps/favn/lib/favn/assets/graph_index.ex` as a pure core-owned graph module.
- [x] Thin `apps/favn/lib/favn.ex` so it delegates build/version/serialization work to `favn_core`.
- [x] Keep public DSL entrypoints in `apps/favn/lib/favn/*.ex` and point them at core-owned structs/helpers.
- [x] Remove duplicate ownership so `Favn.*` internal modules do not compile from both apps at once.
- [x] Remove remaining core boundary leaks so planner/graph/resolver run on explicit inputs rather than `:favn` app-config/facade callbacks.
- [x] Leave runtime execution, scheduling, and storage files untouched for later phases.

## Shared Contracts For Later Runtime Split

- [x] Create `Favn.Contracts.RunnerWork`.
- [x] Create `Favn.Contracts.RunnerResult`.
- [x] Create `Favn.Contracts.RunnerEvent`.
- [x] Ensure each shared contract references pinned manifest versions rather than mutable “latest manifest” concepts.
- [x] Document which fields in those contracts come from the manifest versus from orchestrator runtime state.

## Tests

- [x] Add `apps/favn_core/test/manifest/build_test.exs`.
- [x] Add `apps/favn_core/test/manifest/version_test.exs`.
- [x] Add `apps/favn_core/test/manifest/identity_test.exs`.
- [x] Add `apps/favn_core/test/manifest/serializer_test.exs`.
- [x] Add `apps/favn_core/test/manifest/compatibility_test.exs`.
- [x] Add `apps/favn_core/test/manifest/graph_test.exs`.
- [x] Add `apps/favn_core/test/contracts/runner_work_test.exs`.
- [x] Add `apps/favn_core/test/contracts/runner_result_test.exs`.
- [x] Add `apps/favn_core/test/contracts/runner_event_test.exs`.
- [x] Keep `apps/favn/test` focused on public facade/DSL tests only.
- [x] Keep runtime, scheduler, storage, and SQL execution tests in `apps/favn_legacy/test` for now.
- [x] Lock contract tests before starting Phase 4 runner implementation.
- [x] Add explicit-input boundary tests for planner/resolver in `apps/favn_core/test`.
- [x] Add public-facade default assembly tests in `apps/favn/test`.

## Docs Updates

- [x] Update `README.md` status and documentation pointers.
- [x] Update `docs/REFACTOR.md` Phase 3 section to point at the manifest/versioning plan.
- [x] Update `docs/FEATURES.md` so Phase 2 transitional state and Phase 3 planning are both discoverable.
- [x] Update `docs/lib_structure.md` to show `favn_core` as the intended home for internal manifest/compiler/contracts modules.
- [x] Update `docs/test_structure.md` to show the planned `favn_core` contract-test growth.
- [x] Update `docs/refactor/PHASE_2_MIGRATION_PLAN.md` to link forward to the Phase 3 plan and clarify the transitional layout.

## Explicit Out Of Scope For Later Phases

- [ ] Do not build the full runner execution engine in Phase 3.
- [ ] Do not migrate orchestrator scheduling/runtime in Phase 3.
- [ ] Do not build storage adapters in Phase 3.
- [ ] Do not implement the view/UI layer in Phase 3.
- [ ] Do not migrate full SQL runtime ownership in Phase 3.
- [ ] Do not move DuckDB/plugin ownership in Phase 3.
- [ ] Do not build local dev tooling or packaging flows in Phase 3.
