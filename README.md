# Favn

## Status

Favn `v0.5.0` refactor is in progress.

- umbrella structure is scaffolded under `apps/`
- `apps/favn_legacy` contains the active v0.4 reference runtime during migration
- `apps/favn` now owns the public Phase 2 authoring/DSL surface
- internal compiler/manifest/planning/shared-contract ownership has been re-centered into `apps/favn_core`
- `apps/favn_core` now owns Phase 3 manifest/versioning foundations and shared runner contracts
- Phase 0-3 contracts and migration rules are tracked in `docs/REFACTOR.md`

## Current Refactor Reality

- the public `Favn.*` authoring DSL now compiles from `apps/favn`
- runtime execution, scheduling, storage, and most operator paths still come from `apps/favn_legacy`
- canonical manifest schema/versioning, serializer/hash identity, compatibility checks, and shared runner contracts are now implemented in `favn_core`
- examples that exercise runtime execution may still reflect legacy-reference behavior during migration

## Current Focus

- finish Phase 3 and start Phase 4 runner-boundary implementation
- keep `favn` as the public DSL/facade package
- re-center internal compiler / manifest / planning / shared-contract machinery into `favn_core`
- define canonical manifest schema, version identity, hashing, and compatibility before runner/orchestrator implementation
- breaking changes remain allowed before `v1.0`

## Documentation Pointers

- `docs/REFACTOR.md` - locked architecture boundaries, migration rules, phase plan
- `docs/FEATURES.md` - roadmap status
- `docs/lib_structure.md` - umbrella library layout
- `docs/test_structure.md` - umbrella test layout
- `docs/refactor/PHASE_2_MIGRATION_PLAN.md` - Phase 2 transitional ownership plan
- `docs/refactor/PHASE_3_MANIFEST_VERSIONING_PLAN.md` - Phase 3 manifest/versioning architecture plan
- `docs/refactor/PHASE_3_TODO.md` - Phase 3 implementation checklist
