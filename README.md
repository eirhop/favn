# Favn

## Status

Favn `v0.5.0` refactor is in progress.

- umbrella structure is scaffolded under `apps/`
- `apps/favn_legacy` contains the active v0.4 reference runtime during migration
- `apps/favn` is only a Phase 1 scaffold and is not yet the migrated public API
- Phase 0/1 contracts and migration rules are tracked in `docs/REFACTOR.md`

## Phase 1 Reality

- the old `Favn.*` runtime behavior currently comes from `apps/favn_legacy`
- examples using `Favn.*` runtime APIs are legacy-reference examples for migration comparison
- those examples are not yet the new Phase 1 source of truth for the migrated architecture

## Scope For This Branch Of Development

- structural refactor preparation only
- no Phase 2 runtime/DSL migration work merged into new owner apps yet
- breaking changes remain allowed before `v1.0`

## Documentation Pointers

- `docs/REFACTOR.md` - locked architecture boundaries, migration rules, phase plan
- `docs/FEATURES.md` - roadmap status
- `docs/lib_structure.md` - umbrella library layout
- `docs/test_structure.md` - umbrella test layout
