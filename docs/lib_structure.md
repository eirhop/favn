# Library Folder Structure (`apps/*/lib`)

This document maps the umbrella library layout during the Phase 2 -> Phase 3 transition.

```text
apps/
├── favn/lib/
│   ├── favn.ex
│   └── favn/
│       ├── public_scaffold.ex
│       ├── asset.ex
│       ├── assets.ex
│       ├── connection.ex
│       ├── multi_asset.ex
│       ├── namespace.ex
│       ├── pipeline.ex
│       ├── source.ex
│       ├── sql.ex
│       ├── sql_asset.ex
│       ├── window.ex
│       ├── triggers/
│       └── (public DSL entrypoints only)
├── favn_core/lib/
│   ├── favn_core.ex
│   └── favn/
├── favn_runner/lib/
│   ├── favn_runner.ex
│   └── favn_runner/application.ex
├── favn_orchestrator/lib/
│   ├── favn_orchestrator.ex
│   └── favn_orchestrator/application.ex
├── favn_view/lib/
│   ├── favn_view.ex
│   └── favn_view/application.ex
├── favn_storage_postgres/lib/
│   └── favn_storage_postgres.ex
├── favn_storage_sqlite/lib/
│   └── favn_storage_sqlite.ex
├── favn_duckdb/lib/
│   └── favn_duckdb.ex
├── favn_test_support/lib/
│   └── favn_test_support.ex
└── favn_legacy/lib/
    ├── favn.ex
    └── favn/
        ├── application.ex
        ├── agent_guide.ex
        ├── asset.ex
        ├── assets.ex
        ├── backfill.ex
        ├── connection.ex
        ├── multi_asset.ex
        ├── namespace.ex
        ├── pipeline.ex
        ├── run.ex
        ├── scheduler.ex
        ├── source.ex
        ├── sql.ex
        ├── sql_asset.ex
        ├── storage.ex
        ├── submission.ex
        ├── window.ex
        ├── asset/
        ├── assets/
        ├── connection/
        ├── dsl/
        ├── pipeline/
        ├── run/
        ├── runtime/
        ├── scheduler/
        ├── sql/
        ├── sql_asset/
        ├── storage/
        ├── triggers/
        └── window/
```

Notes:

- `favn_legacy` is the active v0.4 reference runtime during migration.
- Phase 2 migration currently establishes public DSL/facade ownership under `favn`.
- Runtime execution APIs remain legacy-owned while compile-time/manifest foundations are migrated.
- Internal compiler/manifest/planning/shared contracts are now re-centered into `apps/favn_core/lib/favn/`.
- Phase 3 modules now owned in `apps/favn_core/lib/favn/` include:
  - `manifest.ex`
  - `ref.ex`
  - `relation_ref.ex`
  - `timezone.ex`
  - `diagnostic.ex`
  - `asset/dependency.ex`
  - `asset/relation_input.ex`
  - `asset/relation_resolver.ex`
  - `assets/graph_index.ex`
  - `assets/planner.ex`
  - `assets/compiler.ex`
  - `assets/dependency_inference.ex`
  - `dsl/compiler.ex`
  - `connection/definition.ex`
  - `pipeline/definition.ex`
  - `pipeline/resolver.ex`
  - `pipeline/resolution.ex`
  - `plan.ex`
  - `sql/definition.ex`
  - `sql/source.ex`
  - `sql/template.ex`
  - `sql_asset/definition.ex`
  - `sql_asset/compiler.ex`
  - `sql_asset/materialization.ex`
  - `sql_asset/relation_usage.ex`
  - `window/spec.ex`
  - `window/anchor.ex`
  - `window/runtime.ex`
  - `window/key.ex`
  - `window/validate.ex`
  - `triggers/schedule.ex`
  - `manifest/build.ex`
  - `manifest/asset.ex`
  - `manifest/pipeline.ex`
  - `manifest/schedule.ex`
  - `manifest/catalog.ex`
  - `manifest/generator.ex`
  - `manifest/graph.ex`
  - `manifest/serializer.ex`
  - `manifest/identity.ex`
  - `manifest/compatibility.ex`
  - `manifest/version.ex`
  - `contracts/runner_work.ex`
  - `contracts/runner_result.ex`
  - `contracts/runner_event.ex`
- Intended steady-state ownership remains: `favn` public surface, `favn_core` internal compiler/manifest/planning/contracts, `favn_runner` execution, `favn_orchestrator` control plane, and `favn_view` via orchestrator APIs only.
- Phase 3 populated `apps/favn_core/lib/favn/` with canonical manifest schema/versioning, serializer/hash/compatibility logic, graph/planning helpers, SQL helper internals, and shared runner/orchestrator contract structs.
- New runtime/DSL ownership should continue moving from `favn_legacy` to owner apps by bounded slice in later phases.
