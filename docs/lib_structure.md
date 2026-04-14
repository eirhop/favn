# Library Folder Structure (`apps/*/lib`)

This document maps the umbrella library layout after the v0.5 Phase 1 scaffold.

```text
apps/
├── favn/lib/
│   └── favn.ex
├── favn_core/lib/
│   └── favn_core.ex
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
    ├── backfill.ex
    ├── diagnostic.ex
    ├── dsl/
    │   └── compiler.ex
    ├── assets.ex
    ├── namespace.ex
    ├── multi_asset.ex
    ├── relation_ref.ex
    ├── assets/
    │   ├── compiler.ex
    │   ├── dependency_inference.ex
    │   ├── graph_index.ex
    │   ├── planner.ex
    │   └── registry.ex
    ├── asset/
    │   ├── dependency.ex
    │   ├── relation_resolver.ex
    │   └── relation_input.ex
    ├── connection.ex
    ├── connection/
    │   ├── definition.ex
    │   ├── error.ex
    │   ├── info.ex
    │   ├── loader.ex
    │   ├── registry.ex
    │   ├── resolved.ex
    │   ├── sanitizer.ex
    │   └── validator.ex
    ├── freshness.ex
    ├── pipeline.ex
    ├── pipeline/
    │   ├── definition.ex
    │   ├── resolution.ex
    │   └── resolver.ex
    ├── plan.ex
    ├── ref.ex
    ├── run.ex
    ├── run/
    │   ├── asset_result.ex
    │   └── context.ex
    ├── runtime/
    │   ├── coordinator.ex
    │   ├── engine.ex
    │   ├── events.ex
    │   ├── executor.ex
    │   ├── executor/
    │   │   └── local.ex
    │   ├── manager.ex
    │   ├── projector.ex
    │   ├── run_supervisor.ex
    │   ├── state.ex
    │   ├── step_state.ex
    │   ├── telemetry.ex
    │   └── transitions/
    │       ├── run.ex
    │       └── step.ex
    ├── scheduler.ex
    ├── submission.ex
    ├── sql.ex
    ├── sql_asset.ex
    ├── sql/
    │   ├── adapter.ex
    │   ├── capabilities.ex
    │   ├── column.ex
    │   ├── definition.ex
    │   ├── error.ex
    │   ├── incremental_window.ex
    │   ├── materialization_planner.ex
    │   ├── render.ex
    │   ├── relation.ex
    │   ├── relation_ref.ex
    │   ├── result.ex
    │   ├── session.ex
    │   ├── source.ex
    │   ├── template.ex
    │   ├── write_plan.ex
    │   └── adapter/
    │       ├── duckdb.ex
    │       └── duckdb/
    │           ├── client.ex
    │           ├── error_mapper.ex
    │           └── client/
    │               └── duckdbex.ex
    ├── sql_asset/
    │   ├── compiler.ex
    │   ├── definition.ex
    │   ├── error.ex
    │   ├── input.ex
    │   ├── materialization.ex
    │   ├── relation_usage.ex
    │   ├── renderer.ex
    │   └── runtime.ex
    ├── scheduler/
    │   ├── cron.ex
    │   ├── registry.ex
    │   ├── runtime.ex
    │   ├── state.ex
    │   ├── storage.ex
    │   └── supervisor.ex
    ├── storage.ex
    ├── storage/
    │   ├── run_serializer.ex
    │   ├── run_write_semantics.ex
    │   ├── snapshot_hash.ex
    │   ├── term_json.ex
    │   ├── adapter.ex
    │   ├── adapter/
    │   │   ├── memory.ex
    │   │   ├── postgres.ex
    │   │   └── sqlite.ex
    │   ├── postgres/
    │   │   ├── migrations.ex
    │   │   ├── migrations/
    │   │   │   └── create_foundation.ex
    │   │   ├── repo.ex
    │   │   ├── supervisor.ex
    │   └── sqlite/
    │       ├── migrations.ex
    │       ├── migrations/
    │       │   └── create_runs.ex
    │       ├── repo.ex
    │       └── supervisor.ex
    ├── timezone.ex
    ├── triggers/
    │   ├── schedule.ex
    │   └── schedules.ex
        └── window/
            ├── anchor.ex
            ├── key.ex
            ├── runtime.ex
            ├── spec.ex
            └── validate.ex
```
