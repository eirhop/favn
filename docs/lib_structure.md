# Library Folder Structure (`lib/`)

This document maps the current `lib/` layout for the Favn core library.

```text
lib/
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
    │   ├── adapter.ex
    │   ├── adapter/
    │   │   ├── memory.ex
    │   │   └── sqlite.ex
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
