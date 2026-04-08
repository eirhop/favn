# Library Folder Structure (`lib/`)

This document maps the current `lib/` layout for the Favn core library.

```text
lib/
├── favn.ex
└── favn/
    ├── application.ex
    ├── asset.ex
    ├── assets.ex
    ├── assets/
    │   ├── compiler.ex
    │   ├── graph_index.ex
    │   ├── planner.ex
    │   └── registry.ex
    ├── connection.ex
    ├── connection/
    │   ├── definition.ex
    │   ├── error.ex
    │   ├── loader.ex
    │   ├── registry.ex
    │   ├── resolved.ex
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
