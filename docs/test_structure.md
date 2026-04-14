# Test Folder Structure (`apps/*/test`)

This document maps the umbrella test layout after the v0.5 Phase 1 scaffold.

```text
apps/
├── favn/test/
│   ├── test_helper.exs
│   └── favn_test.exs
├── favn_core/test/
│   ├── test_helper.exs
│   └── favn_core_test.exs
├── favn_runner/test/
│   ├── test_helper.exs
│   └── favn_runner_test.exs
├── favn_orchestrator/test/
│   ├── test_helper.exs
│   └── favn_orchestrator_test.exs
├── favn_view/test/
│   ├── test_helper.exs
│   └── favn_view_test.exs
├── favn_storage_postgres/test/
│   ├── test_helper.exs
│   └── favn_storage_postgres_test.exs
├── favn_storage_sqlite/test/
│   ├── test_helper.exs
│   └── favn_storage_sqlite_test.exs
├── favn_duckdb/test/
│   ├── test_helper.exs
│   └── favn_duckdb_test.exs
├── favn_test_support/test/
│   ├── test_helper.exs
│   └── favn_test_support_test.exs
└── favn_legacy/test/
    ├── asset_test.exs
    ├── assets_test.exs
    ├── connection_test.exs
    ├── events_test.exs
    ├── favn_test.exs
    ├── freshness_test.exs
    ├── graph_index_test.exs
    ├── memory_storage_semantics_test.exs
    ├── multi_asset_test.exs
    ├── pipeline_sqlite_smoke_test.exs
    ├── pipeline_test.exs
    ├── planner_test.exs
    ├── postgres_run_serializer_test.exs
    ├── postgres_storage_adapter_test.exs
    ├── postgres_storage_integration_test.exs
    ├── public_docs_test.exs
    ├── ref_test.exs
    ├── runner_test.exs
    ├── runtime_projector_test.exs
    ├── runtime_telemetry_test.exs
    ├── runtime_transitions_test.exs
    ├── scheduler_cron_test.exs
    ├── scheduler_test.exs
    ├── sql_asset_runtime_test.exs
    ├── sql_asset_test.exs
    ├── sql_dependency_inference_test.exs
    ├── sql_dsl_test.exs
    ├── sql_duckdb_adapter_hardening_test.exs
    ├── sql_duckdb_adapter_test.exs
    ├── sql_template_asset_ref_test.exs
    ├── sql_template_ir_test.exs
    ├── sql_test.exs
    ├── sqlite_storage_bootstrap_test.exs
    ├── sqlite_storage_test.exs
    ├── storage_test.exs
    ├── support/
    │   ├── favn_test_setup.ex
    │   └── fixtures/assets/
    │       ├── basic_assets.ex
    │       ├── graph_assets.ex
    │       ├── pipeline_assets.ex
    │       └── runner_assets.ex
    ├── test_helper.exs
    ├── triggers_schedules_test.exs
    └── window_test.exs
```
