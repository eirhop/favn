# Library Folder Structure (`apps/*/lib`)

This document maps the umbrella library layout after v0.5 Phase 1.

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
- New runtime/DSL ownership should move from `favn_legacy` to owner apps by bounded slice in later phases.
