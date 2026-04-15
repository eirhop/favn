# Test Folder Structure (`apps/*/test`)

This document maps the umbrella test layout after v0.5 Phase 1.

```text
apps/
├── favn/test/
│   ├── favn_test.exs
│   ├── dsl_compiler_test.exs
│   ├── manifest_generator_test.exs
│   └── test_helper.exs
├── favn_core/test/
│   ├── favn_core_test.exs
│   └── test_helper.exs
├── favn_runner/test/
│   ├── favn_runner_test.exs
│   └── test_helper.exs
├── favn_orchestrator/test/
│   ├── favn_orchestrator_test.exs
│   └── test_helper.exs
├── favn_view/test/
│   ├── favn_view_test.exs
│   └── test_helper.exs
├── favn_storage_postgres/test/
│   ├── favn_storage_postgres_test.exs
│   └── test_helper.exs
├── favn_storage_sqlite/test/
│   ├── favn_storage_sqlite_test.exs
│   └── test_helper.exs
├── favn_duckdb/test/
│   ├── favn_duckdb_test.exs
│   └── test_helper.exs
├── favn_test_support/test/
│   ├── favn_test_support_test.exs
│   └── test_helper.exs
└── favn_legacy/test/
    ├── asset_test.exs
    ├── assets_test.exs
    ├── connection_test.exs
    ├── favn_test.exs
    ├── pipeline_test.exs
    ├── runner_test.exs
    ├── scheduler_test.exs
    ├── sql_asset_test.exs
    ├── sql_test.exs
    ├── storage_test.exs
    ├── window_test.exs
    ├── support/
    │   ├── favn_test_setup.ex
    │   └── fixtures/assets/
    └── test_helper.exs
```

Notes:

- Most runtime coverage remains in `apps/favn_legacy/test` until slices are migrated.
- Each migrated slice must move or recreate tests in the new owner app without dual-compiling namespace owners.
- During Phase 2, umbrella `mix test` runs migrated owner-app suites first; legacy runtime suites remain available in `apps/favn_legacy/test` as reference coverage.
- `apps/favn_test_support` is the shared home for cross-app fixtures, helpers, builders, and file fixtures used during migration.
- Umbrella apps may depend on `favn_test_support` only with `only: :test`.
- Fixtures used by only one app should stay in that app's local `test/support` directory.
