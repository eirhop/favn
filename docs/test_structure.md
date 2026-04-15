# Test Folder Structure (`apps/*/test`)

This document maps the umbrella test layout during the Phase 2 -> Phase 3 transition.

```text
apps/
├── favn/test/
│   ├── favn_test.exs
│   ├── dsl_compiler_test.exs
│   ├── manifest_generator_test.exs
│   └── test_helper.exs
├── favn_core/test/
│   ├── favn_core_test.exs
│   ├── value_objects_test.exs
│   ├── window_schedule_test.exs
│   ├── asset_and_dsl_test.exs
│   ├── manifest/
│   ├── contracts/
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
- The current umbrella `mix test` alias shape is migration-oriented and not the final CI/test contract.
- Test execution should be simplified again after ownership and runtime boundaries settle in later phases.
- `apps/favn_test_support` is the shared home for cross-app fixtures, helpers, builders, and file fixtures used during migration.
- Umbrella apps may depend on `favn_test_support` only with `only: :test`.
- Fixtures used by only one app should stay in that app's local `test/support` directory.
- Phase 3 should grow `apps/favn_core/test` with manifest schema, manifest versioning, serializer, compatibility, graph, and shared contract tests.
- `apps/favn/test` should stay focused on public DSL/facade coverage once internal compiler/manifest tests move down into `favn_core`.
- Initial Phase 3 tests now exist under `apps/favn_core/test/manifest/` and `apps/favn_core/test/contracts/`.
- `apps/favn_core/test/manifest/` now includes `build_test.exs` and `graph_test.exs` in addition to serializer/version/identity/compatibility coverage.
- `apps/favn_core/test/contracts/contract_lock_test.exs` now locks runner contract key shapes before Phase 4 runner work.
- Boundary-leak cleanup coverage now includes `apps/favn_core/test/boundary_explicit_inputs_test.exs` and `apps/favn/test/boundary_defaults_test.exs`.
