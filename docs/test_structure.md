# Test Folder Structure (`apps/*/test`)

This document maps the current umbrella test layout during the v0.5 refactor.

```text
apps/
├── favn/test/
│   ├── boundary_defaults_test.exs
│   ├── dsl_compiler_test.exs
│   ├── favn_test.exs
│   ├── manifest_generator_test.exs
│   ├── public_authoring_parity_test.exs
│   ├── public_pipeline_parity_test.exs
│   ├── runtime_facade_test.exs
│   └── test_helper.exs
├── favn_local/test/
│   ├── dev_config_test.exs
│   ├── dev_lifecycle_test.exs
│   ├── dev_lock_test.exs
│   ├── dev_orchestrator_client_test.exs
│   ├── dev_process_test.exs
│   ├── dev_reload_test.exs
│   ├── dev_state_test.exs
│   ├── dev_status_test.exs
│   ├── dev_stop_test.exs
│   ├── mix_tasks/
│   │   └── favn_dev_task_test.exs
│   ├── integration/
│   │   └── dev_stack_smoke_test.exs
│   └── test_helper.exs
├── favn_core/test/
│   ├── favn_core_test.exs
│   ├── value_objects_test.exs
│   ├── window_schedule_test.exs
│   ├── asset_and_dsl_test.exs
│   ├── assets/
│   │   ├── compiler_parity_test.exs
│   │   └── graph_planner_parity_test.exs
│   ├── pipeline/
│   │   └── resolver_parity_test.exs
│   ├── manifest/
│   ├── contracts/
│   └── test_helper.exs
├── favn_runner/test/
│   ├── favn_runner_test.exs
│   └── test_helper.exs
├── favn_orchestrator/test/
│   ├── events_test.exs
│   ├── manifest_store_test.exs
│   ├── orchestrator_runner_integration_test.exs
│   ├── projector_test.exs
│   ├── runner_client/
│   │   └── local_node_test.exs
│   ├── run_manager_test.exs
│   ├── run_server_test.exs
│   ├── scheduler/
│   │   └── runtime_test.exs
│   ├── storage/
│   │   ├── manifest_codec_test.exs
│   │   ├── memory_adapter_test.exs
│   │   ├── run_event_codec_test.exs
│   │   ├── run_state_codec_test.exs
│   │   ├── scheduler_state_codec_test.exs
│   │   └── write_semantics_test.exs
│   ├── storage_facade_test.exs
│   └── test_helper.exs
├── favn_storage_postgres/test/
│   ├── favn_storage_postgres_test.exs
│   └── test_helper.exs
├── favn_storage_sqlite/test/
│   ├── adapter_test.exs
│   ├── sqlite_storage_bootstrap_test.exs
│   ├── sqlite_storage_test.exs
│   └── test_helper.exs
├── favn_duckdb/test/
│   ├── favn_duckdb_test.exs
│   └── test_helper.exs
├── favn_test_support/test/
│   ├── fixtures_test.exs
│   └── test_helper.exs
```

Notes:

- Each migrated slice must move or recreate tests in the new owner app without dual-compiling namespace owners.
- Legacy and same-BEAM view tests are deleted; supported coverage now lives in the owner-app suites.
- The current umbrella `mix test` alias includes the owner apps plus `apps/favn_local/test`.
- `apps/favn_test_support` is the shared home for cross-app fixtures, helpers, builders, and file fixtures used during migration.
- shared fixture source for migration parity now lives under `apps/favn_test_support/priv/fixtures/assets/` and is loaded via `FavnTestSupport.Fixtures`.
- batch 1 parity migration moved broad authoring/compiler/planning/window ownership into `apps/favn/test` and `apps/favn_core/test`.
- Umbrella apps may depend on `favn_test_support` only with `only: :test`.
- Fixtures used by only one app should stay in that app's local `test/support` directory.
- Phase 3 should grow `apps/favn_core/test` with manifest schema, manifest versioning, serializer, compatibility, graph, and shared contract tests.
- `apps/favn/test` should stay focused on public DSL/facade coverage once internal compiler/manifest tests move down into `favn_core`.
- Initial Phase 3 tests now exist under `apps/favn_core/test/manifest/` and `apps/favn_core/test/contracts/`.
- `apps/favn_core/test/manifest/` now includes `build_test.exs` and `graph_test.exs` in addition to serializer/version/identity/compatibility coverage.
- `apps/favn_core/test/contracts/contract_lock_test.exs` now locks runner contract key shapes before Phase 4 runner work.
- Boundary-leak cleanup coverage now includes `apps/favn_core/test/boundary_explicit_inputs_test.exs` and `apps/favn/test/boundary_defaults_test.exs`.
- Phase 4 grew `apps/favn_runner/test/` around manifest registration, manifest resolution, worker execution, connection runtime ownership, SQL asset execution, and same-node runner integration.
- Phase 4 also added `apps/favn_core/test/run/` for the shared `Favn.Run.Context` and `Favn.Run.AssetResult` contracts after moving them out of legacy.
- Initial Phase 4 tests now include:
  - `apps/favn_core/test/run/context_test.exs`
  - `apps/favn_core/test/run/asset_result_test.exs`
  - `apps/favn_runner/test/manifest_store_test.exs`
  - `apps/favn_runner/test/manifest_resolver_test.exs`
  - `apps/favn_runner/test/server_test.exs`
  - `apps/favn_runner/test/worker_test.exs`
  - `apps/favn_runner/test/execution/sql_asset_test.exs`
  - `apps/favn_runner/test/favn_runner_test.exs` for same-node manifest registration + runner execution flow
- Initial Phase 5 core-planning tests now include:
  - `apps/favn_core/test/contracts/runner_client_test.exs`
  - `apps/favn_core/test/manifest/index_test.exs`
  - `apps/favn_core/test/manifest/pipeline_resolver_test.exs`
- Initial Phase 5 orchestrator runtime tests now include:
  - `apps/favn_orchestrator/test/integration/storage_adapter_contract_test.exs`
  - `apps/favn_orchestrator/test/storage/manifest_codec_test.exs`
  - `apps/favn_orchestrator/test/storage/run_state_codec_test.exs`
  - `apps/favn_orchestrator/test/storage/run_event_codec_test.exs`
  - `apps/favn_orchestrator/test/storage/scheduler_state_codec_test.exs`
  - `apps/favn_orchestrator/test/storage/memory_adapter_test.exs`
  - `apps/favn_orchestrator/test/storage/write_semantics_test.exs`
  - `apps/favn_orchestrator/test/storage_facade_test.exs`
  - `apps/favn_orchestrator/test/manifest_store_test.exs`
  - `apps/favn_orchestrator/test/run_manager_test.exs`
  - `apps/favn_orchestrator/test/run_server_test.exs`
  - `apps/favn_orchestrator/test/scheduler/runtime_test.exs`
- Initial Phase 6 SQLite adapter tests now include:
  - `apps/favn_storage_sqlite/test/adapter_test.exs`
- Initial Phase 6 Postgres adapter tests now include:
  - `apps/favn_storage_postgres/test/adapter_test.exs`
  - `apps/favn_storage_postgres/test/integration/adapter_live_test.exs` (opt-in via `FAVN_POSTGRES_TEST_URL`)
- Public facade runtime delegation tests now include:
  - `apps/favn/test/runtime_facade_test.exs`
  - coverage for scheduler runtime wrapper availability semantics in `apps/favn/test/runtime_facade_test.exs`
- Phase 7 runner/plugin tests now include:
  - `apps/favn_runner/test/execution/sql_asset_test.exs` for manifest-pinned SQL execution through both `:in_process` and `:separate_process` DuckDB modes
  - `apps/favn_runner/test/plugin_test.exs` for generic runner plugin config normalization
  - `apps/favn_duckdb/test/favn_duckdb_test.exs` for DuckDB plugin child specs, separate worker lifecycle behavior, and separate-process client behavior
- Runner execution parity migration batch 2 expanded owner-app coverage with:
  - `apps/favn_runner/test/connection/loader_test.exs` for runner-owned connection runtime config loading/validation behavior
  - `apps/favn_runner/test/sql/runtime_bridge_test.exs` for runtime SQL connection/session/operation semantics
  - `apps/favn_runner/test/worker_test.exs` expanded failure-shape coverage (`raise`/`throw`/`exit`/invalid return/arity mismatch)
  - `apps/favn_runner/test/server_test.exs` expanded execution timeout/not-found/input-validation behavior
  - `apps/favn_runner/test/execution/sql_asset_test.exs` expanded manifest SQL runtime failure-path coverage
- Control-plane/runtime-state parity migration batch 3 expanded owner-app coverage with:
  - `apps/favn_orchestrator/test/orchestrator_runner_integration_test.exs`
  - `apps/favn_orchestrator/test/projector_test.exs`
  - `apps/favn_orchestrator/test/run_manager_test.exs`
  - `apps/favn_orchestrator/test/run_server_test.exs`
  - `apps/favn_orchestrator/test/scheduler/runtime_test.exs`
  - `apps/favn_orchestrator/test/storage_facade_test.exs`
  - `apps/favn_storage_sqlite/test/sqlite_storage_test.exs`
  - `apps/favn/test/runtime_facade_test.exs`
- Batch 3 also migrated remaining storage semantics parity from legacy into owner suites:
  - `apps/favn_orchestrator/test/storage/memory_adapter_test.exs`
  - `apps/favn_storage_postgres/test/adapter_test.exs`
  - `apps/favn_storage_postgres/test/integration/adapter_live_test.exs`
- Shared test setup used by owner apps now lives in `apps/favn_test_support/lib/favn_test_support/test_setup.ex` (`Favn.TestSetup`).
- DuckDB plugin parity migration batch 2 expanded owner-app coverage with:
  - `apps/favn_duckdb/test/sql/adapter/duckdb_hardening_test.exs` for DuckDB adapter transaction/appender/resource-cleanup hardening semantics
  - `apps/favn_duckdb/test/favn_duckdb_test.exs` expanded runtime option-default and invalid-handle behavior
- Phase 8 web/orchestrator boundary test additions are tracked in `docs/refactor/PHASE_8_TODO.md`.
- Initial Phase 8 orchestrator live-event boundary tests now include:
  - `apps/favn_orchestrator/test/events_test.exs`
- Phase 8/9 emphasis stays on orchestrator HTTP contract tests, auth/authz tests, SSE replay tests, service-auth tests, and thin web smoke tests.
- Initial Phase 8 boundary-correction API/auth coverage now includes:
  - `apps/favn_orchestrator/test/api/router_test.exs`
  - `apps/favn_orchestrator/test/api/config_test.exs`
  - `apps/favn_orchestrator/test/api/router_test.exs` now also covers schedule list/detail reads, command/authz paths, run-scoped SSE replay, and actor admin read/management authz cases
  - `apps/favn_orchestrator/test/http_contract/schema_test.exs` for orchestrator-owned machine-readable schema lock coverage
- Initial Phase 8 `favn_web` auth/session E2E coverage now includes:
  - `web/favn_web/tests/e2e/auth-session-runs.e2e.ts`
  - `web/favn_web/tests/e2e/mock-orchestrator-server.mjs` (deterministic local orchestrator mock used during Playwright runs)
  - `web/favn_web/tests/e2e/auth-session-runs.e2e.ts` now also covers thin operator smoke over `/api/web/v1/**` (runs/manifests/schedules commands + run stream relay validation)
