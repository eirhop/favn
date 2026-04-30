# Library Folder Structure (`apps/*/lib`)

This document maps the current umbrella library layout after the v0.5 refactor closeout.

App-level technical docs:

- `apps/favn_local/README.md`

Top-level product docs:

- `docs/FEATURES.md` for implemented capabilities
- `docs/FEATURE_AUDIT_TASKLIST.md` for the current feature audit work breakdown
- `docs/ISSUE_171_SOURCE_RAW_LANDING_PLAN.md` for the source-system raw landing implementation plan and reference notes
- `docs/ISSUE_172_LOCAL_INSPECTION_PANEL_PLAN.md` for the planned safe local landed-data inspection panel implementation
- `docs/ISSUE_173_PIPELINE_WINDOW_POLICY_PLAN.md` for the planned pipeline window policy implementation
- `docs/ROADMAP.md` for planned work
- `docs/DUCKLAKE_CONNECTION_BOOTSTRAP_PLAN.md` for issue 170 implementation planning

Consumer examples:

- `examples/basic-workflow-tutorial` is a standalone Mix project outside the
  umbrella apps. Its `lib/` tree demonstrates a complete Favn workflow using
  local path dependencies back to `apps/favn` and `apps/favn_duckdb`.

```text
apps/
├── favn/lib/
│   ├── favn/
│   │   ├── ai.ex
│   │   └── sql_client.ex
│   ├── favn.ex
│   └── mix/tasks/
│       ├── favn.backfill.ex
│       ├── favn.build.orchestrator.ex
│       ├── favn.build.runner.ex
│       ├── favn.build.single.ex
│       ├── favn.build.web.ex
│       ├── favn.dev.ex
│       ├── favn.doctor.ex
│       ├── favn.init.ex
│       ├── favn.install.ex
│       ├── favn.logs.ex
│       ├── favn.read_doc.ex
│       ├── favn.reload.ex
│       ├── favn.reset.ex
│       ├── favn.run.ex
│       ├── favn.status.ex
│       └── favn.stop.ex
├── favn_authoring/lib/
│   ├── favn.ex
│   ├── favn_authoring/
│   │   └── doc_reader.ex
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
│       └── (authoring entrypoints)
├── favn_local/lib/
│   ├── favn_local.ex
│   ├── favn/
│   │   ├── dev.ex
│   │   └── dev/
│   │       ├── backfill.ex
│   │       ├── build/
│   │       │   ├── orchestrator.ex
│   │       │   ├── runner.ex
│   │       │   ├── single.ex
│   │       │   └── web.ex
│   │       ├── config.ex
│   │       ├── consumer_config_transport.ex
│   │       ├── doctor.ex
│   │       ├── init.ex
│   │       ├── install.ex
│   │       ├── lock.ex
│   │       ├── local_http_client.ex
│   │       ├── logs.ex
│   │       ├── node_control.ex
│   │       ├── orchestrator_client.ex
│   │       ├── paths.ex
│   │       ├── process.ex
│   │       ├── reload.ex
│   │       ├── reset.ex
│   │       ├── run.ex
│   │       ├── runtime_launch.ex
│   │       ├── runtime_source.ex
│   │       ├── runtime_tree_policy.ex
│   │       ├── runtime_workspace.ex
│   │       ├── runner_control.ex
│   │       ├── consumer_code_path.ex
│   │       ├── secrets.ex
│   │       ├── stack.ex
│   │       ├── state.ex
│   │       └── status.ex
├── favn_core/lib/
│   ├── favn_core.ex
│   └── favn/
├── favn_runner/lib/
│   ├── favn_runner.ex
│   └── favn_runner/application.ex
├── favn_sql_runtime/lib/
│   ├── favn_sql_runtime.ex
│   ├── favn_sql_runtime/application.ex
│   └── favn/
│       ├── connection/
│       └── sql/
├── favn_orchestrator/lib/
│   ├── favn_orchestrator.ex
│   └── favn_orchestrator/
│       ├── application.ex
│       └── runner_client/
│           └── local_node.ex
├── favn_storage_postgres/lib/
│   └── favn_storage_postgres.ex
├── favn_storage_sqlite/lib/
│   └── favn_storage_sqlite.ex
├── favn_duckdb/lib/
│   └── favn_duckdb.ex
├── favn_test_support/lib/
│   ├── favn_test_support.ex
│   └── favn_test_support/
│       └── fixtures.ex
```

Notes:

- `apps/favn_legacy` and `apps/favn_view` were removed in the Phase 10 deletion pass.
- `apps/favn` now owns the thin public package boundary and public `mix favn.*` entrypoints.
- `apps/favn/lib/favn/ai.ex` is the AI-oriented compiled-doc starting point for choosing the right public Favn module to read next.
- `apps/favn_authoring` now owns authoring/manifest-facing implementation internals.
- `apps/favn_local` continues to own local lifecycle/tooling implementation internals.
- `apps/favn_local/lib/favn/dev/backfill.ex` owns the local operational-backfill workflow behind `mix favn.backfill`, including active-manifest pipeline target resolution, local operator authentication, submission, state reads, and failed-window rerun forwarding.
- `apps/favn_local/lib/favn/dev/consumer_config_transport.ex` owns the local-only runner transport for explicitly supported consumer `:favn` config keys and redacted diagnostics.
- `apps/favn_local/lib/favn/dev/init.ex` owns the idempotent `mix favn.init --duckdb --sample` local consumer scaffold, while `apps/favn_local/lib/favn/dev/doctor.ex` owns setup validation before running local tooling.
- Internal compiler/manifest/planning/shared contracts are now re-centered into `apps/favn_core/lib/favn/`.
- Phase 3 modules now owned in `apps/favn_core/lib/favn/` include:
  - `backfill/lookback_policy.ex`
  - `backfill/range_request.ex`
  - `backfill/range_resolver.ex`
  - `manifest.ex`
  - `ref.ex`
  - `relation_ref.ex`
  - `runtime_config/error.ex`
  - `runtime_config/ref.ex`
  - `runtime_config/redactor.ex`
  - `runtime_config/requirements.ex`
  - `runtime_config/resolver.ex`
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
  - `pipeline/selector_normalizer.ex`
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
  - `window/policy.ex`
  - `window/request.ex`
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
  - `manifest/rehydrate.ex`
  - `manifest/version.ex`
  - `contracts/runner_work.ex`
  - `contracts/runner_result.ex`
  - `contracts/runner_event.ex`
  - `contracts/relation_inspection_request.ex`
  - `contracts/relation_inspection_result.ex`
- Intended steady-state ownership is now: `favn` public surface, `favn_core` internal compiler/manifest/planning/contracts, `favn_runner` execution, `favn_orchestrator` control plane plus auth/authz/audit, and a separate `favn_web` tier outside the umbrella talking to orchestrator over a remote boundary.
- Phase 3 populated `apps/favn_core/lib/favn/` with canonical manifest schema/versioning, serializer/hash/compatibility logic, graph/planning helpers, SQL helper internals, and shared runner/orchestrator contract structs.
- Phase 4 implementation grew `apps/favn_runner/lib/favn_runner/` around a small execution-owned set of modules such as a runner server, manifest store/resolver, worker supervision, and context builder.
- Runner-owned local inspection now includes `apps/favn_runner/lib/favn_runner/inspection.ex` for safe read-only relation previews against pinned manifest assets.
- Phase 4 implementation moved `Favn.Run.Context` and `Favn.Run.AssetResult` out of `favn_legacy` into `apps/favn_core/lib/favn/run/`, while runner-owned connection and SQL runtime modules moved into `apps/favn_runner/lib/favn/` under preserved `Favn.*` names.
- Initial Phase 5 implementation added manifest-native orchestrator planning helpers and shared runner-client behaviour in `apps/favn_core/lib/favn/`:
  - `contracts/runner_client.ex`
  - `manifest/index.ex`
  - `manifest/pipeline_resolver.ex`
- Initial Phase 5 orchestrator runtime slice added control-plane modules in `apps/favn_orchestrator/lib/favn_orchestrator/`:
  - `storage.ex`
  - `storage/manifest_codec.ex`
  - `storage/payload_codec.ex`
  - `storage/run_state_codec.ex`
  - `storage/run_event_codec.ex`
  - `storage/scheduler_state_codec.ex`
  - `storage/write_semantics.ex`
  - `storage/adapter/memory.ex`
  - `manifest_store.ex`
  - `run_state.ex`
  - `projector.ex`
  - `run_manager.ex`
  - `run_server.ex`
- Issue 168 storage-contract preparation added orchestrator-owned normalized backfill persistence shapes under `apps/favn_orchestrator/lib/favn_orchestrator/backfill/`.
  - `coverage_baseline.ex`
  - `coverage_projector.ex`
  - `backfill_window.ex`
  - `asset_window_state.ex`
  - `projector.ex`
- Issue 168 pure backfill planning added `apps/favn_core/lib/favn/backfill/` for range requests, range resolution, and lookback-policy normalization.
- Issue 168 parent/child backfill submission added `apps/favn_orchestrator/lib/favn_orchestrator/backfill_manager.ex` as the orchestrator-owned internal submitter used by the orchestrator facade and private HTTP API for pipeline backfills.
- Issue 168 storage persistence added backfill-state migration modules in the SQL storage apps.
  - `apps/favn_storage_sqlite/lib/favn_storage_sqlite/migrations/add_backfill_state.ex`
  - `apps/favn_storage_postgres/lib/favn_storage_postgres/migrations/add_backfill_state.ex`
- Phase 5 scheduler/runtime expansion added:
  - `scheduler/cron.ex`
  - `scheduler/manifest_entries.ex`
  - `scheduler/runtime.ex`
- Public run projection is now served through orchestrator-backed reads using the existing `%Favn.Run{}` contract.
- Preserved public contracts now owned from `apps/favn_orchestrator/lib/favn/` include:
  - `run.ex`
  - `storage.ex`
  - `storage/adapter.ex`
  - `scheduler.ex`
  - `scheduler/state.ex`
- `apps/favn_orchestrator/lib/favn/storage/adapter.ex` is now the single authoritative low-level storage adapter behaviour used by both `Favn.Storage` and `FavnOrchestrator.Storage`.
- `apps/favn_storage_sqlite/lib/` now includes the initial Phase 6 adapter foundation:
  - `favn_storage_sqlite.ex`
  - `favn/storage/adapter/sqlite.ex`
  - `favn_storage_sqlite/repo.ex`
  - `favn_storage_sqlite/supervisor.ex`
  - `favn_storage_sqlite/migrations.ex`
  - `favn_storage_sqlite/migrations/create_foundation.ex`
- `apps/favn_storage_postgres/lib/` now includes the initial Phase 6 adapter foundation:
  - `favn_storage_postgres.ex`
  - `favn/storage/adapter/postgres.ex`
  - `favn_storage_postgres/repo.ex`
  - `favn_storage_postgres/supervisor.ex`
  - `favn_storage_postgres/migrations.ex`
  - `favn_storage_postgres/migrations/create_foundation.ex`
- Phase 10 closeout re-established the preserved stable adapter entrypoints under `Favn.Storage.Adapter.SQLite` and `Favn.Storage.Adapter.Postgres` while keeping adapter-app repo/supervisor ownership inside `favn_storage_sqlite` and `favn_storage_postgres`.
- Phase 10 closeout also replaced SQL adapter BEAM term payload persistence with shared inspectable `json-v1` payload codecs owned by `apps/favn_orchestrator/lib/favn_orchestrator/storage/payload_codec.ex`.
- Initial Phase 4 implementation now includes:
  - `apps/favn_core/lib/favn/run/context.ex`
  - `apps/favn_core/lib/favn/run/asset_result.ex`
  - `apps/favn_runner/lib/favn_runner/server.ex`
  - `apps/favn_runner/lib/favn_runner/manifest_store.ex`
  - `apps/favn_runner/lib/favn_runner/manifest_resolver.ex`
  - `apps/favn_runner/lib/favn_runner/context_builder.ex`
  - `apps/favn_runner/lib/favn_runner/worker.ex`
  - `apps/favn_runner/lib/favn_runner/event_sink.ex`
- Shared connection runtime ownership now lives in `apps/favn_sql_runtime/lib/favn/connection/`.
- Runtime configuration references and resolution helpers now live in `apps/favn_core/lib/favn/runtime_config/`; authoring declares references, manifests serialize references only, and runner/connection runtime code resolves values at execution time.
- Shared SQL runtime/session contracts now live in `apps/favn_sql_runtime/lib/favn/sql/` including `Favn.SQL.Client`, `Favn.SQL.RuntimeBridge`, `Favn.SQL.ConcurrencyPolicy`, and `Favn.SQL.Admission`.
- Runner-owned SQL asset execution modules remain under `apps/favn_runner/lib/favn/sql/` and `apps/favn_runner/lib/favn/sql_asset/`.
- Phase 7 implementation now moves DuckDB ownership into `apps/favn_duckdb/lib/` including:
  - `apps/favn_duckdb/lib/favn/sql/adapter/duckdb.ex`
  - `apps/favn_duckdb/lib/favn/sql/adapter/duckdb/bootstrap.ex`
  - `apps/favn_duckdb/lib/favn/sql/adapter/duckdb/client.ex`
  - `apps/favn_duckdb/lib/favn/sql/adapter/duckdb/client/duckdbex.ex`
  - `apps/favn_duckdb/lib/favn/sql/adapter/duckdb/error_mapper.ex`
  - `apps/favn_duckdb/lib/favn_duckdb/runtime.ex`
  - `apps/favn_duckdb/lib/favn_duckdb/runtime/in_process.ex`
  - `apps/favn_duckdb/lib/favn_duckdb/runtime/separate_process.ex`
  - `apps/favn_duckdb/lib/favn_duckdb/worker.ex`
- `apps/favn_runner/lib/favn_runner/plugin.ex` now owns the minimal generic plugin boundary used to load plugin child specs with plugin-local options.
- `apps/favn_core/lib/favn/manifest/sql_execution.ex` now carries manifest SQL execution payload for SQL assets.
- Phase 8 web/orchestrator boundary planning docs now live in:
  - `docs/refactor/PHASE_8_WEB_ORCHESTRATOR_BOUNDARY_PLAN.md`
  - `docs/refactor/PHASE_8_TODO.md`
- Phase 8 orchestrator live-event boundary now begins with:
  - `apps/favn_orchestrator/lib/favn_orchestrator/events.ex`
  - `apps/favn_orchestrator/lib/favn_orchestrator/run_event.ex`
  - `apps/favn_orchestrator/lib/favn_orchestrator/scheduler_entry.ex`
  - `apps/favn_orchestrator/lib/favn_orchestrator/transition_writer.ex`
- Phase 8 boundary-correction backend slices now also include orchestrator HTTP/API + auth foundations:
  - `apps/favn_orchestrator/lib/favn_orchestrator/api/router.ex`
  - `apps/favn_orchestrator/lib/favn_orchestrator/api/config.ex`
  - `apps/favn_orchestrator/lib/favn_orchestrator/auth.ex`
  - `apps/favn_orchestrator/lib/favn_orchestrator/auth/store.ex`
  - `apps/favn_orchestrator/priv/http_contract/v1/*.schema.json`
- Issue 168 local operator tooling added `apps/favn/lib/mix/tasks/favn.backfill.ex` and `apps/favn_local/lib/favn/dev/backfill.ex` for public local backfill submit, window inspection, coverage-baseline reads, asset/window state reads, and failed-window reruns through the private local orchestrator HTTP API.
- Issue 168 private HTTP contract coverage added backfill-oriented schemas under `apps/favn_orchestrator/priv/http_contract/v1/` for backfill windows, coverage baselines, and asset/window state.
- Initial separate web workspace boundary slice now includes:
  - `web/favn_web/src/lib/components/favn/` for Favn-specific presentational Svelte components used by the prototype dashboard and login screen
  - `web/favn_web/src/lib/components/favn/*Run*`, `*Asset*`, `*Output*`, `*Manifest*`, and `ErrorPanel.svelte` for componentized run-inspector and asset-catalog UI surfaces with colocated Storybook coverage
  - `web/favn_web/src/lib/components/ui/` for local shadcn-svelte-style UI primitives
  - `web/favn_web/src/lib/run_view_types.ts` for normalized run inspector view types
  - `web/favn_web/src/lib/asset_catalog_types.ts` for normalized asset catalog/detail view types
  - `web/favn_web/src/lib/server/run_views.ts` for BFF normalization from orchestrator run payloads into UI-oriented run views
  - `web/favn_web/src/lib/server/asset_catalog_views.ts` for BFF normalization from active manifest targets and run history into UI-oriented asset catalog/detail views
  - `web/favn_web/src/lib/utils.ts` for shared frontend utility helpers
  - `web/favn_web/src/hooks.server.ts`
  - `web/favn_web/src/lib/server/orchestrator.ts`
  - `web/favn_web/src/lib/server/session_guard.ts`
  - `web/favn_web/src/lib/server/web_api.ts`
  - `web/favn_web/src/lib/server/session.ts`
  - `web/favn_web/src/routes/login/+page.server.ts`
  - `web/favn_web/src/routes/login/+page.svelte`
  - `web/favn_web/src/routes/+page.server.ts`
  - `web/favn_web/src/routes/runs/+page.server.ts`
  - `web/favn_web/src/routes/runs/+page.svelte`
  - `web/favn_web/src/routes/runs/[run_id]/+page.server.ts`
  - `web/favn_web/src/routes/runs/[run_id]/+page.svelte`
  - `web/favn_web/src/routes/assets/+page.server.ts`
  - `web/favn_web/src/routes/assets/+page.svelte`
  - `web/favn_web/src/routes/assets/[asset_ref]/+page.server.ts`
  - `web/favn_web/src/routes/assets/[asset_ref]/+page.svelte`
  - `web/favn_web/src/routes/api/web/v1/streams/runs/+server.ts`
  - `web/favn_web/src/routes/api/web/v1/streams/runs/[run_id]/+server.ts`
  - `web/favn_web/src/routes/api/web/v1/runs/+server.ts`
  - `web/favn_web/src/routes/api/web/v1/runs/[run_id]/+server.ts`
  - `web/favn_web/src/routes/api/web/v1/runs/[run_id]/cancel/+server.ts`
  - `web/favn_web/src/routes/api/web/v1/runs/[run_id]/rerun/+server.ts`
  - `web/favn_web/src/routes/api/web/v1/manifests/+server.ts`
  - `web/favn_web/src/routes/api/web/v1/manifests/active/+server.ts`
  - `web/favn_web/src/routes/api/web/v1/manifests/[manifest_version_id]/activate/+server.ts`
  - `web/favn_web/src/routes/api/web/v1/schedules/+server.ts`
  - `web/favn_web/src/routes/api/web/v1/schedules/[schedule_id]/+server.ts`
- Legacy-owned runtime and UI slices are now deleted; new ownership should land directly in the current owner apps.
