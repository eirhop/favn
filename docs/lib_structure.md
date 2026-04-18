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
- Intended steady-state ownership is now: `favn` public surface, `favn_core` internal compiler/manifest/planning/contracts, `favn_runner` execution, `favn_orchestrator` control plane plus auth/authz/audit, and a separate `favn_web` tier outside the umbrella talking to orchestrator over a remote boundary. `favn_view` remains transitional only.
- Phase 3 populated `apps/favn_core/lib/favn/` with canonical manifest schema/versioning, serializer/hash/compatibility logic, graph/planning helpers, SQL helper internals, and shared runner/orchestrator contract structs.
- Phase 4 implementation grew `apps/favn_runner/lib/favn_runner/` around a small execution-owned set of modules such as a runner server, manifest store/resolver, worker supervision, and context builder.
- Phase 4 implementation moved `Favn.Run.Context` and `Favn.Run.AssetResult` out of `favn_legacy` into `apps/favn_core/lib/favn/run/`, while runner-owned connection and SQL runtime modules moved into `apps/favn_runner/lib/favn/` under preserved `Favn.*` names.
- Initial Phase 5 implementation added manifest-native orchestrator planning helpers and shared runner-client behaviour in `apps/favn_core/lib/favn/`:
  - `contracts/runner_client.ex`
  - `manifest/index.ex`
  - `manifest/pipeline_resolver.ex`
- Initial Phase 5 orchestrator runtime slice added control-plane modules in `apps/favn_orchestrator/lib/favn_orchestrator/`:
  - `storage.ex`
  - `storage/manifest_codec.ex`
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
  - `favn_storage_sqlite/adapter.ex`
  - `favn_storage_sqlite/repo.ex`
  - `favn_storage_sqlite/supervisor.ex`
  - `favn_storage_sqlite/migrations.ex`
  - `favn_storage_sqlite/migrations/create_foundation.ex`
- `apps/favn_storage_postgres/lib/` now includes the initial Phase 6 adapter foundation:
  - `favn_storage_postgres.ex`
  - `favn_storage_postgres/adapter.ex`
  - `favn_storage_postgres/repo.ex`
  - `favn_storage_postgres/supervisor.ex`
  - `favn_storage_postgres/migrations.ex`
  - `favn_storage_postgres/migrations/create_foundation.ex`
- Initial Phase 4 implementation now includes:
  - `apps/favn_core/lib/favn/run/context.ex`
  - `apps/favn_core/lib/favn/run/asset_result.ex`
  - `apps/favn_runner/lib/favn_runner/server.ex`
  - `apps/favn_runner/lib/favn_runner/manifest_store.ex`
  - `apps/favn_runner/lib/favn_runner/manifest_resolver.ex`
  - `apps/favn_runner/lib/favn_runner/context_builder.ex`
  - `apps/favn_runner/lib/favn_runner/worker.ex`
  - `apps/favn_runner/lib/favn_runner/event_sink.ex`
- Connection runtime ownership moved to runner-owned files under `apps/favn_runner/lib/favn/connection/`.
- SQL runtime execution ownership moved to runner-owned files under `apps/favn_runner/lib/favn/sql/` and `apps/favn_runner/lib/favn/sql_asset/` including `Favn.SQL.RuntimeBridge`.
- Phase 7 implementation now moves DuckDB ownership into `apps/favn_duckdb/lib/` including:
  - `apps/favn_duckdb/lib/favn/sql/adapter/duckdb.ex`
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
- The existing `favn_view` runtime slice remains in-repo only as a transitional same-BEAM prototype and currently includes:
  - `apps/favn_view/lib/favn_view/endpoint.ex`
  - `apps/favn_view/lib/favn_view/runs.ex`
  - `apps/favn_view/lib/favn_view/manifests.ex`
  - `apps/favn_view/lib/favn_view/scheduler.ex`
  - `apps/favn_view/lib/favn_view/presenters/run_presenter.ex`
  - `apps/favn_view/lib/favn_view/presenters/manifest_presenter.ex`
  - `apps/favn_view/lib/favn_view/presenters/scheduler_presenter.ex`
  - `apps/favn_view/lib/favn_view_web.ex`
  - `apps/favn_view/lib/favn_view_web/router.ex`
  - `apps/favn_view/lib/favn_view_web/layouts.ex`
  - `apps/favn_view/lib/favn_view_web/core_components.ex`
  - `apps/favn_view/lib/favn_view_web/telemetry.ex`
  - `apps/favn_view/lib/favn_view_web/error_html.ex`
  - `apps/favn_view/lib/favn_view_web/error_json.ex`
  - `apps/favn_view/lib/favn_view_web/live/dashboard_live.ex`
  - `apps/favn_view/lib/favn_view_web/live/manifests/index_live.ex`
  - `apps/favn_view/lib/favn_view_web/live/manifests/show_live.ex`
  - `apps/favn_view/lib/favn_view_web/live/runs/index_live.ex`
  - `apps/favn_view/lib/favn_view_web/live/runs/show_live.ex`
  - `apps/favn_view/lib/favn_view_web/live/scheduler/index_live.ex`
- Phase 8 boundary-correction backend slices now also include orchestrator HTTP/API + auth foundations:
  - `apps/favn_orchestrator/lib/favn_orchestrator/api/router.ex`
  - `apps/favn_orchestrator/lib/favn_orchestrator/api/config.ex`
  - `apps/favn_orchestrator/lib/favn_orchestrator/api/idempotency_store.ex`
  - `apps/favn_orchestrator/lib/favn_orchestrator/auth.ex`
  - `apps/favn_orchestrator/lib/favn_orchestrator/auth/store.ex`
  - `apps/favn_orchestrator/priv/http_contract/v1/*.schema.json`
- Initial separate web workspace boundary slice now includes:
  - `web/favn_web/src/hooks.server.ts`
  - `web/favn_web/src/lib/server/orchestrator.ts`
  - `web/favn_web/src/lib/server/web_api.ts`
  - `web/favn_web/src/lib/server/session.ts`
  - `web/favn_web/src/routes/login/+page.server.ts`
  - `web/favn_web/src/routes/login/+page.svelte`
  - `web/favn_web/src/routes/+page.server.ts`
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
- New runtime/DSL ownership should continue moving from `favn_legacy` to owner apps by bounded slice in later phases.
