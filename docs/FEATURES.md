# Favn Features

This file describes the feature set that is implemented in the repository today.

- Only shipped behavior belongs here.
- Future work belongs in `docs/ROADMAP.md`.
- Audit scope for this update: `apps/favn`, `apps/favn_authoring`, `apps/favn_core`, `apps/favn_runner`, `apps/favn_duckdb`, `apps/favn_orchestrator`, `apps/favn_storage_sqlite`, `apps/favn_storage_postgres`, `apps/favn_local`, and `web/favn_web`.

State labels used below:

- `solid but still private-dev`: implemented and well-covered, but not presented in the repo as release-ready.
- `prototype`: implemented and usable for development, but still clearly early or intentionally thin.
- `compatibility-only`: supported mainly to preserve an older/current authoring path, not because it is the preferred future shape.
- `needs hardening`: implemented, but the code/docs/tests still point to important production-readiness gaps.

## Architecture Status

Favn `v0.5.0` established the intended manifest-first product architecture and the runtime boundaries between:

- `favn` as the public authoring surface and public `mix favn.*` entrypoint owner
- `favn_authoring` as the internal authoring and manifest implementation owner
- `favn_local` as the internal local lifecycle, tooling, and packaging implementation owner
- `favn_core` as the shared compiler, manifest, planning, and contracts layer
- `favn_orchestrator` as the control plane and system of record
- `favn_runner` as the execution runtime
- `favn_web` as the separate public web edge

The refactor closeout is complete enough that the main remaining work is product hardening and support-boundary clarity, not more architecture migration.

Implemented closeout outcomes:

- The refactor phases are complete, including the local lifecycle and packaging command surface: `mix favn.install`, `mix favn.dev`, `mix favn.stop`, `mix favn.reload`, `mix favn.status`, `mix favn.logs`, `mix favn.reset`, `mix favn.build.web`, `mix favn.build.orchestrator`, `mix favn.build.runner`, `mix favn.build.single`, and `mix favn.read_doc`.
- Legacy same-BEAM app paths are removed from supported runtime flows: `apps/favn_legacy` and `apps/favn_view` are gone from the umbrella.
- Stable storage adapter entrypoints are restored as `Favn.Storage.Adapter.SQLite` and `Favn.Storage.Adapter.Postgres`.
- SQL adapters now persist canonical inspectable `json-v1` payloads for run snapshots, run events, and scheduler state instead of BEAM term blobs.
- Scheduler state writes now use explicit optimistic versions.
- External Postgres repo mode now caches successful schema-readiness validation instead of repeating readiness checks on every adapter call.
- Pre-closeout SQL rows persisted as BEAM term blobs are intentionally unsupported. Existing persisted runtime state must be reset or recreated when upgrading to the closeout adapters.

## Implemented Today

### Define assets and data relations

- Single Elixir assets are supported, including docs, metadata, dependencies, windows, and owned relations. State: `solid but still private-dev`. Refs: `Favn.Asset`, `apps/favn_authoring/lib/favn/asset.ex`.
- Older multi-function asset modules are still supported through `@asset`, although new code is clearly steered toward `Favn.Asset` or `Favn.MultiAsset`. State: `compatibility-only`. Refs: `Favn.Assets`, `apps/favn_authoring/lib/favn/assets.ex`.
- Generated multi-assets are supported when many assets share one runtime implementation but differ by config, metadata, dependencies, or relation ownership. State: `solid but still private-dev`. Refs: `Favn.MultiAsset`, `apps/favn_authoring/lib/favn/multi_asset.ex`.
- SQL assets are supported with inline SQL or file-backed SQL, compile-time SQL analysis, tracked relation usage, relation-style SQL references, inferred dependencies from owned relation references, and table, view, and limited incremental materializations. State: `solid but still private-dev`. Refs: `Favn.SQLAsset`, `Favn.SQL`, `apps/favn_authoring/lib/favn/sql_asset.ex`, `apps/favn_authoring/lib/favn/sql.ex`.
- External source relations can be declared in the catalog without becoming runnable assets. State: `solid but still private-dev`. Refs: `Favn.Source`, `apps/favn_authoring/lib/favn/source.ex`.
- Shared relation defaults can be inherited from module structure for connection, catalog, and schema settings. State: `solid but still private-dev`. Refs: `Favn.Namespace`, `apps/favn_authoring/lib/favn/namespace.ex`.
- Named connection definitions are supported as part of the public authoring contract. State: `solid but still private-dev`. Refs: `Favn.Connection`, `apps/favn_authoring/lib/favn/connection.ex`.
- Window helpers are implemented for hourly, daily, and monthly asset windows plus run-level anchor and runtime windows. State: `solid but still private-dev`. Refs: `Favn.Window`, `apps/favn_authoring/lib/favn/window.ex`.

### Select work and compile manifests

- Pipelines are supported with direct asset targets, multi-target shorthands, additive selectors by asset, module, tag, and category, plus config, metadata, schedules, named window/source, and outputs. State: `solid but still private-dev`. Refs: `Favn.Pipeline`, `apps/favn_authoring/lib/favn/pipeline.ex`, `apps/favn/test/public_pipeline_parity_test.exs`.
- Reusable named schedules are supported and can be referenced from pipelines. State: `solid but still private-dev`. Refs: `Favn.Triggers.Schedules`, `apps/favn_authoring/lib/favn/triggers/schedules.ex`.
- Public facade APIs can list assets, fetch a compiled asset or pipeline, resolve a pipeline, generate a manifest, build a manifest artifact, serialize it, hash it, validate compatibility, and pin a manifest version. State: `solid but still private-dev`. Refs: `Favn`, `FavnAuthoring`, `apps/favn/lib/favn.ex`, `apps/favn_authoring/lib/favn.ex`.
- Canonical manifest generation is implemented from explicit module lists or app config, including assets, pipelines, schedules, and dependency graph data. State: `solid but still private-dev`. Refs: `Favn.Manifest.Generator`, `apps/favn_core/lib/favn/manifest/generator.ex`.
- Configured authoring modules are resolved reliably in standalone consumer projects even when modules are not preloaded in the VM before authoring/runtime checks run. State: `solid but still private-dev`. Refs: `Favn.Assets.Compiler`, `Favn.Manifest.Generator`, `Favn.Connection.Loader`, `apps/favn_core/lib/favn/assets/compiler.ex`, `apps/favn_core/lib/favn/manifest/generator.ex`, `apps/favn_runner/lib/favn/connection/loader.ex`.
- Stable manifest serialization, hashing, compatibility validation, and immutable manifest-version envelopes are implemented. State: `solid but still private-dev`. Refs: `Favn.Manifest.Serializer`, `Favn.Manifest.Identity`, `Favn.Manifest.Compatibility`, `Favn.Manifest.Version`, `apps/favn_core/test/manifest/version_test.exs`.
- Deterministic graph indexing and planning are implemented, including dependency inclusion modes, topological stages, and expansion of backfill ranges across windowed assets. State: `solid but still private-dev`. Refs: `Favn.Assets.GraphIndex`, `Favn.Assets.Planner`, `apps/favn_core/lib/favn/assets/planner.ex`, `apps/favn_core/test/assets/graph_planner_parity_test.exs`.

### Run pinned work

- A separate runner boundary is implemented. The runner can register pinned manifest versions, accept asynchronous work, wait for results, cancel work, and run synchronously through the same boundary. State: `solid but still private-dev`. Refs: `FavnRunner`, `FavnRunner.Server`, `apps/favn_runner/lib/favn_runner.ex`, `apps/favn_runner/lib/favn_runner/server.ex`.
- Elixir assets execute through the runner using manifest-pinned runtime context instead of ad hoc orchestrator-side module discovery. State: `solid but still private-dev`. Refs: `apps/favn_runner/test/favn_runner_test.exs`, `apps/favn_orchestrator/test/orchestrator_runner_integration_test.exs`.
- Source assets are treated as observe/no-op nodes in execution. State: `solid but still private-dev`. Refs: `apps/favn_runner/test/favn_runner_test.exs`, `Favn.Assets.Planner`.
- SQL assets execute from manifest-carried SQL payloads, and the runner does not fall back to compiled modules when manifest data is missing. State: `needs hardening`. Refs: `apps/favn_runner/test/execution/sql_asset_test.exs`, `apps/favn_core/lib/favn/manifest/sql_execution.ex`.
- Runner-side cancellation, timeout handling, and crash reporting are implemented. State: `solid but still private-dev`. Refs: `apps/favn_runner/test/server_test.exs`, `apps/favn_runner/lib/favn_runner/server.ex`.
- DuckDB is implemented as a runner plugin with both in-process and separate-process execution modes. State: `prototype`. Refs: `FavnDuckdb`, `FavnDuckdb.Runtime`, `apps/favn_duckdb/lib/favn_duckdb.ex`, `apps/favn_duckdb/test/favn_duckdb_test.exs`.

### Orchestrate and operate runs

- Manifest storage is implemented in the orchestrator, including registration, listing, activation, active-manifest lookup, and manifest-scoped run targets. State: `solid but still private-dev`. Refs: `FavnOrchestrator`, `apps/favn_orchestrator/lib/favn_orchestrator.ex`.
- Asset runs and pipeline runs are submitted as manifest-pinned runs, and reruns stay pinned to the source run's manifest version. State: `solid but still private-dev`. Refs: `FavnOrchestrator`, `apps/favn_orchestrator/test/orchestrator_runner_integration_test.exs`.
- Run state views, run history, persisted run events, cancellation, and rerun flows are implemented. State: `solid but still private-dev`. Refs: `FavnOrchestrator`, `FavnOrchestrator.Projector`, `apps/favn_orchestrator/lib/favn_orchestrator.ex`.
- Schedule inspection is implemented from the active manifest, with runtime reload and tick hooks when the scheduler process is running. State: `solid but still private-dev`. Refs: `FavnOrchestrator.reload_scheduler/0`, `FavnOrchestrator.tick_scheduler/0`, `FavnOrchestrator.list_schedule_entries/0`, `apps/favn_orchestrator/lib/favn_orchestrator.ex`.
- Live run updates are implemented with global and run-scoped SSE endpoints, including replay-after-cursor for run streams. State: `prototype`. Refs: `FavnOrchestrator.API.Router`, `FavnOrchestrator.list_run_stream_events/2`, `apps/favn_orchestrator/test/api/router_test.exs`.
- A private orchestrator HTTP API is implemented for manifests, runs, schedules, auth, actor management, audit reads, and SSE streams. State: `solid but still private-dev`. Refs: `apps/favn_orchestrator/lib/favn_orchestrator/api/router.ex`, `apps/favn_orchestrator/priv/http_contract/v1/*.schema.json`.
- Local username/password auth, actor roles (`viewer`, `operator`, `admin`), session introspection and revocation, and audit logging are implemented in the orchestrator. State: `needs hardening`. Refs: `FavnOrchestrator.Auth`, `apps/favn_orchestrator/lib/favn_orchestrator/api/router.ex`, `apps/favn_orchestrator/test/api/router_test.exs`.

### Store runtime state

- In-memory orchestrator storage is implemented and is the default local mode. State: `solid but still private-dev`. Refs: `FavnOrchestrator.Storage.Adapter.Memory`, `apps/favn_orchestrator/lib/favn_orchestrator.ex`.
- SQLite control-plane persistence is implemented for runs, events, scheduler state, write ordering, optimistic scheduler versions, and canonical `json-v1` payload storage. State: `solid but still private-dev`. Refs: `Favn.Storage.Adapter.SQLite`, `apps/favn_storage_sqlite/test/sqlite_storage_test.exs`.
- Postgres control-plane persistence is implemented with managed-repo and external-repo modes, cached external readiness validation, and canonical `json-v1` payload storage. State: `solid but still private-dev`. Refs: `Favn.Storage.Adapter.Postgres`, `apps/favn_storage_postgres/test/adapter_test.exs`.

### Develop locally

- Public Mix tasks exist for `install`, `dev`, `status`, `logs`, `reload`, `stop`, `reset`, `build.runner`, `build.web`, `build.orchestrator`, and `build.single`. State: `solid but still private-dev`. Refs: `apps/favn/lib/mix/tasks/*.ex`, `apps/favn/test/mix_tasks/public_tasks_test.exs`.
- Local compiled-document lookup is implemented through `mix favn.read_doc` for modules and public functions. State: `solid but still private-dev`. Refs: `apps/favn/lib/mix/tasks/favn.read_doc.ex`, `apps/favn/test/mix_tasks/read_doc_task_test.exs`.
- `mix favn.install` fingerprints toolchain and runtime source inputs, materializes an install-owned runtime workspace under `.favn/install/runtime_root`, records runtime metadata under `.favn/install/runtime.json`, and installs runtime/web dependencies for local tooling. State: `solid but still private-dev`. Refs: `Favn.Dev.Install`, `apps/favn_local/lib/favn/dev/install.ex`, `apps/favn_local/test/dev_install_test.exs`.
- `mix favn.dev` starts runner, orchestrator, and web as separate local processes, stores runtime state under `.favn/runtime.json`, checks readiness, and bootstraps a manifest into runner and orchestrator. State: `solid but still private-dev`. Refs: `Favn.Dev.Stack`, `apps/favn_local/lib/favn/dev/stack.ex`.
- Local storage selection is implemented for memory, SQLite, and Postgres. State: `solid but still private-dev`. Refs: `apps/favn_local/README.md`, `apps/favn_local/test/integration/dev_storage_verification_test.exs`.
- `mix favn.status`, `mix favn.logs`, `mix favn.stop`, and `mix favn.reset` are implemented for stack inspection, log reading, shutdown, and local state cleanup. State: `solid but still private-dev`. Refs: `apps/favn_local/test/dev_logs_test.exs`, `apps/favn_local/test/dev_reset_test.exs`, `apps/favn/test/mix_tasks/public_tasks_test.exs`.
- Local build flows are implemented for runner, web, orchestrator, and single-node assembly outputs. State: `prototype` overall. `build.runner` is the most complete target, while `build.web`, `build.orchestrator`, and `build.single` are intentionally metadata-oriented or assembly-only outputs. Refs: `apps/favn_local/test/dev_build_runner_test.exs`, `apps/favn_local/test/dev_build_web_test.exs`, `apps/favn_local/test/dev_build_orchestrator_test.exs`, `apps/favn_local/test/dev_build_single_test.exs`.

### Use the web prototype

- A separate SvelteKit web/BFF workspace is implemented under `web/favn_web`. It handles signed cookie sessions, login/logout, protected pages, and server-side relays to the private orchestrator API. State: `needs hardening`. Refs: `web/favn_web/src/hooks.server.ts`, `web/favn_web/src/lib/server/session.ts`, `web/favn_web/src/lib/server/orchestrator.ts`.
- Web BFF endpoints exist for runs, manifests, schedules, and run-stream relays. State: `prototype`. Refs: `web/favn_web/src/routes/api/web/v1/**`, `web/favn_web/tests/e2e/auth-session-runs.e2e.ts`.
- End-to-end browser tests cover login, logout, unauthorized redirects, and basic runs, manifests, schedules, and run-stream relay behavior. State: `solid but still private-dev` as test coverage, but it covers a thin prototype rather than a mature product UI. Refs: `web/favn_web/tests/e2e/auth-session-runs.e2e.ts`.

## Current Caveats And Partial Areas

- No major area in this repository is currently documented as fully release-ready. The strongest areas today are authoring, manifest compilation, planning, runner and orchestrator basics, and the local storage loops, but the project still presents itself as private development.
- The web surface is a working prototype, not a polished product UI. The clearest evidence is the thin BFF route set and the current E2E coverage, not the default scaffold `web/favn_web/README.md`, which is still outdated.
- The orchestrator HTTP API is private service-to-service infrastructure, not a documented public external API contract.
- The public `Favn` module intentionally spans both stable authoring helpers and thin runtime delegation helpers. Runtime-facing calls such as `run_pipeline`, `get_run`, and scheduler helpers still depend on the orchestrator and runtime apps being available and return `:runtime_not_available` when they are not. Refs: `apps/favn/lib/favn.ex`, `apps/favn/test/runtime_facade_test.exs`.
- Auth, session, and audit foundations exist now, but the current implementation is still prototype-grade: the orchestrator auth store is in-memory, browser-edge abuse controls are not present, and service credentials are still a simple static-token model. Refs: `apps/favn_orchestrator/lib/favn_orchestrator/auth/store.ex`, `apps/favn_orchestrator/lib/favn_orchestrator/api/router.ex`, `docs/REFACTOR.md`.
- SSE support is uneven today. Run-scoped replay is the strongest implemented path, while the global runs stream is still much thinner and not yet a durable or scalable live-update contract. Refs: `apps/favn_orchestrator/lib/favn_orchestrator/api/router.ex`, `apps/favn_orchestrator/test/api/router_test.exs`, `docs/REFACTOR.md`.
- Packaging is uneven today. `build.runner` produces the strongest operational package contract, while `build.web`, `build.orchestrator`, and `build.single` currently produce metadata-oriented or assembly-only outputs rather than full deployable product artifacts. Refs: `apps/favn_local/test/dev_build_runner_test.exs`, `apps/favn_local/test/dev_build_web_test.exs`, `apps/favn_local/test/dev_build_orchestrator_test.exs`, `apps/favn_local/test/dev_build_single_test.exs`.
- Persisted runtime state from pre-closeout SQL adapters is not upgrade-compatible. Older SQLite or Postgres rows stored as BEAM term blobs must be reset or recreated once before using the closeout adapters. Refs: `README.md`, `docs/REFACTOR.md`.
- Postgres support is implemented at the adapter and configuration level, but broader live verification is still opt-in in tests. Refs: `apps/favn_storage_postgres/test/adapter_test.exs`, `apps/favn_storage_postgres/test/integration/adapter_live_test.exs`, `docs/REFACTOR.md`.

## Not In Scope For This File

- Planned next work and later ideas live in `docs/ROADMAP.md`.
