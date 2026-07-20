# favn_view

Purpose: thin Phoenix/LiveView UI boundary app for Favn. It owns browser-facing
routes, LiveViews, controllers, components, endpoint configuration, and Phoenix
assets.

Ownership rules:

- `apps/favn_view/lib/favn_view/` contains the endpoint, router, LiveViews,
  controllers, components, and web helpers.
- `apps/favn_view/lib/favn_view/readiness.ex` and
  `apps/favn_view/lib/favn_view/production_runtime_config.ex` own web health,
  readiness, and web-owned production config validation.
- `apps/favn_view/lib/favn_view/auth.ex` and
  `apps/favn_view/lib/favn_view/auth/*.ex` own browser auth glue, Phoenix
  session handling, the volatile server-side browser-session token mapping,
  LiveView `on_mount` auth, and sanitized view-local scope assignment. Durable
  auth state remains in `favn_orchestrator`.
- `apps/favn_view/assets/` contains the standard Phoenix-generated asset setup.
- `apps/favn_view/storybook/` contains PhoenixStorybook stories for reusable
  UI component states.
- `apps/favn_view/test/` contains endpoint, controller, and LiveView tests.
- Run overview and run detail LiveViews must render orchestrator-owned run read
  models. They may prepare view-models and UI query state, but must not derive
  backfill hierarchy, asset attempt aggregation, window membership, or log
  lookup rules from ids, names, or formatted strings.
- Run detail Storybook coverage is split by view so heavy Timeline variations do
  not slow down the overview story.
- `favn_view` must call backend behavior only through the public orchestrator
  facade. It must not depend on storage, scheduler, runner, persistence, repo,
  manifest compiler, plugin, adapter, or low-level orchestrator internals.
- Production web readiness calls the public `FavnOrchestrator` facade in the same
  BEAM. It must not use orchestrator HTTP, service-token shortcuts, storage
  access, scheduler runtime access, or runner internals.
- Browser auth calls only public `FavnOrchestrator` facade functions. It must not
  call `FavnOrchestrator.Auth`, auth storage, password hashing internals, audit
  storage, service-token internals, storage adapters, scheduler internals, or
  runner internals directly.
- Operator mutation forms for pipeline runs, pipeline backfills, asset/window
  runs, and asset range backfills must pass dependency and refresh intent through
  the public orchestrator facade rather than reconstructing freshness, range
  expansion, or backfill state in the UI. The asset form may reject incompatible
  choices immediately, but the orchestrator remains authoritative for forged
  events and non-browser callers.
- Asset detail renders orchestrator-owned run-anchor, data-coverage, and calendar
  freshness timelines as distinct views. Calendar freshness periods are
  read-only; the view must never translate one into pipeline anchor or exact
  data-window submission intent. When the orchestrator reports multiple pipeline
  run contexts, the view keeps the selected stable context id in the asset route
  and includes it in run requests; run actions stay disabled until one is selected.
- Operator run cancellation controls must call the public `FavnOrchestrator`
  facade only. UI state may disable buttons, show confirmations, and map stable
  error atoms to labels, but cancellation lifecycle, audit, idempotency, runner
  dispatch, and terminal status semantics remain orchestrator-owned.
- The schedules list LiveView renders orchestrator-owned schedule list read models
  through the public facade. It may manage filters and visual formatting, but
  activation state, runtime state, effective scheduling, fingerprint review, and
  next-due semantics remain in `favn_orchestrator`.
- The schedule detail LiveView decodes the route-safe schedule id and calls
  `FavnOrchestrator.get_schedule_entry/1`. The Occurrences tab renders
  `FavnOrchestrator.preview_schedule_occurrences/2`; the view must not derive
  cron, missed, overlap, window, next-due, or failure semantics locally. Detail
  tabs beyond Overview and Occurrences must stay disabled or backed by new
  orchestrator read models.
- Tidewave is plugged only in dev. PhoenixStorybook is mounted under
  `/storybook` when dev routes are enabled.
- SQL contracts, expected-versus-observed candidate schema, flattened fragment
  origins, ordered row-count bounds/parameters and policies, lineage, Contract
  and Custom checks,
  quality warnings, no-op writes, and rolled-back diagnostics render from
  orchestrator-owned asset-detail assurance and attempt
  `output_metadata`. `FavnView.Components.OutputMetadata` owns the shared run
  result presentation. The view does not query manifests, runner state, or SQL
  adapter state to reconstruct assurance.
