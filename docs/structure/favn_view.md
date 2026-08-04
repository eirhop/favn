# favn_view

Purpose: thin Phoenix/LiveView UI boundary app for Favn. It owns browser-facing
routes, LiveViews, controllers, components, endpoint configuration, and Phoenix
assets.

Ownership rules:

- `apps/favn_view/lib/favn_view/` contains the endpoint, router, LiveViews,
  controllers, components, and web helpers.
- `apps/favn_view/lib/favn_view/ui/` owns the design-system element library
  (`FavnView.UI`): tones, type scale, surfaces, buttons, badges, states, data
  presentation, and form/filter controls. Sections and pages compose these
  elements and must not re-implement their styling with utility classes. The
  visual contract is [`../design/style-guide.md`](../design/style-guide.md) and
  the composition contract is
  [`../design/component-patterns.md`](../design/component-patterns.md).
- `FavnView.Components.Navigation` is the single source of truth for primary
  navigation. Every entry must resolve to a live route in `FavnView.Router`.
- `FavnView.ApplicationConfig` owns the baseline endpoint and browser-session
  configuration used both inside the umbrella and when Favn starts from a
  consumer project. Boot owners may add listener and runtime overrides without
  duplicating those View internals.
- `apps/favn_view/lib/favn_view/readiness.ex` and
  `apps/favn_view/lib/favn_view/production_runtime_config.ex` own web health,
  readiness, and web-owned production config validation.
- `apps/favn_view/lib/favn_view/auth.ex` and
  `apps/favn_view/lib/favn_view/auth/*.ex` own browser auth glue, Phoenix
  session handling, the volatile server-side browser-session token mapping,
  LiveView `on_mount` auth, and sanitized view-local scope assignment. Durable
  auth state remains in `favn_orchestrator`.
- `apps/favn_view/assets/` contains the standard Phoenix-generated asset setup.
- `apps/favn_view/dev/` contains the design-system browser and the view-model
  fixtures it shares with tests. It is compiled in `:dev` and `:test` only.
- `apps/favn_view/test/` contains endpoint, controller, and LiveView tests.
- Run overview and run detail LiveViews must render orchestrator-owned run read
  models. They may prepare view-models and UI query state, but must not derive
  backfill hierarchy, asset attempt aggregation, window membership, or log
  lookup rules from ids, names, or formatted strings.
- Run detail presents requested anchors, permitted expansion, and effective
  asset windows as separate concepts. It asks for compact view-specific data and
  requests the bounded event detail only when the Events tab is active.
- Run detail design-system examples name the mode they render, so a heavy
  Timeline example can be requested without rendering the overview.
- `favn_view` must call backend behavior only through the public orchestrator
  facade. It must not depend on storage, scheduler, runner, persistence, repo,
  manifest compiler, plugin, adapter, or low-level orchestrator internals.
- Production web readiness calls the public `FavnOrchestrator` facade in the same
  BEAM. It must not use orchestrator HTTP, service-token shortcuts, storage
  access, scheduler runtime access, or runner internals.
- Production View configuration is applied by the unified control-plane loader.
  View has no storage-adapter dependency. It trusts forwarded scheme, host, port,
  and client IP only from the configured private proxy CIDRs, uses the public
  origin as URL authority, and applies boot-frozen listener and body-read limits.
  `docs/production/network_and_proxy.md` is the operator-facing exposure and
  reverse-proxy contract; the private Orchestrator listener is never a browser
  route.
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
- Asset detail is one LiveView with `live_action` sub-pages, so moving between
  Overview, Runs, Coverage, Documentation, and Diagnostics patches rather than
  remounts. Each sub-page loads only what it renders; coverage and documentation are
  read when their own page opens.
- Asset detail submits a run through one dialog, prefilled from the orchestrator's
  reported due period. The view does not derive that period, and offers period fields
  only for an asset the orchestrator reports as windowed. When the orchestrator
  reports multiple pipeline run contexts, the view keeps the selected stable context
  id in the asset route and includes it in run requests; run actions stay disabled
  until one is selected.
- Asset coverage renders the orchestrator's expected windows as a calendar whose grain
  and navigation bounds come from `FavnView.CoverageCalendar`. The view owns which
  unit fills one screen per grain; it does not compute which windows exist, how many a
  period holds across a clock change, or whether one is covered.
- Asset catalogue and detail render the orchestrator-owned target compatibility
  status independently from health, freshness, and coverage. Diagnostics states the
  verdict and the fix in operator words and keeps the bounded reason, structured diff,
  generation, and fingerprints reachable behind disclosures. It must disable ordinary
  run/backfill actions when `blocks_writes?` is true, and does not reclassify
  compatibility from manifest or physical target metadata.
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
- Tidewave is plugged only in dev, and the design-system browser is routed under
  `/design-system` only when dev routes are enabled. Both therefore exist on the
  umbrella development server and nowhere else; see
  [`../contributing/dev-server.md`](../contributing/dev-server.md).
- SQL contracts, expected-versus-observed candidate schema, flattened fragment
  origins, ordered row-count bounds/parameters and policies, lineage, Contract
  and Custom checks,
  quality warnings, no-op writes, and rolled-back diagnostics render from
  orchestrator-owned asset-detail assurance and attempt
  `output_metadata`. `FavnView.Components.OutputMetadata` owns the shared run
  result presentation. The view does not query manifests, runner state, or SQL
  adapter state to reconstruct assurance.
