defmodule Favn.AI do
  @moduledoc """
  AI-oriented documentation entrypoint for learning and using Favn.

  Favn is an Elixir library for defining business-oriented data assets,
  describing how they depend on each other, compiling them into an explicit
  manifest, and then using that manifest for planning and runtime execution.

  Use this module to decide which docs to read next with `mix favn.read_doc`.

  ## Recommended Consumer Shape

  For project layout, namespace files, and relation hierarchy, read
  `Favn.Namespace` before authoring files. The short default is:

  - `connections/important_lakehouse.ex` owns server/session/auth connection config.
  - `lakehouse.ex` owns the root relation namespace with connection only.
  - `lakehouse/<phase>.ex` owns each catalog/database phase such as `raw` or `mart`.
  - `lakehouse/<phase>/<segment>.ex` owns each schema/domain such as `sales`.
  - `lakehouse/<phase>/<segment>/<asset>.ex` owns leaf asset modules.
  - integration clients, pipelines, triggers, and reusable SQL live outside the
    lakehouse asset tree.

  ## Consumer Dependency Shape

  A normal consumer project depends on `:favn` for the public DSL, helper
  functions, and `mix favn.*` tasks. Add `:favn_duckdb` only when the project
  executes DuckDB-backed SQL assets or uses DuckDB through `Favn.SQLClient`.

  Do not add internal runtime/control-plane apps such as `:favn_storage_sqlite`,
  `:favn_orchestrator`, `:favn_runner`, `:favn_local`, `:favn_core`, or
  `:favn_sql_runtime` as ordinary consumer dependencies. Local SQLite
  control-plane storage is selected through `config :favn, :local` or
  `mix favn.dev --sqlite`.

  ## What To Read

  - To author one Elixir asset, read `Favn.Asset`, then `Favn.Namespace` and
    `Favn.Window` if needed.
  - To declare required runtime configuration or secrets for assets, read
    `Favn.Asset` and `Favn.RuntimeConfig.Ref`.
  - To declare asset freshness or understand skip/force behavior, read
    `Favn.Freshness`, then `Favn.Freshness.Policy` for `@freshness` input values
    and `Favn.Freshness.Key` for stored freshness keys. Asset DSL docs for
    `Favn.Asset`, `Favn.SQLAsset`, `Favn.MultiAsset`, and `Favn.Assets` show
    where `@freshness` must be attached.
  - To author a source-system raw landing asset, read `Favn.Asset`, then
    `Favn.SQLClient`, `Favn.Namespace`, and the standalone tutorial at
    `examples/basic-workflow-tutorial`. The canonical pattern is: declare
    source IDs/tokens with `source_config/2`, read resolved values from
    `ctx.config`, call a source client outside the asset, write raw rows through
    `Favn.SQLClient`, and return structured metadata with row counts, relation,
    load mode, timestamp, and hashed source identity.
  - To author one SQL asset, read `Favn.SQLAsset`, then `Favn.SQL`,
    `Favn.Connection`, `Favn.Namespace`, and `Favn.Window` as needed.
  - To author many similar assets in one module, read `Favn.MultiAsset`.
  - To declare external source relations, read `Favn.Source`.
  - To share relation defaults, read `Favn.Namespace`.
  - To define a pipeline, read `Favn.Pipeline`, then
    `Favn.Triggers.Schedules` if schedules are involved. If the pipeline is
    windowed, also read `Favn.Window`, `Favn.Window.Policy`, and
    `Favn.Window.Request`. If the pipeline or its assets touch rate-limited
    source systems or shared infrastructure, read the `max_concurrency` and
    `execution_pool` sections in `Favn.Pipeline` and `Favn.Asset`.
  - To limit asset execution before asset code starts, use orchestrator-owned
    execution concurrency controls: pipeline `max_concurrency`, pipeline
    `execution_pool`, asset `@execution_pool`, and `config :favn,
    execution_pools: [...]`. Do not model artificial dependencies only to
    serialize independent assets, and do not confuse these controls with SQL
    `write_concurrency`, which protects writer/backend admission after asset
    execution has already started.
  - To debug duplicate materializations, reruns that should skip already-finished
    work, or crash recovery after a stopped orchestrator, read
    `FavnOrchestrator.MaterializationClaim`,
    `FavnOrchestrator.Freshness.StateWriter`,
    `FavnOrchestrator.Repair.RuntimeState`, and `Mix.Tasks.Favn.RepairRuntimeState`.
    The repair Mix task is orchestrator-internal; consumer projects using
    `mix favn.dev --sqlite` get automatic migration and startup recovery when the
    local dev stack starts.
  - To work with windows or one-off run input, read `Favn.Window`, then
    `Favn.Window.Policy` for pipeline/scheduler policy, `Favn.Window.Request`
    for CLI/API run input, and `Favn plan_asset_run` if you need planning
    details.
  - To work with operational backfill ranges, dry-run planning, compact
    `--window kind:FROM..TO` input, forced refresh repair, or successful-window
    backfill reruns, read `Favn.Backfill.RangeRequest`,
    `Favn.Backfill.RangeResolver`, and `FavnOrchestrator.RefreshPolicy`. If you
    are wiring orchestrator-side submission, planning, or projection, then read
    `FavnOrchestrator`, `FavnOrchestrator.BackfillManager`, and the internal
    modules under `FavnOrchestrator.Backfill.*`. If you are changing the
    private service HTTP surface for backfills, also read
    `FavnOrchestrator.API.Router`. For the local operator CLI, read
    `Favn.Dev.Backfill`, `Favn.Dev.Run`, `Mix.Tasks.Favn.Backfill`, and
    `Mix.Tasks.Favn.Run`.
  - To define connection contracts, read `Favn.Connection`; if connection
    modules should be discovered from an OTP app, read `Favn.ModuleDiscovery`.
    If connection values come from environment variables or secrets, also read
    `Favn.RuntimeConfig.Ref`.
  - To configure DuckDB/DuckLake connection bootstrap with `open: [...]` and
    `duckdb: [...]`, attached DuckDB/DuckLake catalogs, scoped
    `:required_catalogs` attach behavior, Azure credential-chain secrets,
    optional ADLS `SCOPE`, credential `CHAIN`, PostgreSQL metadata secrets,
    DuckLake `META_SECRET` attach, catalog-level bootstrap/write concurrency, or
    ADLS paths, read
    `Favn.Connection`, `Favn.RuntimeConfig.Ref`, `Favn.SQL.Adapter.DuckDB`, and
    `Favn.Azure.PostgresEntraToken`. If the deployment uses the ADBC DuckDB
    adapter, also read `Favn.SQL.Adapter.DuckDB.ADBC`. Azure PostgreSQL Entra
    auth can fetch managed identity or Azure CLI tokens during session bootstrap
    for DuckLake PostgreSQL metadata catalogs. The PostgreSQL `user` must be the
    role created for the Entra principal, and token expiry requires reconnect and
    rebootstrap. When sizing DuckLake PostgreSQL metadata capacity, remember that
    one concurrent DuckLake writer can use multiple PostgreSQL backend
    connections; observed deployments used about three backends per writer, so
    `write_concurrency` needs PostgreSQL headroom beyond the logical writer count.
  - To run SQL queries from plain Elixir code using named Favn connections, read
    `Favn.SQLClient`.
  - To compile a manifest, read `Favn generate_manifest`; if the project uses
    `config :favn, discovery: [apps: [...], assets: :all, pipelines: :all,
    schedules: :all]`, also read `Favn.ModuleDiscovery`. Read
    `Favn.Manifest.Generator` if you need internal compilation details.
  - To resolve pipeline targets, read `Favn resolve_pipeline`, then
    `Favn.Pipeline.Resolver` if needed.
  - To plan execution order, read `Favn plan_asset_run`, then
    `Favn.Assets.Planner` if needed.
  - To inspect local runs, run events, relation metadata, relation partitions, or
    ad hoc read-only SQL, read `Favn.Dev.Runs`, `Favn.Dev.DataInspection`,
    `Mix.Tasks.Favn.Runs`, `Mix.Tasks.Favn.Logs`, `Mix.Tasks.Favn.Inspect`, and
    `Mix.Tasks.Favn.Query`. `mix favn.inspect ...` and `mix favn.query
    "select ..."` are direct local operator entrypoints: their Mix tasks start
    the app, and `Favn.Dev.DataInspection` starts `:favn_sql_runtime` before
    connecting, so users do not need `mix do app.start + ...`.
  - To run local tooling, read `Favn.Dev`, then `apps/favn_local/README.md`.
    The public local command surface is `mix favn.install`, `mix favn.init`,
    `mix favn.doctor`, `mix favn.dev`, `mix favn.run`, `mix favn.backfill`,
    `mix favn.runs`, `mix favn.status`, `mix favn.logs`, `mix favn.inspect`,
    `mix favn.query`, `mix favn.diagnostics`, `mix favn.reload`,
    `mix favn.stop`, `mix favn.reset`, `mix favn.build.runner`,
    `mix favn.build.web`, `mix favn.build.orchestrator`,
    `mix favn.build.single`, `mix favn.bootstrap.single`, and
    `mix favn.read_doc`.
  - To inspect the public helper functions collected in one place, read `Favn`.

  ## About `Favn`

  - you need helper functions like `generate_manifest`, `resolve_pipeline`, or
    `plan_asset_run`
  - you need the thin public facade shape
  - you are debugging delegation to `FavnAuthoring` or runtime apps

  ## Read Internals Only When

  - `Favn.Manifest.Generator`: when you need the exact manifest compilation path
    from modules to `%Favn.Manifest{}`
  - `Favn.ModuleDiscovery`: when app-scoped `:all` discovery maps compiled OTP
    application modules to authored assets, pipelines, schedules, or connections
  - `Favn.Pipeline.Resolver`: when you need selector normalization, schedule
    resolution, execution-concurrency policy propagation, or the exact
    `%Favn.Pipeline.Resolution{}` shape
  - `Favn.Assets.Planner`: when you need topological stages, dependency
    expansion, anchor windows, effective per-node execution pools, or backfill
    planning
  - `FavnOrchestrator.ExecutionAdmission`: when working inside the orchestrator
    on persisted execution leases, run-level concurrency limits, global/pool
    admission, queue reasons, lease release, and recovery semantics
  - `FavnOrchestrator.MaterializationClaim`: when working inside the orchestrator
    on duplicate materialization prevention, active/succeeded/expired claims, or
    claim-backed skip behavior across overlapping runs
  - `FavnOrchestrator.Freshness.StateWriter`: when execution code needs to build
    or persist latest asset freshness state immediately after step completion
  - `FavnOrchestrator.Repair.RuntimeState`: when implementing or running
    reusable crash repair for orphaned runs, stale active step events, claims,
    leases, backfill parent reprojection, or conservative freshness rebuilds
  - `Favn.Freshness.Policy`: when you need accepted `@freshness` values such as
    `:daily`, `{:daily, timezone: "Europe/Oslo"}`, `[max_age: {:hours, 6}]`,
    `[window_success: true]`, and `:always`
  - `Favn.Freshness.Key`: when you need exact freshness-state keys for latest,
    calendar, or window-scoped successes
  - `FavnOrchestrator.RefreshPolicy`: when orchestrator run submission needs
    `:auto`, `:force`, `:missing`, or selected forced assets
  - `FavnOrchestrator.Freshness.Query`: when internal control-plane code needs
    to explain stale assets from current upstream freshness versions
  - `Favn.Backfill.RangeResolver`: when operator backfill input must become
    concrete hourly/daily/monthly/yearly anchors before submission
  - `FavnOrchestrator.BackfillManager`: when working inside the orchestrator on
    parent backfill runs, child run submission, and the normalized backfill
    window ledger
  - `FavnOrchestrator.Backfill.Projector`: when working inside the orchestrator
    on derived backfill-window and asset-window state after child run
    transitions
  - `FavnOrchestrator.Backfill.CoverageProjector`: when working inside the
    orchestrator on safe coverage baseline projection from successful run
    metadata
  - `Favn.RuntimeConfig.Ref`: when you need the manifest-safe representation of
    required environment values and secret environment values
  - `Favn.SQL.Adapter.DuckDB`: when a DuckDB connection needs
    `config_schema_fields/0`, `open: [database: ...]`, keyed `duckdb.attach`
    catalogs, DuckLake session setup, Azure ADLS DuckDB secrets, PostgreSQL
    metadata secret wiring, DuckLake PostgreSQL connection sizing, or
    catalog-level write admission
  - `Favn.SQL.Adapter.DuckDB.ADBC`: when the ADBC DuckDB adapter needs the same
    DuckDB/DuckLake config shape with explicit DuckDB driver control
  - `Favn.Azure.PostgresEntraToken`: when DuckDB bootstrap needs runtime Azure
    PostgreSQL Entra token acquisition through managed identity or Azure CLI

  ## Working Style

  - Prefer `mix favn.read_doc ModuleName` before reading source files.
  - Prefer `Favn.Asset` for new Elixir assets and `Favn.SQLAsset` for new SQL
    assets.
  - Prefer the consumer shape above unless the current project already has a
    stronger convention.
  - Keep `@moduledoc` on assets business-oriented. It should explain what the
    data means, not only how the code runs.
  - Read `Favn.Namespace` whenever relation naming, connection defaults, or SQL
    relation references are involved.
  - For external-source ingestion, keep connector/client logic in your project,
    not in Favn. Favn owns the asset/runtime/config/SQL boundaries; your project
    owns API-specific pagination, auth, request, and response logic.
  - Read `Favn.Window` whenever a task mentions backfills,
    hourly/daily/monthly/yearly processing, required runtime windows, or
    incremental SQL materialization. Read `Favn.Window.Policy` and
    `Favn.Window.Request` when the task mentions pipeline windows, scheduler
    anchor resolution, `mix favn.run --window`, or operator/API run input.
  - Read `Favn.Freshness`, `Favn.Freshness.Policy`, and the relevant asset DSL
    docs whenever a task mentions `@freshness`, skipping fresh work, forcing
    refresh, stale downstream assets, or backfill children running only missing
    windows.
  - Read `Favn.Pipeline`, `Favn.Asset`, and orchestrator admission docs whenever
    a task mentions `max_concurrency`, `execution_pool`, rate-limited APIs,
    runner-local versus orchestrator-owned limits, queue reasons, execution
    leases, or why many independent assets should not start at once.
  - Read `FavnOrchestrator.MaterializationClaim` and
    `FavnOrchestrator.Repair.RuntimeState` when a task mentions duplicate asset
    materializations, stuck runs after a crash, orphaned `running`/`queued` steps,
    stale materialization claims, or repairing persisted runtime state.
  - Read `Favn.Backfill.RangeRequest`, `Favn.Backfill.RangeResolver`, and
    `FavnOrchestrator.RefreshPolicy` when a task mentions operational backfill
    ranges, relative `last` ranges, baseline cutover, force-refresh repair,
    successful-window rerun, or expanding operator intent into concrete anchors.
    Read `FavnOrchestrator.Backfill.*` only for internal control-plane
    persistence, projection, and parent/child orchestration work.
  - Read `Favn.Dev` and `apps/favn_local/README.md` when the task is about local
    lifecycle, local pipeline submission, local run investigation, local SQL
    inspection/querying, docs lookup, or packaging, not asset authoring. Read
    `Favn.Dev.Backfill` for the local `mix favn.backfill` workflow over the
    private orchestrator backfill endpoints. Read `Favn.Dev.Run` for
    `mix favn.run`. Read
    `Favn.Dev.Runs` for `mix favn.runs` and `mix favn.logs RUN_ID`. Read
    `Favn.Dev.DataInspection` for `mix favn.inspect` and `mix favn.query`.
    `Mix.Tasks.Favn.Inspect` and `Mix.Tasks.Favn.Query` own CLI app startup for
    local SQL inspection; `Favn.Dev.DataInspection` owns SQL runtime startup
    before opening client sessions.

  ## Related docs outside BEAM docs

  - `README.md`: top-level product overview and quickstart
  - `docs/FEATURES.md`: implemented feature set only
  - `docs/ROADMAP.md`: planned work only
  - `examples/basic-workflow-tutorial`: standalone consumer-style tutorial with
    the canonical source-system raw landing example in
    `FavnReferenceWorkload.Warehouse.Raw.Orders`
  - `docs/structure/README.md`: ownership and folder map
  - `examples/basic-workflow-tutorial`: larger consumer-style example with its
    own layer convention
  """
end
