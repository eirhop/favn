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
  Add the optional `:favn_azure` package when runner code or DuckDB session
  scripts need cached Azure CLI or managed-identity access tokens.

  Do not add internal runtime/control-plane apps such as `:favn_storage_sqlite`,
  `:favn_orchestrator`, `:favn_runner`, `:favn_local`, `:favn_core`, or
  `:favn_sql_runtime` as ordinary consumer dependencies. Local SQLite
  control-plane storage is selected through `config :favn, :local` or
  `mix favn.dev --sqlite`.

  ## What To Read

  - To author one Elixir asset, read `Favn.Asset`, then `Favn.Namespace` and
    `Favn.Window` if needed.
  - To choose where a value belongs, use this fixed runtime model: non-secret
    static asset values use `settings` and `ctx.asset.settings`; pipeline
    settings use `ctx.pipeline.settings`; submitted run values use `ctx.params`;
    environment-dependent values and secrets use `runtime_config` and
    `ctx.runtime_config`; metadata remains descriptive. Read `Favn.Settings` and
    `Favn.Run.Context` for the exact shapes.
  - To declare required runtime configuration or secrets for assets, read
    `Favn.Asset` and `Favn.RuntimeConfig.Ref`.
  - To declare asset freshness or understand skip/force behavior, read
    `Favn.Freshness`, then `Favn.Freshness.Policy` for `freshness` input values
    and `Favn.Freshness.Key` for stored freshness keys. Asset DSL docs for
    `Favn.Asset`, `Favn.SQLAsset`, and `Favn.MultiAsset` show
    where `freshness` must be attached.
  - To author a source-system raw landing asset, read `Favn.Asset`, then
    `Favn.SQLClient`, `Favn.Namespace`, and the standalone tutorial at
    `examples/basic-workflow-tutorial`. The canonical pattern is: declare
    source IDs/tokens with `runtime_config/2`, read resolved values from
    `ctx.runtime_config`, call a source client outside the asset, write raw rows through
    `Favn.SQLClient`, and return structured metadata with row counts, relation,
    load mode, timestamp, and hashed source identity.
  - To author one SQL asset, including a typed output `contract`,
    behaviour-based runtime inputs, or transactional `check` declarations with
    fail, warning, or successful warning/no-op outcomes, read `Favn.SQLAsset`,
    then `Favn.SQL.Contract`, `Favn.SQLAsset.RuntimeInputs`, `Favn.SQL`,
    `Favn.Connection`, `Favn.Namespace`, and `Favn.Window` as needed. A contract
    describes ordered output columns, grain, keys, minimum row count, and
    explicit lineage; it does not generate SQL. Runtime input values remain
    bound parameters, including through reusable `defsql`. Referenced scalar
    SQLAsset settings also become bound parameters; settings cannot provide
    relation identifiers, and a settings/params collision is an error.
    Contract-generated
    and authored checks use the normal check engine; `query()` is the exact
    staged candidate and `target()` is the transaction-visible owned relation.
    The only runtime-input declaration is
    `runtime_inputs MyApp.Inputs`; do not invent an inline block, anonymous
    function, capture, or MFA form. The HexDocs guide
    [Runtime Inputs For SQL Assets](sql-runtime-inputs.html) is the complete
    author workflow.
  - To author many similar assets in one module, read `Favn.MultiAsset`.
    Shared declarations are defaults, child declarations override, and child
    descriptions use `description`; the removed `Favn.Assets` DSL must not be
    suggested.
  - To declare external source relations, read `Favn.Source`; use the real
    module `@moduledoc` for its description.
  - To share relation defaults, read `Favn.Namespace`.
  - To define a pipeline, read `Favn.Pipeline`, then
    `Favn.Triggers.Schedules` if schedules are involved. If the pipeline is
    windowed, also read `Favn.Window`, `Favn.Window.Policy`, and
    `Favn.Window.Request`. If the pipeline or its assets touch rate-limited
    source systems or shared infrastructure, read the `max_concurrency` and
    `execution_pool` sections in `Favn.Pipeline` and `Favn.Asset`.
  - To configure retries, reruns, replay input behavior, or runtime-input pins,
    first read [Retries, Replay, And Runtime-Input Pins](retries-and-replay.html),
    then `Favn.Retry.Policy`, `Favn.Retry.Backoff`, `Favn.Pipeline`, and the
    applicable asset DSL. Retry policy controls attempt count and timing only;
    it never authorizes an unknown-outcome write or external side effect. Read
    [Runtime Inputs For SQL Assets](sql-runtime-inputs.html) when a SQL asset
    selects execution-specific bind values.
  - To limit asset execution before asset code starts, use orchestrator-owned
    execution concurrency controls: pipeline `max_concurrency`, pipeline
    `execution_pool`, asset `execution_pool`, and `config :favn,
    execution_pools: [...]`. Do not model artificial dependencies only to
    serialize independent assets, and do not confuse these controls with SQL
    `write_concurrency`, which protects writer/backend admission after asset
    execution has already started.
  - To debug duplicate materializations, reruns that should skip already-finished
    work, or crash recovery after a stopped orchestrator, read
    `FavnOrchestrator.MaterializationClaim`,
    `FavnOrchestrator.Freshness.StateWriter`,
    `FavnOrchestrator.Repair.RuntimeState`, and `Mix.Tasks.Favn.RepairRuntimeState`.
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
  - To start consumer-owned credential caches, API sessions, client pools, rate
    limiters, or other supervised services inside an isolated runner, read
    `Favn.Runner.Plugin`, `Favn.Runner.SupervisedChildren`, and
    [Runner Plugins And Runner-Local Services](runner-plugins.html). Plugin state
    is runner-local and disposable: use it only for rebuildable operational
    state, never durable business data or cross-run coordination that must
    survive a restart. Use the optional plugin application callback when a
    packaged OTP application must start inside the isolated runner. For Azure,
    also read `Favn.Azure.RunnerPlugin` and
    `Favn.Azure.Credentials` from the optional `:favn_azure` package.
  - To configure DuckDB/DuckLake physical-session setup, read
    [DuckDB Session Scripts And Resources](duckdb-session-scripts.html), then
    `Favn.Connection`, `Favn.RuntimeConfig.Ref`, `Favn.SQLAsset`,
    `Favn.Namespace`, and `Favn.SQL.Adapter.DuckDB`. The canonical shape is
    `open: [...]` plus `duckdb: [startup: ..., resources: ..., catalogs: ...]`.
    Native SQL files own `INSTALL`, `LOAD`, `SET`, `CREATE SECRET`, `ATTACH`,
    `USE`, and extension-specific syntax; the removed structured
    `load/settings/secrets/attach/use` forms must not be suggested. SQL assets
    select stable names with `resources [...]`, while namespaces can add
    resources for every descendant SQL asset. Treat Quack and other evolving
    extensions as native SQL resources with a deployment-pinned compatible
    DuckDB build; do not invent extension-specific Favn fields or version gates.
    Environment-backed `Favn.RuntimeConfig.Ref` values resolve at runner startup,
    so they still require a refresh-capable native provider or runner restart
    after rotation. A secret `Favn.RuntimeValue` such as
    `Favn.Azure.Credentials.token_ref/2` instead resolves once during pooled
    session preparation, is reused for that bootstrap, and changes pool identity
    when its token refreshes. DuckLake metadata using Azure Database for
    PostgreSQL requests `https://ossrdbms-aad.database.windows.net` and injects
    the ref as the password in a native DuckDB PostgreSQL secret. Pool idle
    timeout is not a maximum physical-session age. In local development,
    `mix favn.reload` performs that runner restart and reevaluates runtime config.
    If the deployment uses ADBC, also read `Favn.SQL.Adapter.DuckDB.ADBC`. Keep
    DuckLake `write_concurrency` conservative because one logical writer may use
    several PostgreSQL backend connections.
  - To run SQL queries or raw landing writes from plain Elixir code using named
    Favn connections, read `Favn.SQLClient`. For Elixir asset landing helpers,
    prefer `Favn.SQLClient.with_connection/3` so one asset execution can reuse a
    single SQL session across setup, inspection, and write operations. For raw
    DuckDB/DuckLake writes, prefer session `required_catalogs` or explicit
    `admission: [...]` operation catalog targets over relying on SQL text
    inference; Favn does not parse arbitrary SQL for target catalogs. SQL
    sessions are process-owned and must not be shared concurrently across child
    tasks.
  - To compile a manifest, read `Favn generate_manifest`; if the project uses
    `config :favn, discovery: [apps: [...], assets: :all, pipelines: :all,
    schedules: :all]`, also read `Favn.ModuleDiscovery`. Read
    `Favn.Manifest.Generator` if you need internal compilation details.
  - To resolve pipeline targets, read `Favn resolve_pipeline`, then
    `Favn.Pipeline.Resolver` if needed.
  - To plan execution order, read `Favn plan_asset_run`, then
    `Favn.Assets.Planner` if needed.
  - To inspect or cancel local runs, inspect run events, inspect relation
    metadata, inspect relation partitions, or run ad hoc read-only SQL, read
    `Favn.Dev.Runs`, `Favn.Dev.DataInspection`, `Mix.Tasks.Favn.Runs`,
    `Mix.Tasks.Favn.Logs`, `Mix.Tasks.Favn.Inspect`, and `Mix.Tasks.Favn.Query`.
    `mix favn.runs cancel RUN_ID` requests cancellation through the local
    orchestrator HTTP boundary; add `--wait` to poll the run until it is
    terminal. `mix favn.inspect ...` and `mix favn.query
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
    `mix favn.read_doc`. Dev and reload load the project `.env` before evaluating
    `config/runtime.exs`; existing shell values take precedence.
    `mix favn.run` resolves asset and pipeline targets from the active manifest.
    Direct asset repair can combine `--dependencies all|none` with
    `--refresh auto|missing|force_selected|force_selected_upstream|force_all`;
    pipeline runs have the narrower `auto|missing|force_all` refresh contract and
    do not accept `--dependencies`. Read `Mix.Tasks.Favn.Run` and `Favn.Dev.Run`
    before changing this boundary.
  - To inspect the public helper functions collected in one place, read `Favn`.

  ## Transactional SQL Check Breadcrumbs

  When a task mentions SQL quality checks, candidate validation, target
  validation, `query()`, `target()`, `on_violation`, `quality_status`,
  `write_outcome`, or a successful materialization no-op, read these docs in
  order:

  1. `Favn.SQLAsset` for the public authoring and transaction contract.
  2. `Favn.SQLAsset.check/3` for the exact check options and result shape.
  3. `Favn.SQL` when the check calls reusable or file-backed `defsql`.
  4. `Favn.SQL.CheckResult` when interpreting durable run metadata.
  5. `Favn.SQL.Check` only when inspecting the compiled manifest contract; user
     code declares checks through `Favn.SQLAsset.check/3` and does not construct
     this struct directly.

  The package guide `guides/sql-asset-checks.md` is the complete human-facing
  how-to and reference. It covers transaction order, first-target bootstrap,
  fail/warn/no-op policy, metric limits, invalid result shapes, and persisted
  outcomes.

  ## SQL Output Contract Breadcrumbs

  When a task mentions output schema, grain, data contract, column lineage,
  logical types, nullable columns, unique keys, minimum row count, contract
  diffs, `renamed_from`, expected-versus-observed schema, or Contract versus
  Custom checks, read these docs in order:

  1. `Favn.SQLAsset` and `Favn.SQLAsset.contract/1` for the public declaration.
  2. `Favn.SQL.Contract` and its nested column, grain, lineage, unique-key, and
     row-count modules for the typed compiled model.
  3. `Favn.SQL.ContractValidation` for candidate schema enforcement.
  4. `Favn.SQL.Contract.Diff` for semantic manifest-to-manifest changes.
  5. `Favn.SQL.CheckResult` for durable generated and authored check outcomes.

  The canonical lineage form is a plain `from:` list containing internal asset
  tuples or external dataset/field string tuples. Do not invent `input()` or
  `external()` wrappers. A contract never generates a `select` list. Read
  [SQL Output Contracts](sql-output-contracts.html) for the complete human
  workflow, policy behavior, automatic checks, evolution model, and limits.

  ## SQL Runtime Input Breadcrumbs

  When a task mentions `runtime_inputs`, external manifest or snapshot IDs,
  runtime-selected files, watermarks, resolver timeouts, parameter collisions,
  or sensitive SQL bind values, read these docs in order:

  1. `Favn.SQLAsset` for declaration timing, budgets, binding, and retry limits.
  2. `Favn.SQLAsset.RuntimeInputs` for the resolver callback contract.
  3. `Favn.SQLAsset.RuntimeInputs.Result` and
     `Favn.SQLAsset.RuntimeInputs.Error` for the only accepted return shapes.
  4. `Favn.RuntimeInputResolver.Ref` only for manifest/compiler work; authors
     declare the module and do not construct this reference directly.

  The canonical public declaration is `runtime_inputs MyApp.Inputs` before
  `query`. Anonymous functions, captures, MFA tuples, and inline resolver blocks
  are unsupported. Read
  [Runtime Inputs For SQL Assets](sql-runtime-inputs.html) for the complete
  human workflow, limits, redaction rules, retry boundary, and examples.

  ## DuckDB Session Script Breadcrumbs

  When a task mentions DuckDB extensions, settings, secrets, attach, `USE`,
  `resources`, session startup, or native SQL setup, read these docs in order:

  1. [DuckDB Session Scripts And Resources](duckdb-session-scripts.html) for the
     complete public configuration, lifecycle, safety, and misuse example.
  2. `Favn.SQLAsset` for leaf `resources` declaration and manifest behavior.
  3. `Favn.Namespace` for additive inherited resources.
  4. `Favn.Connection` and `Favn.RuntimeConfig.Ref` for runtime values and secret
     parameters.
  5. The selected DuckDB adapter module only for adapter-specific deployment
     behavior.

  Session-script and asset SQL parameters both use `@name`, not `{{name}}`, but
  they are separate scopes. Never reconstruct the removed structured DuckDB
  feature allowlist.

  ## Runner Plugin Breadcrumbs

  When a task mentions a service needed inside an isolated runner, runner-local
  state, a GenServer plugin, credential caching, Azure CLI, managed identity, or
  a future AWS runner integration, read these docs in order:

  1. [Runner Plugins And Runner-Local Services](runner-plugins.html) for the
     public lifecycle, examples, and state-lifetime rule.
  2. `Favn.Runner.Plugin` for computed child specifications or validation.
  3. `Favn.Runner.SupervisedChildren` for the no-boilerplate path.
  4. `Favn.Azure.RunnerPlugin` and `Favn.Azure.Credentials` when using the
     optional Azure package.
  5. `Favn.RuntimeValue` only when an integration must inject a deferred value
     into a boundary that explicitly supports it.

  Plugin state is not durable, replicated, or shared across runners. Never
  describe it as a general way for assets to pass correctness-sensitive data
  between runs.

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
  - `Favn.Freshness.Policy`: when you need accepted `freshness` values such as
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
    `config_schema_fields/0`, `open: [database: ...]`, native startup/resource
    SQL files, catalog-to-resource metadata, script pool fingerprints, or
    catalog-level write admission
  - `Favn.SQL.Adapter.DuckDB.ADBC`: when the ADBC DuckDB adapter needs the same
    DuckDB/DuckLake config shape with explicit DuckDB driver control

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
    docs whenever a task mentions `freshness`, skipping fresh work, forcing
    refresh, stale downstream assets, or backfill children running only missing
    windows.
  - Read `Favn.Pipeline`, `Favn.Asset`, and orchestrator admission docs whenever
    a task mentions `max_concurrency`, `execution_pool`, rate-limited APIs,
    runner-local versus orchestrator-owned limits, queue reasons, execution
    leases, or why many independent assets should not start at once.
  - Read `Favn.SQLAsset` and `Favn.SQLAsset.RuntimeInputs` whenever a task
    mentions runtime-selected files, external manifests/snapshots, watermarks,
    or resolver-provided SQL parameters. Read `Favn.RuntimeInputResolver.Ref`
    only for manifest/compiler work.
  - Read the DuckDB session-script guide, `Favn.SQLAsset`, and `Favn.Namespace`
    whenever a task mentions `resources`, extension setup, settings, secrets,
    catalog attach, or physical-session lifecycle. Treat the files as trusted
    deployment code, keep durable business writes out of them, and account for
    runner restarts when environment-resolved credentials rotate. For cached
    Azure token injection, also read the runner-plugin guide and
    `Favn.Azure.Credentials`.
  - Read the runner-plugin guide, `Favn.Runner.Plugin`, and
    `Favn.Runner.SupervisedChildren` whenever a task mentions consumer-owned
    services inside a runner, GenServer plugins, credential/session caches,
    runner-local state, or future cloud authentication plugins. Preserve the
    disposable-state boundary.
  - Read `Favn.SQLAsset`, `Favn.SQLAsset.check/3`, and `Favn.SQL.CheckResult`
    whenever a task mentions transactional SQL checks, data quality warnings,
    keeping an existing target on an empty candidate, `query()`, `target()`,
    `quality_status`, `write_outcome`, or check result metrics. Read
    `Favn.SQL.Check` only for manifest/compiler work.
  - Read `Favn.SQLAsset.contract/1`, `Favn.SQL.Contract`, and
    `Favn.SQL.ContractValidation` whenever a task mentions SQL output shape,
    grain, keys, column lineage, logical types, contract-generated checks, or
    expected-versus-observed schema. Read `Favn.SQL.Contract.Diff` when
    comparing manifest definitions. User code declares the DSL and does not
    construct these core structs directly.
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
    lifecycle, local pipeline submission, local run investigation or
    cancellation, local SQL inspection/querying, docs lookup, or packaging, not
    asset authoring. Read
    `Favn.Dev.Backfill` for the local `mix favn.backfill` workflow over the
    private orchestrator backfill endpoints. Read `Favn.Dev.Run` for
    `mix favn.run`. Read
    `Favn.Dev.Runs` for `mix favn.runs` list/show/cancel behavior and
    `mix favn.logs RUN_ID`. Read
    `Favn.Dev.DataInspection` for `mix favn.inspect` and `mix favn.query`.
    `Mix.Tasks.Favn.Inspect` and `Mix.Tasks.Favn.Query` own CLI app startup for
    local SQL inspection; `Favn.Dev.DataInspection` owns SQL runtime startup
    before opening client sessions.

  ## Related docs outside BEAM docs

  - `README.md`: top-level product overview and quickstart
  - `docs/FEATURES.md`: implemented feature set only
  - `docs/ROADMAP.md`: planned work only
  - `apps/favn/guides/sql-asset-checks.md`: complete transactional SQL check
    authoring and result reference
  - `apps/favn/guides/sql-output-contracts.md`: canonical SQL output contract
    DSL, enforcement, lineage, policies, assurance, and evolution reference
  - `apps/favn/guides/sql-runtime-inputs.md`: canonical behaviour-based SQL
    runtime input authoring, result/error contracts, limits, and retry boundary
  - `apps/favn/guides/duckdb-session-scripts.md`: native DuckDB session setup,
    resources DSL, file locators, pooling lifecycle, and safety rules
  - `examples/basic-workflow-tutorial`: standalone consumer-style tutorial with
    the canonical source-system raw landing example in
    `FavnReferenceWorkload.Warehouse.Raw.Orders`
  - `docs/structure/README.md`: ownership and folder map
  - `examples/basic-workflow-tutorial`: larger consumer-style example with its
    own layer convention
  """
end
