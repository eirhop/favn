defmodule Favn.AI do
  @moduledoc """
  AI-oriented documentation entrypoint for learning and using Favn.

  Favn is an Elixir library for defining business-oriented data assets,
  describing how they depend on each other, compiling them into an explicit
  manifest, and then using that manifest for planning and runtime execution.

  Use this module to decide which docs to read next with `mix favn.read_doc`.

  ## What To Read

  - To author one Elixir asset, read `Favn.Asset`, then `Favn.Namespace` and
    `Favn.Window` if needed.
  - To declare required runtime configuration or secrets for assets, read
    `Favn.Asset` and `Favn.RuntimeConfig.Ref`.
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
    `Favn.Window.Request`.
  - To work with windows or backfills, read `Favn.Window`, then
    `Favn.Window.Policy` for pipeline/scheduler policy, `Favn.Window.Request`
    for CLI/API run input, and `Favn plan_asset_run` if you need planning
    details.
  - To define connection contracts, read `Favn.Connection`; if connection
    values come from environment variables or secrets, also read
    `Favn.RuntimeConfig.Ref`.
  - To configure DuckDB/DuckLake connection bootstrap for extension loading,
    Azure credential-chain secrets, DuckLake attach, or ADLS paths, read
    `Favn.Connection`, `Favn.RuntimeConfig.Ref`, and
    `Favn.SQL.Adapter.DuckDB`.
  - To run SQL queries from plain Elixir code using named Favn connections, read
    `Favn.SQLClient`.
  - To compile a manifest, read `Favn generate_manifest`, then
    `Favn.Manifest.Generator` if you need internal compilation details.
  - To resolve pipeline targets, read `Favn resolve_pipeline`, then
    `Favn.Pipeline.Resolver` if needed.
  - To plan execution order, read `Favn plan_asset_run`, then
    `Favn.Assets.Planner` if needed.
  - To run local tooling, read `Favn.Dev`, then `apps/favn_local/README.md`.
    The public local command surface is `mix favn.install`, `mix favn.dev`,
    `mix favn.run`, `mix favn.status`, `mix favn.logs`, `mix favn.reload`,
    `mix favn.stop`, `mix favn.reset`, `mix favn.build.runner`,
    `mix favn.build.web`, `mix favn.build.orchestrator`,
    `mix favn.build.single`, and `mix favn.read_doc`.
  - To inspect the public helper functions collected in one place, read `Favn`.

  ## About `Favn`

  - you need helper functions like `generate_manifest`, `resolve_pipeline`, or
    `plan_asset_run`
  - you need the thin public facade shape
  - you are debugging delegation to `FavnAuthoring` or runtime apps

  ## Read Internals Only When

  - `Favn.Manifest.Generator`: when you need the exact manifest compilation path
    from modules to `%Favn.Manifest{}`
  - `Favn.Pipeline.Resolver`: when you need selector normalization, schedule
    resolution, or the exact `%Favn.Pipeline.Resolution{}` shape
  - `Favn.Assets.Planner`: when you need topological stages, dependency
    expansion, anchor windows, or backfill planning
  - `Favn.RuntimeConfig.Ref`: when you need the manifest-safe representation of
    required environment values and secret environment values
  - `Favn.SQL.Adapter.DuckDB`: when a DuckDB connection needs the
    `bootstrap_schema_field/0` helper for DuckLake session setup

  ## Working Style

  - Prefer `mix favn.read_doc ModuleName` before reading source files.
  - Prefer `Favn.Asset` for new Elixir assets and `Favn.SQLAsset` for new SQL
    assets.
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
  - Read `Favn.Dev` and `apps/favn_local/README.md` when the task is about local
    lifecycle, local pipeline submission, docs lookup, or packaging, not asset
    authoring.

  ## Related docs outside BEAM docs

  - `README.md`: top-level product overview and quickstart
  - `docs/FEATURES.md`: implemented feature set only
  - `docs/ROADMAP.md`: planned work only
  - `examples/basic-workflow-tutorial`: standalone consumer-style tutorial with
    the canonical source-system raw landing example in
    `FavnReferenceWorkload.Warehouse.Raw.Orders`
  - `docs/lib_structure.md`: ownership and folder map
  """
end
