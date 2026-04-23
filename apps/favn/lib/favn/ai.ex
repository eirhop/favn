defmodule Favn.AI do
  @moduledoc """
  AI-oriented documentation entrypoint for learning and using Favn.

  Favn is an Elixir library for defining business-oriented data assets,
  describing how they depend on each other, compiling them into an explicit
  manifest, and then using that manifest for planning and runtime execution.

  Use this module to decide which docs to read next with `mix favn.read_doc`.

  ## What To Read

  | Task | Read | Then read |
  | ---- | ---- | --------- |
  | Author one Elixir asset | `Favn.Asset` | `Favn.Namespace`, `Favn.Window` |
  | Author one SQL asset | `Favn.SQLAsset` | `Favn.SQL`, `Favn.Connection`, `Favn.Namespace`, `Favn.Window` |
  | Author many similar assets | `Favn.MultiAsset` | `Favn.Namespace`, `Favn.Window` |
  | Declare external source relations | `Favn.Source` | `Favn.Namespace` |
  | Share relation defaults | `Favn.Namespace` | the asset/source module you are using |
  | Reuse SQL definitions | `Favn.SQL` | `Favn.SQLAsset` |
  | Define a pipeline | `Favn.Pipeline` | `Favn.Triggers.Schedules` |
  | Define reusable schedules | `Favn.Triggers.Schedules` | `Favn.Pipeline` |
  | Work with windows/backfills | `Favn.Window` | `Favn plan_asset_run` |
  | Define connection contracts | `Favn.Connection` | `Favn.SQLAsset` |
  | Compile a manifest | `Favn generate_manifest` | `Favn.Manifest.Generator` |
  | Resolve pipeline targets | `Favn resolve_pipeline` | `Favn.Pipeline.Resolver` |
  | Plan execution order | `Favn plan_asset_run` | `Favn.Assets.Planner` |
  | Run local tooling | `Favn.Dev` | `apps/favn_local/README.md` |
  | Inspect public helper functions | `Favn` | the specific function doc |

  ## Read `Favn` Only When

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

  ## Working Style

  - Prefer `mix favn.read_doc ModuleName` before reading source files.
  - Prefer `Favn.Asset` for new Elixir assets and `Favn.SQLAsset` for new SQL
    assets.
  - Read `Favn.Namespace` whenever relation naming, connection defaults, or SQL
    relation references are involved.
  - Read `Favn.Window` whenever a task mentions backfills, daily/hourly/monthly
    processing, or incremental SQL materialization.
  - Read `Favn.Dev` and `apps/favn_local/README.md` when the task is about local
    lifecycle or packaging, not asset authoring.

  ## Related docs outside BEAM docs

  - `README.md`: top-level product overview and quickstart
  - `docs/FEATURES.md`: implemented feature set only
  - `docs/ROADMAP.md`: planned work only
  - `docs/lib_structure.md`: ownership and folder map
  """
end
