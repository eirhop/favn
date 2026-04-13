defmodule Favn.AgentGuide do
  @moduledoc """
  AI-first routing guide for working with Favn.

  Start here when you need to understand the public surface quickly through
  `Code.fetch_docs/1`. This module is a router, not the source of truth for all
  authoring details.

  ## What Favn is

  Favn is an asset-oriented orchestration library for Elixir. Users define
  business-facing assets in Elixir or SQL, Favn compiles them into canonical
  `%Favn.Asset{}` values, then uses the dependency graph to inspect, plan, and
  run workflows.

  ## Public modules to trust first

  - `Favn`: runtime and operator API facade
  - `Favn.Asset`: preferred single-asset Elixir DSL
  - `Favn.SQLAsset`: preferred single-asset SQL DSL
  - `Favn.MultiAsset`: repetitive extraction DSL with generated assets
  - `Favn.Assets`: compact multi-asset function DSL
  - `Favn.Namespace`: inherited relation defaults
  - `Favn.SQL`: reusable SQL DSL and SQL runtime facade
  - `Favn.Pipeline`: pipeline composition DSL
  - `Favn.Triggers.Schedules`: reusable named schedules
  - `Favn.Window`: window constructors used by assets and pipelines
  - `Favn.Connection`: connection provider contract
  - `Favn.Source`: external relation declarations

  Internal modules under `Favn.*.*` are implementation details unless a public
  moduledoc points you there explicitly.

  ## Recommended reading order

  1. Read `Favn.AgentGuide`
  2. Read `Favn` for the public runtime API
  3. Read the specific authoring DSL for the task
  4. Read supporting modules such as `Favn.Namespace`, `Favn.Window`, `Favn.Connection`, or `Favn.Triggers.Schedules`

  ## Which module to read for each task

  - Create one Elixir asset: `Favn.Asset`
  - Create one SQL asset: `Favn.SQLAsset`, then `Favn.SQL`
  - Create repetitive extraction assets: `Favn.MultiAsset`
  - Work in the older compact multi-asset style: `Favn.Assets`
  - Inspect, run, await, rerun, or list runs: `Favn`
  - Define pipelines: `Favn.Pipeline`
  - Work with relation defaults: `Favn.Namespace`
  - Work with windows: `Favn.Window`
  - Work with connections: `Favn.Connection`
  - Work with reusable schedules: `Favn.Triggers.Schedules`
  - Model external upstream relations: `Favn.Source`

  ## Common scenarios

  ### Create one Elixir asset

  Read `Favn.Asset`.

  ### Create one SQL asset

  Read `Favn.SQLAsset` for asset structure, then `Favn.SQL` for `~SQL` and `defsql`.

  ### Create repetitive extraction assets

  Read `Favn.MultiAsset`, especially `defaults/1`, `asset/2`, and the shared `asset/1` runtime contract.

  ### Inspect and run assets or pipelines

  Read `Favn` and look at `list_assets/0`, `get_asset/1`, `run_asset/2`, `await_run/2`, `plan_pipeline/2`, and `run_pipeline/2`.

  ### Work with connections, windows, or schedules

  Read `Favn.Connection`, `Favn.Window`, `Favn.Pipeline`, and `Favn.Triggers.Schedules`.
  """
end
