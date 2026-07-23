defmodule Favn.Pipeline.ResolverParityTest do
  use ExUnit.Case, async: false

  alias Favn.Pipeline.Definition
  alias Favn.Pipeline.Resolver
  alias Favn.Triggers.Schedule
  alias Favn.Window.{Anchor, Selection}

  test "resolve/2 applies additive selector union semantics" do
    definition = %Definition{
      module: MyApp.Pipelines.Daily,
      name: :daily,
      selectors: [{:tag, :daily}, {:category, :gold}, {:module, MyApp.Reporting}],
      deps: :none
    }

    assets = [
      %{
        ref: {MyApp.Reporting, :snapshot},
        module: MyApp.Reporting,
        name: :snapshot,
        meta: %{tags: [:daily], category: :gold}
      },
      %{
        ref: {MyApp.Sales, :extract},
        module: MyApp.Sales,
        name: :extract,
        meta: %{tags: [:daily], category: :bronze}
      },
      %{
        ref: {MyApp.Sales, :daily_gold},
        module: MyApp.Sales,
        name: :daily_gold,
        meta: %{tags: [:daily], category: :gold}
      }
    ]

    assert {:ok, resolution} = Resolver.resolve(definition, assets: assets)

    assert resolution.dependencies == :none

    assert resolution.target_refs == [
             {MyApp.Reporting, :snapshot},
             {MyApp.Sales, :daily_gold},
             {MyApp.Sales, :extract}
           ]
  end

  test "resolve/2 keeps explicit schedule timezone over app default" do
    assert {:ok, schedule} = Schedule.new_inline(cron: "0 2 * * *", timezone: "UTC")

    definition = %Definition{
      module: MyApp.Pipelines.Daily,
      name: :daily,
      selectors: [{:asset, {MyApp.Sales, :daily_gold}}],
      schedule: {:inline, schedule}
    }

    assets = [
      %{ref: {MyApp.Sales, :daily_gold}, module: MyApp.Sales, name: :daily_gold, meta: %{}}
    ]

    assert {:ok, resolution} =
             Resolver.resolve(definition,
               assets: assets,
               default_timezone: "Europe/Oslo",
               default_timezone_source: :application_default
             )

    assert resolution.pipeline_ctx.schedule.timezone == "UTC"
    assert resolution.pipeline_ctx.schedule.timezone_source == :local
  end

  test "resolve/2 applies an explicit build default to schedules without timezone" do
    assert {:ok, schedule} = Schedule.new_inline(cron: "0 2 * * *")

    definition = %Definition{
      module: MyApp.Pipelines.Daily,
      name: :daily,
      selectors: [{:asset, {MyApp.Sales, :daily_gold}}],
      schedule: {:inline, schedule}
    }

    assets = [
      %{ref: {MyApp.Sales, :daily_gold}, module: MyApp.Sales, name: :daily_gold, meta: %{}}
    ]

    assert {:ok, resolution} =
             Resolver.resolve(definition,
               assets: assets,
               default_timezone: "Europe/Oslo",
               default_timezone_source: :application_default
             )

    assert resolution.pipeline_ctx.schedule.timezone == "Europe/Oslo"
    assert resolution.pipeline_ctx.schedule.timezone_source == :application_default
  end

  test "resolve/2 carries pipeline execution policy into pipeline context" do
    definition = %Definition{
      module: MyApp.Pipelines.Daily,
      name: :daily,
      selectors: [{:asset, {MyApp.Sales, :daily_gold}}],
      max_concurrency: 2,
      execution_pool: :github_api
    }

    assets = [
      %{ref: {MyApp.Sales, :daily_gold}, module: MyApp.Sales, name: :daily_gold, meta: %{}}
    ]

    assert {:ok, resolution} = Resolver.resolve(definition, assets: assets)
    assert resolution.pipeline_ctx.max_concurrency == 2
    assert resolution.pipeline_ctx.execution_pool == :github_api
  end

  test "resolve/2 rejects invalid pipeline execution policy" do
    assets = [
      %{ref: {MyApp.Sales, :daily_gold}, module: MyApp.Sales, name: :daily_gold, meta: %{}}
    ]

    definition = %Definition{
      module: MyApp.Pipelines.Daily,
      name: :daily,
      selectors: [{:asset, {MyApp.Sales, :daily_gold}}],
      max_concurrency: 0
    }

    assert {:error, {:invalid_max_concurrency, 0}} = Resolver.resolve(definition, assets: assets)

    definition = %Definition{definition | max_concurrency: nil, execution_pool: "github_api"}

    assert {:error, {:invalid_execution_pool, "github_api"}} =
             Resolver.resolve(definition, assets: assets)
  end

  test "resolve/2 reports invalid schedule references and invalid opts" do
    definition = %Definition{
      module: MyApp.Pipelines.Daily,
      name: :daily,
      selectors: [{:asset, {MyApp.Sales, :daily_gold}}],
      schedule: {:ref, {MyApp.Schedules, :daily}}
    }

    assets = [
      %{ref: {MyApp.Sales, :daily_gold}, module: MyApp.Sales, name: :daily_gold, meta: %{}}
    ]

    assert {:error, :missing_schedule_lookup} = Resolver.resolve(definition, assets: assets)

    assert {:error, :schedule_not_found} =
             Resolver.resolve(definition,
               assets: assets,
               schedule_lookup: fn _module, _name -> {:error, :schedule_not_found} end
             )

    assert {:error, :invalid_run_params} =
             Resolver.resolve(definition, assets: assets, params: :bad)

    assert {:error, :invalid_pipeline_trigger} =
             Resolver.resolve(definition, assets: assets, trigger: :bad)

    assert {:error, {:invalid_schedule_lookup, :bad}} =
             Resolver.resolve(definition, assets: assets, schedule_lookup: :bad)

    assert {:error, {:invalid_assets_opt, :bad}} = Resolver.resolve(definition, assets: :bad)
  end

  test "resolve/2 carries a canonical selection into authoring pipeline context" do
    definition = %Definition{
      module: MyApp.Pipelines.Daily,
      name: :daily,
      selectors: [{:asset, {MyApp.Sales, :daily_gold}}],
      window: Favn.Window.Policy.new!(:day, timezone: "Etc/UTC")
    }

    assets = [
      %{ref: {MyApp.Sales, :daily_gold}, module: MyApp.Sales, name: :daily_gold, meta: %{}}
    ]

    anchor = Anchor.new!(:day, ~U[2026-07-01 00:00:00Z], ~U[2026-07-02 00:00:00Z])
    assert {:ok, selection} = Selection.manual(anchor, "Etc/UTC")

    assert {:ok, resolution} =
             Resolver.resolve(definition, assets: assets, window_selection: selection)

    assert resolution.pipeline_ctx.window_selection == selection
    assert resolution.pipeline_ctx.anchor_window == anchor
  end
end
