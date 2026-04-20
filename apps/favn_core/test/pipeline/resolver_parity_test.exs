defmodule Favn.Pipeline.ResolverParityTest do
  use ExUnit.Case, async: true

  alias Favn.Pipeline.Definition
  alias Favn.Pipeline.Resolver
  alias Favn.Triggers.Schedule

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
    previous_scheduler = Application.get_env(:favn, :scheduler)
    Application.put_env(:favn, :scheduler, default_timezone: "Europe/Oslo")

    on_exit(fn ->
      if is_nil(previous_scheduler) do
        Application.delete_env(:favn, :scheduler)
      else
        Application.put_env(:favn, :scheduler, previous_scheduler)
      end
    end)

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

    assert {:ok, resolution} = Resolver.resolve(definition, assets: assets)
    assert resolution.pipeline_ctx.schedule.timezone == "UTC"
    assert resolution.pipeline_ctx.schedule.timezone_source == :schedule
  end

  test "resolve/2 applies scheduler default timezone for inline schedules without timezone" do
    previous_scheduler = Application.get_env(:favn, :scheduler)
    Application.put_env(:favn, :scheduler, default_timezone: "Europe/Oslo")

    on_exit(fn ->
      if is_nil(previous_scheduler) do
        Application.delete_env(:favn, :scheduler)
      else
        Application.put_env(:favn, :scheduler, previous_scheduler)
      end
    end)

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

    assert {:ok, resolution} = Resolver.resolve(definition, assets: assets)
    assert resolution.pipeline_ctx.schedule.timezone == "Europe/Oslo"
    assert resolution.pipeline_ctx.schedule.timezone_source == :app_default
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
end
