defmodule Favn.BoundaryExplicitInputsTest do
  use ExUnit.Case, async: true

  alias Favn.Assets.GraphIndex
  alias Favn.Assets.Planner
  alias Favn.Pipeline.Definition
  alias Favn.Pipeline.Resolver
  alias Favn.Triggers.Schedule

  defmodule RawAsset do
    use Favn.Asset

    def asset(_ctx), do: :ok
  end

  defmodule GoldAsset do
    use Favn.Asset

    @depends RawAsset
    def asset(_ctx), do: :ok
  end

  test "planner builds from explicit graph index input" do
    assets = [
      %{ref: {MyApp.Raw, :asset}, module: MyApp.Raw, name: :asset, depends_on: []},
      %{
        ref: {MyApp.Gold, :asset},
        module: MyApp.Gold,
        name: :asset,
        depends_on: [{MyApp.Raw, :asset}]
      }
    ]

    assert {:ok, index} = GraphIndex.build_index(assets)

    assert {:ok, plan} =
             Planner.plan({MyApp.Gold, :asset},
               graph_index: index,
               dependencies: :all
             )

    assert plan.target_refs == [{MyApp.Gold, :asset}]
    assert plan.topo_order == [{MyApp.Raw, :asset}, {MyApp.Gold, :asset}]
  end

  test "planner rejects missing explicit graph input" do
    assert {:error, :missing_graph_index_input} = Planner.plan({MyApp.Gold, :asset})
  end

  test "planner builds from explicit asset modules input" do
    assert {:ok, plan} =
             Planner.plan({GoldAsset, :asset},
               asset_modules: [RawAsset, GoldAsset],
               dependencies: :all
             )

    assert plan.target_refs == [{GoldAsset, :asset}]
    assert plan.topo_order == [{RawAsset, :asset}, {GoldAsset, :asset}]
  end

  test "resolver resolves from explicit assets and schedule lookup" do
    definition = %Definition{
      module: MyApp.Pipelines.Daily,
      name: :daily,
      selectors: [{:module, MyApp.Gold}],
      deps: :all,
      schedule: {:ref, {MyApp.Schedules, :daily}}
    }

    assets = [
      %{ref: {MyApp.Gold, :asset}, module: MyApp.Gold, name: :asset, meta: %{tags: [:daily]}}
    ]

    assert {:ok, named_schedule} =
             Schedule.named(:daily, cron: "0 2 * * *", timezone: "Etc/UTC")

    schedule_lookup = fn MyApp.Schedules, :daily -> {:ok, named_schedule} end

    assert {:ok, resolution} =
             Resolver.resolve(definition,
               assets: assets,
               schedule_lookup: schedule_lookup
             )

    assert resolution.target_refs == [{MyApp.Gold, :asset}]
    assert resolution.pipeline_ctx.schedule.id == :daily
    assert resolution.pipeline_ctx.schedule.cron == "0 2 * * *"
  end

  test "resolver rejects missing explicit assets" do
    definition = %Definition{
      module: MyApp.Pipelines.Daily,
      name: :daily,
      selectors: [{:asset, {MyApp.Gold, :asset}}]
    }

    assert {:error, :missing_assets} = Resolver.resolve(definition)
  end

  test "resolver rejects schedule refs without explicit lookup" do
    definition = %Definition{
      module: MyApp.Pipelines.Daily,
      name: :daily,
      selectors: [{:asset, {MyApp.Gold, :asset}}],
      schedule: {:ref, {MyApp.Schedules, :daily}}
    }

    assets = [%{ref: {MyApp.Gold, :asset}, module: MyApp.Gold, name: :asset, meta: %{}}]

    assert {:error, :missing_schedule_lookup} = Resolver.resolve(definition, assets: assets)
  end
end
