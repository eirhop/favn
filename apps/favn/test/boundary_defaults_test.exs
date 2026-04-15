defmodule Favn.BoundaryDefaultsTest do
  use ExUnit.Case, async: false

  defmodule TestSchedules do
    use Favn.Triggers.Schedules

    schedule(:daily, cron: "0 2 * * *", timezone: "Etc/UTC")
  end

  defmodule RawAsset do
    use Favn.Asset

    def asset(_ctx), do: :ok
  end

  defmodule GoldAsset do
    use Favn.Asset

    @depends RawAsset
    def asset(_ctx), do: :ok
  end

  defmodule DailyPipeline do
    use Favn.Pipeline

    pipeline :daily do
      select do
        module(GoldAsset)
      end

      deps(:all)
      schedule({TestSchedules, :daily})
    end
  end

  setup do
    previous_assets = Application.get_env(:favn, :asset_modules)

    Application.put_env(:favn, :asset_modules, [RawAsset, GoldAsset])

    on_exit(fn ->
      if is_nil(previous_assets) do
        Application.delete_env(:favn, :asset_modules)
      else
        Application.put_env(:favn, :asset_modules, previous_assets)
      end
    end)

    :ok
  end

  test "public plan API supplies asset module defaults before delegating" do
    assert {:ok, plan} = Favn.plan_asset_run({GoldAsset, :asset}, dependencies: :all)

    assert plan.target_refs == [{GoldAsset, :asset}]
    assert plan.topo_order == [{RawAsset, :asset}, {GoldAsset, :asset}]
  end

  test "public pipeline resolve API supplies assets and schedule lookup defaults" do
    assert {:ok, resolution} = Favn.resolve_pipeline(DailyPipeline)

    assert resolution.target_refs == [{GoldAsset, :asset}]
    assert resolution.pipeline_ctx.schedule.ref == {TestSchedules, :daily}
  end

  test "public pipeline resolve API honors explicit assets override" do
    assert {:ok, assets} = Favn.list_assets([RawAsset, GoldAsset])
    Application.put_env(:favn, :asset_modules, [])

    assert {:ok, resolution} = Favn.resolve_pipeline(DailyPipeline, assets: assets)
    assert resolution.target_refs == [{GoldAsset, :asset}]
    assert resolution.pipeline_ctx.schedule.ref == {TestSchedules, :daily}
  end
end
