defmodule Favn.Manifest.DiagnosticsTest do
  use ExUnit.Case, async: true

  alias Favn.Coverage.Effective
  alias Favn.Coverage.Spec, as: CoverageSpec
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Diagnostics
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.Schedule
  alias Favn.Window.Policy
  alias Favn.Window.Spec, as: WindowSpec

  test "warns when selected asset and pipeline timezones differ" do
    asset = asset("Etc/UTC", 0)
    pipeline = pipeline(asset, "Europe/Oslo", "0 8 * * *")

    assert [warning] = Diagnostics.for_manifest(manifest(asset, pipeline))
    assert warning.code == :pipeline_asset_timezone_mismatch
    assert warning.asset_ref == asset.ref
    assert warning.details.pipeline_timezone == "Europe/Oslo"
    assert warning.details.asset_timezone == "Etc/UTC"
  end

  test "warns only when every aligned cron occurrence is before availability" do
    asset = asset("Europe/Oslo", 6 * 3_600)

    early = Diagnostics.for_manifest(manifest(asset, pipeline(asset, "Europe/Oslo", "0 2 * * *")))
    later = Diagnostics.for_manifest(manifest(asset, pipeline(asset, "Europe/Oslo", "0 8 * * *")))

    mixed =
      Diagnostics.for_manifest(manifest(asset, pipeline(asset, "Europe/Oslo", "0 2,8 * * *")))

    weekdays =
      Diagnostics.for_manifest(manifest(asset, pipeline(asset, "Europe/Oslo", "0 2 * * 1-5")))

    assert [%{code: :cron_before_coverage_availability} = warning] = early
    assert warning.details.occurrence_offset_seconds == 2 * 3_600
    assert warning.details.availability_delay_seconds == 6 * 3_600
    assert [%{code: :cron_before_coverage_availability}] = weekdays
    assert later == []
    assert mixed == []
  end

  defp asset(timezone, delay_seconds) do
    window = WindowSpec.new!(:day, timezone: timezone)

    coverage =
      CoverageSpec.new!(
        from: ~D[2026-01-01],
        availability_delay: {:seconds, delay_seconds}
      )

    {:ok, effective} = Effective.resolve(coverage, window, nil)
    ref = {MyApp.Assets.Daily, :asset}

    %Asset{
      ref: ref,
      module: elem(ref, 0),
      name: elem(ref, 1),
      type: :source,
      window: window,
      coverage: effective
    }
  end

  defp pipeline(asset, timezone, cron) do
    schedule = %Schedule{
      module: MyApp.Pipelines.Daily,
      name: :daily,
      ref: {MyApp.Pipelines.Daily, :daily},
      cron: cron,
      timezone: timezone,
      timezone_source: :local,
      origin: :inline
    }

    %Pipeline{
      module: MyApp.Pipelines.Daily,
      name: :daily,
      selectors: [{:asset, asset.ref}],
      window: Policy.new!(:day, timezone: timezone),
      schedule: {:inline, schedule}
    }
  end

  defp manifest(asset, pipeline) do
    {:ok, graph} = Graph.build([asset])
    %Manifest{assets: [asset], pipelines: [pipeline], graph: graph}
  end
end
