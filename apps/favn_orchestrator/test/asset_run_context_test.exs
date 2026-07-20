defmodule FavnOrchestrator.AssetRunContextTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.Schedule
  alias Favn.Manifest.Version
  alias Favn.Window.Policy
  alias FavnOrchestrator.AssetRunContext
  alias FavnOrchestrator.ManifestTarget

  @asset_ref {__MODULE__.Orders, :asset}

  test "requires an explicit stable context when multiple pipelines select an asset" do
    asset = %Asset{ref: @asset_ref, module: elem(@asset_ref, 0), name: :asset}
    version = version_fixture(asset, pipelines(asset))

    assert {:ok, %{status: :ambiguous, selected: nil, contexts: contexts}} =
             AssetRunContext.select(version, asset)

    assert Enum.map(contexts, & &1.id) == Enum.sort(Enum.map(contexts, & &1.id))

    scheduled_id = ManifestTarget.pipeline_id({__MODULE__.Scheduled, :current})

    assert {:ok, %{status: :selected, selected: scheduled}} =
             AssetRunContext.select(version, asset, scheduled_id)

    assert scheduled.pipeline_ref == {__MODULE__.Scheduled, :current}
    assert scheduled.policy.anchor == :current_period
    assert scheduled.timezone == "Europe/Oslo"

    assert {:error, :invalid_asset_run_context} =
             AssetRunContext.select(version, asset, "pipeline:forged")
  end

  test "context-free results are independent of manifest pipeline ordering" do
    asset = %Asset{ref: @asset_ref, module: elem(@asset_ref, 0), name: :asset}
    pipelines = pipelines(asset)

    assert {:ok, left} = AssetRunContext.select(version_fixture(asset, pipelines, "left"), asset)

    assert {:ok, right} =
             AssetRunContext.select(
               version_fixture(asset, Enum.reverse(pipelines), "right"),
               asset
             )

    assert left.status == :ambiguous
    assert right.status == :ambiguous

    assert Enum.map(left.contexts, &AssetRunContext.descriptor/1) ==
             Enum.map(right.contexts, &AssetRunContext.descriptor/1)
  end

  test "a single selecting pipeline remains automatic" do
    asset = %Asset{ref: @asset_ref, module: elem(@asset_ref, 0), name: :asset}
    [pipeline | _] = pipelines(asset)

    assert {:ok, %{status: :selected, selected: context}} =
             AssetRunContext.select(version_fixture(asset, [pipeline], "single"), asset)

    assert context.pipeline_ref == {pipeline.module, pipeline.name}
  end

  defp pipelines(_asset) do
    schedule = schedule_fixture()

    [
      %Pipeline{
        module: __MODULE__.Manual,
        name: :previous,
        selectors: [{:asset, @asset_ref}],
        window: Policy.new!(:monthly, anchor: :previous_complete_period)
      },
      %Pipeline{
        module: __MODULE__.Scheduled,
        name: :current,
        selectors: [{:asset, @asset_ref}],
        schedule: {:ref, schedule.ref},
        window: Policy.new!(:monthly, anchor: :current_period)
      }
    ]
  end

  defp version_fixture(asset, pipelines, suffix \\ "default") do
    {:ok, graph} = Graph.build([asset])

    %Version{
      manifest_version_id: "mv_asset_run_context_#{suffix}",
      content_hash: "sha256:asset-run-context-#{suffix}",
      manifest: %Manifest{
        assets: [asset],
        pipelines: pipelines,
        schedules: [schedule_fixture()],
        graph: graph
      }
    }
  end

  defp schedule_fixture do
    %Schedule{
      module: __MODULE__.Schedules,
      name: :daily,
      ref: {__MODULE__.Schedules, :daily},
      cron: "0 8 * * *",
      timezone: "Europe/Oslo"
    }
  end
end
