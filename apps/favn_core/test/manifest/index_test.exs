defmodule Favn.Manifest.IndexTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Index
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.Schedule

  test "builds deterministic lookup maps and graph index from manifest" do
    manifest = sample_manifest()

    assert {:ok, %Index{} = index} = Index.build(manifest)

    assert {:ok, %Asset{ref: {MyApp.Raw, :asset}}} = Index.fetch_asset(index, {MyApp.Raw, :asset})

    assert {:ok, %Pipeline{name: :daily}} =
             Index.fetch_pipeline(index, {MyApp.Pipelines.Daily, :daily})

    assert {:ok, %Schedule{name: :daily}} =
             Index.fetch_schedule(index, {MyApp.Schedules, :daily})

    assert index.graph_index.topo_order == [{MyApp.Raw, :asset}, {MyApp.Gold, :asset}]
  end

  test "fails when pipeline refs are duplicated" do
    manifest =
      sample_manifest()
      |> Map.put(:pipelines, [
        %Pipeline{
          module: MyApp.Pipelines.Daily,
          name: :daily,
          selectors: [{:asset, {MyApp.Raw, :asset}}]
        },
        %Pipeline{
          module: MyApp.Pipelines.Daily,
          name: :daily,
          selectors: [{:asset, {MyApp.Gold, :asset}}]
        }
      ])

    assert {:error, {:duplicate_pipeline_ref, {MyApp.Pipelines.Daily, :daily}}} =
             Index.build(manifest)
  end

  test "falls back to module and name when ref shape is invalid" do
    manifest =
      sample_manifest()
      |> Map.put(:schedules, [
        %Schedule{module: MyApp.Schedules, name: :daily, ref: :invalid_ref, cron: "0 1 * * *"}
      ])

    assert {:ok, %Index{} = index} = Index.build(manifest)
    assert {:ok, %Schedule{name: :daily}} = Index.fetch_schedule(index, {MyApp.Schedules, :daily})
  end

  test "fails when schedule identity is invalid" do
    manifest =
      sample_manifest()
      |> Map.put(:schedules, [
        %Schedule{module: nil, name: nil, ref: :invalid_ref, cron: "0 1 * * *"}
      ])

    assert {:error, {:invalid_schedule_ref, :invalid_ref}} = Index.build(manifest)
  end

  defp sample_manifest do
    %Manifest{
      assets: [
        %Asset{
          ref: {MyApp.Gold, :asset},
          module: MyApp.Gold,
          name: :asset,
          depends_on: [{MyApp.Raw, :asset}]
        },
        %Asset{ref: {MyApp.Raw, :asset}, module: MyApp.Raw, name: :asset, depends_on: []}
      ],
      pipelines: [
        %Pipeline{
          module: MyApp.Pipelines.Daily,
          name: :daily,
          selectors: [{:asset, {MyApp.Gold, :asset}}],
          deps: :all,
          schedule: {:ref, {MyApp.Schedules, :daily}},
          metadata: %{}
        }
      ],
      schedules: [
        %Schedule{
          module: MyApp.Schedules,
          name: :daily,
          ref: {MyApp.Schedules, :daily},
          cron: "0 1 * * *",
          timezone: "Etc/UTC"
        }
      ]
    }
  end
end
