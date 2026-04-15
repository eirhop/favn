defmodule Favn.Manifest.PipelineResolverTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Index
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.PipelineResolver
  alias Favn.Manifest.Schedule

  test "resolves persisted pipeline selectors through manifest index" do
    assert {:ok, index} = sample_manifest() |> Index.build()

    assert {:ok, resolution} =
             PipelineResolver.resolve(index, {MyApp.Pipelines.Daily, :daily},
               trigger: %{kind: :schedule},
               params: %{full_refresh: false}
             )

    assert resolution.target_refs == [{MyApp.Gold, :asset}, {MyApp.Raw, :asset}]
    assert resolution.dependencies == :all
    assert resolution.pipeline_ctx.schedule.name == :daily
    assert resolution.pipeline_ctx.trigger == %{kind: :schedule}
    assert resolution.pipeline_ctx.params == %{full_refresh: false}
  end

  test "returns clear errors for missing schedule refs" do
    assert {:ok, index} = sample_manifest() |> Index.build()

    pipeline = %Pipeline{
      module: MyApp.Pipelines.Broken,
      name: :broken,
      selectors: [{:asset, {MyApp.Gold, :asset}}],
      deps: :all,
      schedule: {:ref, {MyApp.Schedules, :missing}}
    }

    assert {:error, :schedule_not_found} = PipelineResolver.resolve(index, pipeline, [])
  end

  test "returns pipeline_resolved_empty when selectors match no assets" do
    assert {:ok, index} = sample_manifest() |> Index.build()

    pipeline = %Pipeline{
      module: MyApp.Pipelines.Empty,
      name: :empty,
      selectors: [{:tag, :never}],
      deps: :all
    }

    assert {:error, :pipeline_resolved_empty} = PipelineResolver.resolve(index, pipeline, [])
  end

  defp sample_manifest do
    %Manifest{
      assets: [
        %Asset{
          ref: {MyApp.Gold, :asset},
          module: MyApp.Gold,
          name: :asset,
          depends_on: [{MyApp.Raw, :asset}],
          metadata: %{tags: [:daily], category: :gold}
        },
        %Asset{
          ref: {MyApp.Raw, :asset},
          module: MyApp.Raw,
          name: :asset,
          depends_on: [],
          metadata: %{tags: [:daily], category: :raw}
        }
      ],
      pipelines: [
        %Pipeline{
          module: MyApp.Pipelines.Daily,
          name: :daily,
          selectors: [{:tag, :daily}, {:category, :gold}],
          deps: :all,
          schedule: {:ref, {MyApp.Schedules, :daily}},
          metadata: %{owner: :ops}
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
