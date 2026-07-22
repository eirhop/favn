defmodule Favn.Manifest.PipelineResolverTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Index
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.PipelineResolver
  alias Favn.Manifest.Schedule
  alias Favn.Window.{Anchor, Selection}

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
    assert resolution.pipeline_ctx.max_concurrency == 2
    assert resolution.pipeline_ctx.execution_pool == :warehouse_api
    assert resolution.pipeline_ctx.trigger == %{kind: :schedule}
    refute Map.has_key?(resolution.pipeline_ctx, :params)
    assert resolution.pipeline_ctx.module == MyApp.Pipelines.Daily
    assert resolution.pipeline_ctx.dependencies == :all
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

  test "gives an inline schedule the owning pipeline identity" do
    asset = %Asset{ref: {MyApp.Raw, :asset}, module: MyApp.Raw, name: :asset}
    {:ok, graph} = Graph.build([asset])

    pipeline = %Pipeline{
      module: MyApp.Pipelines.Inline,
      name: :hourly,
      selectors: [{:asset, asset.ref}],
      schedule: {:inline, %Schedule{cron: "0 * * * *", timezone: "Etc/UTC", origin: :inline}}
    }

    {:ok, index} =
      %Manifest{assets: [asset], pipelines: [pipeline], graph: graph} |> Index.build()

    assert {:ok, resolution} = PipelineResolver.resolve(index, pipeline, [])

    assert resolution.pipeline_ctx.schedule.name == :hourly
    assert resolution.pipeline_ctx.schedule.module == MyApp.Pipelines.Inline
    assert resolution.pipeline_ctx.schedule.ref == {MyApp.Pipelines.Inline, :hourly}
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

  test "carries one canonical selection and rejects competing anchor input" do
    assert {:ok, index} = sample_manifest() |> Index.build()

    pipeline =
      index
      |> Index.list_pipelines()
      |> List.first()
      |> Map.put(:window, Favn.Window.Policy.new!(:day, timezone: "Etc/UTC"))

    anchor = Anchor.new!(:day, ~U[2026-07-01 00:00:00Z], ~U[2026-07-02 00:00:00Z])
    assert {:ok, selection} = Selection.manual(anchor, "Etc/UTC")

    assert {:ok, resolution} =
             PipelineResolver.resolve(index, pipeline, window_selection: selection)

    assert resolution.pipeline_ctx.window_selection == selection
    assert resolution.pipeline_ctx.anchor_window == anchor

    assert {:error, :ambiguous_window_selection} =
             PipelineResolver.resolve(index, pipeline,
               anchor_window: anchor,
               window_selection: selection
             )

    assert {:ok, month} =
             Selection.manual(
               Anchor.new!(:month, ~U[2026-07-01 00:00:00Z], ~U[2026-08-01 00:00:00Z]),
               "Etc/UTC"
             )

    assert {:error, {:window_kind_mismatch, :day, :month}} =
             PipelineResolver.resolve(index, pipeline, window_selection: month)

    assert {:ok, scheduled} = Selection.scheduled(anchor, 1, "Etc/UTC")

    assert {:error, {:window_lookback_mismatch, 0, 1}} =
             PipelineResolver.resolve(index, pipeline, window_selection: scheduled)

    oslo_anchor =
      Anchor.new!(
        :day,
        DateTime.new!(
          ~D[2026-07-01],
          ~T[00:00:00],
          "Europe/Oslo",
          Favn.Timezone.database!()
        ),
        DateTime.new!(
          ~D[2026-07-02],
          ~T[00:00:00],
          "Europe/Oslo",
          Favn.Timezone.database!()
        ),
        timezone: "Europe/Oslo"
      )

    assert {:ok, oslo_manual} = Selection.manual(oslo_anchor, "Europe/Oslo")

    assert {:ok, %{pipeline_ctx: %{window_selection: ^oslo_manual}}} =
             PipelineResolver.resolve(index, pipeline, window_selection: oslo_manual)

    assert {:ok, oslo_backfill} = Selection.backfill([oslo_anchor], "Europe/Oslo")

    assert {:ok, %{pipeline_ctx: %{window_selection: ^oslo_backfill}}} =
             PipelineResolver.resolve(index, pipeline, window_selection: oslo_backfill)
  end

  defp sample_manifest do
    assets =
      [
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
      ]

    {:ok, graph} = Graph.build(assets)

    %Manifest{
      assets: assets,
      graph: graph,
      pipelines: [
        %Pipeline{
          module: MyApp.Pipelines.Daily,
          name: :daily,
          selectors: [{:tag, :daily}, {:category, :gold}],
          deps: :all,
          schedule: {:ref, {MyApp.Schedules, :daily}},
          max_concurrency: 2,
          execution_pool: :warehouse_api,
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
