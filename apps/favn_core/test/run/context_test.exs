defmodule Favn.Run.ContextTest do
  use ExUnit.Case, async: true

  alias Favn.Run.AssetContext
  alias Favn.Run.Context
  alias Favn.Run.PipelineContext

  test "context struct keeps runtime invocation fields" do
    now = ~U[2026-04-15 12:00:00Z]

    context = %Context{
      run_id: "run_1",
      target_refs: [{MyApp.Asset, :asset}],
      asset: %AssetContext{ref: {MyApp.Asset, :asset}, relation: nil, settings: %{}},
      runtime_config: %{},
      params: %{full_refresh: false},
      window: nil,
      pipeline: nil,
      run_started_at: now,
      stage: 0,
      attempt: 1,
      max_attempts: 1
    }

    assert context.run_id == "run_1"
    assert context.asset.ref == {MyApp.Asset, :asset}
    assert context.params == %{full_refresh: false}
  end

  test "pipeline context canonicalizes resolver schedules for the runner contract" do
    {:ok, schedule} =
      Favn.Triggers.Schedule.new_inline(
        cron: "0 2 * * *",
        timezone: "Europe/Oslo",
        missed: :one,
        overlap: :queue_one
      )

    context =
      PipelineContext.from_map(%{
        module: MyApp.Pipelines.Daily,
        name: :daily,
        schedule: schedule
      })

    assert context.schedule == %Favn.Manifest.Schedule{
             module: MyApp.Pipelines.Daily,
             name: :daily,
             ref: {MyApp.Pipelines.Daily, :daily},
             kind: :cron,
             cron: "0 2 * * *",
             timezone: "Europe/Oslo",
             missed: :one,
             overlap: :queue_one,
             active: true,
             origin: :inline
           }
  end
end
