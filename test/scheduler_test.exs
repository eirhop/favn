defmodule Favn.SchedulerTest do
  use ExUnit.Case

  alias Favn.Scheduler.State
  alias Favn.Test.Fixtures.Assets.Pipeline.CtxRecorder
  alias Favn.Test.Fixtures.Assets.Pipeline.ReportingAssets
  alias Favn.Test.Fixtures.Assets.Pipeline.SalesAssets
  alias Favn.Test.Fixtures.Pipelines.SchedulerDailyPipeline
  alias Favn.Test.Fixtures.Pipelines.SchedulerInactivePipeline

  setup_all do
    start_supervised!(CtxRecorder)
    :ok
  end

  setup do
    state = Favn.TestSetup.capture_state()

    :ok =
      Favn.TestSetup.setup_asset_modules([SalesAssets, ReportingAssets],
        reload_graph?: true
      )

    Application.put_env(:favn, :pipeline_modules, [
      SchedulerDailyPipeline,
      SchedulerInactivePipeline
    ])

    Application.put_env(:favn, :scheduler,
      enabled: true,
      default_timezone: "Etc/UTC",
      tick_ms: 60_000
    )

    :ok = Favn.TestSetup.clear_memory_storage_adapter()
    :ok = Favn.TestSetup.clear_memory_scheduler_storage()
    CtxRecorder.reset()

    :ok = Favn.Scheduler.reload()

    on_exit(fn ->
      :ok = Favn.TestSetup.restore_state(state, reload_graph?: true)
      _ = Favn.Scheduler.reload()
    end)

    :ok
  end

  test "scheduler discovers pipelines and skips inactive schedules" do
    scheduled = Favn.Scheduler.list_scheduled_pipelines()

    assert Enum.any?(scheduled, &(&1.module == SchedulerDailyPipeline and &1.schedule.active))

    assert Enum.any?(
             scheduled,
             &(&1.module == SchedulerInactivePipeline and not &1.schedule.active)
           )

    now = DateTime.utc_now() |> DateTime.truncate(:second)
    now = %{now | second: 0, microsecond: {0, 0}}
    two_minutes_ago = DateTime.add(now, -120, :second)

    :ok =
      Favn.Scheduler.Storage.put_state(%State{
        pipeline_module: SchedulerDailyPipeline,
        schedule_id: :scheduler_daily,
        last_due_at: two_minutes_ago
      })

    :ok =
      Favn.Scheduler.Storage.put_state(%State{
        pipeline_module: SchedulerInactivePipeline,
        schedule_id: :scheduler_inactive,
        last_due_at: two_minutes_ago
      })

    :ok = Favn.Scheduler.reload()
    :ok = Favn.Scheduler.tick()

    assert {:ok, runs} = Favn.list_runs()

    assert Enum.any?(runs, fn run ->
             run.submit_ref == SchedulerDailyPipeline and run.pipeline.trigger.kind == :schedule
           end)

    refute Enum.any?(runs, fn run -> run.submit_ref == SchedulerInactivePipeline end)
  end

  test "scheduler persists trigger provenance and anchor window on scheduled run" do
    now = DateTime.utc_now() |> DateTime.truncate(:second)
    now = %{now | second: 0, microsecond: {0, 0}}

    :ok =
      Favn.Scheduler.Storage.put_state(%State{
        pipeline_module: SchedulerDailyPipeline,
        schedule_id: :scheduler_daily,
        last_due_at: DateTime.add(now, -120, :second)
      })

    :ok = Favn.Scheduler.reload()
    :ok = Favn.Scheduler.tick()

    assert {:ok, runs} = Favn.list_runs(limit: 1)
    [run | _] = runs

    assert run.submit_ref == SchedulerDailyPipeline
    assert run.pipeline.trigger.kind == :schedule
    assert run.pipeline.trigger.pipeline.module == SchedulerDailyPipeline
    assert run.pipeline.trigger.schedule.active == true
    assert is_binary(run.pipeline.trigger.occurrence.occurrence_key)
    assert %Favn.Window.Anchor{} = run.pipeline.anchor_window
  end
end
