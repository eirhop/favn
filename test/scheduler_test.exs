defmodule Favn.SchedulerTest do
  use ExUnit.Case
  import ExUnit.CaptureLog

  alias Favn.Scheduler
  alias Favn.Scheduler.State
  alias Favn.Scheduler.Storage
  alias Favn.Test.Fixtures.Assets.Pipeline.CtxRecorder
  alias Favn.Test.Fixtures.Assets.Pipeline.ReportingAssets
  alias Favn.Test.Fixtures.Assets.Pipeline.SalesAssets
  alias Favn.Test.Fixtures.Pipelines.SchedulerDailyPipeline
  alias Favn.Test.Fixtures.Pipelines.SchedulerInactivePipeline
  alias Favn.Test.Fixtures.Pipelines.SchedulerMissedAllPipeline
  alias Favn.Test.Fixtures.Pipelines.SchedulerMissedOnePipeline
  alias Favn.Test.Fixtures.Pipelines.SchedulerMissedSkipPipeline

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

    :ok = Scheduler.reload()

    on_exit(fn ->
      :ok = Favn.TestSetup.restore_state(state, reload_graph?: true)
      _ = Scheduler.reload()
    end)

    :ok
  end

  test "scheduler discovers pipelines and skips inactive schedules" do
    scheduled = Scheduler.list_scheduled_pipelines()

    assert Enum.any?(scheduled, &(&1.module == SchedulerDailyPipeline and &1.schedule.active))

    assert Enum.any?(
             scheduled,
             &(&1.module == SchedulerInactivePipeline and not &1.schedule.active)
           )

    now = DateTime.utc_now() |> DateTime.truncate(:second)
    now = %{now | second: 0, microsecond: {0, 0}}
    two_minutes_ago = DateTime.add(now, -120, :second)

    :ok =
      Storage.put_state(%State{
        pipeline_module: SchedulerDailyPipeline,
        schedule_id: :scheduler_daily,
        schedule_fingerprint: fingerprint_for(SchedulerDailyPipeline),
        last_due_at: two_minutes_ago
      })

    :ok =
      Storage.put_state(%State{
        pipeline_module: SchedulerInactivePipeline,
        schedule_id: :scheduler_inactive,
        schedule_fingerprint: fingerprint_for(SchedulerInactivePipeline),
        last_due_at: two_minutes_ago
      })

    :ok = Scheduler.reload()
    :ok = Scheduler.tick()

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
      Storage.put_state(%State{
        pipeline_module: SchedulerDailyPipeline,
        schedule_id: :scheduler_daily,
        schedule_fingerprint: fingerprint_for(SchedulerDailyPipeline),
        last_due_at: DateTime.add(now, -120, :second)
      })

    :ok = Scheduler.reload()
    :ok = Scheduler.tick()

    assert {:ok, runs} = Favn.list_runs(limit: 1)
    [run | _] = runs

    assert run.submit_ref == SchedulerDailyPipeline
    assert run.pipeline.trigger.kind == :schedule
    assert run.pipeline.trigger.pipeline.module == SchedulerDailyPipeline
    assert run.pipeline.trigger.schedule.active == true
    assert is_binary(run.pipeline.trigger.occurrence.occurrence_key)
    assert %Favn.Window.Anchor{} = run.pipeline.anchor_window
  end

  test "scheduler resets persisted cursor when schedule fingerprint changes" do
    now = DateTime.utc_now() |> DateTime.truncate(:second)
    now = %{now | second: 0, microsecond: {0, 0}}

    :ok =
      Storage.put_state(%State{
        pipeline_module: SchedulerDailyPipeline,
        schedule_id: :scheduler_daily,
        schedule_fingerprint: "stale-fingerprint",
        last_due_at: DateTime.add(now, -3_600, :second)
      })

    :ok = Scheduler.reload()
    :ok = Scheduler.tick()

    assert {:ok, runs} = Favn.list_runs(limit: 20)

    scheduler_runs = Enum.filter(runs, &(&1.submit_ref == SchedulerDailyPipeline))
    assert length(scheduler_runs) <= 1

    case scheduler_runs do
      [run] ->
        due_at = run.pipeline.trigger.occurrence.due_at
        assert abs(DateTime.diff(now, due_at, :second)) < 120

      [] ->
        :ok
    end

    assert {:ok, %State{} = reloaded} = Storage.get_state(SchedulerDailyPipeline)
    assert reloaded.schedule_fingerprint != "stale-fingerprint"
    assert reloaded.last_due_at != nil
  end

  test "missed policies differ for skip, one, and all with overlap allow" do
    Application.put_env(:favn, :pipeline_modules, [
      SchedulerMissedSkipPipeline,
      SchedulerMissedOnePipeline,
      SchedulerMissedAllPipeline
    ])

    :ok = Scheduler.reload()

    now = DateTime.utc_now() |> DateTime.truncate(:second)
    now = %{now | second: 0, microsecond: {0, 0}}
    three_minutes_ago = DateTime.add(now, -180, :second)

    for {pipeline, id} <- [
          {SchedulerMissedSkipPipeline, :scheduler_missed_skip},
          {SchedulerMissedOnePipeline, :scheduler_missed_one},
          {SchedulerMissedAllPipeline, :scheduler_missed_all}
        ] do
      :ok =
        Storage.put_state(%State{
          pipeline_module: pipeline,
          schedule_id: id,
          schedule_fingerprint: fingerprint_for(pipeline),
          last_due_at: three_minutes_ago
        })
    end

    :ok = Scheduler.reload()
    :ok = Scheduler.tick()

    assert {:ok, runs} = Favn.list_runs(limit: 20)

    by_pipeline = Enum.group_by(runs, & &1.submit_ref)

    assert length(Map.get(by_pipeline, SchedulerMissedSkipPipeline, [])) == 1
    assert length(Map.get(by_pipeline, SchedulerMissedOnePipeline, [])) == 1
    assert length(Map.get(by_pipeline, SchedulerMissedAllPipeline, [])) >= 3

    skip_due =
      by_pipeline
      |> Map.fetch!(SchedulerMissedSkipPipeline)
      |> hd()
      |> Map.fetch!(:pipeline)
      |> Map.fetch!(:trigger)
      |> Map.fetch!(:occurrence)
      |> Map.fetch!(:due_at)

    one_due =
      by_pipeline
      |> Map.fetch!(SchedulerMissedOnePipeline)
      |> hd()
      |> Map.fetch!(:pipeline)
      |> Map.fetch!(:trigger)
      |> Map.fetch!(:occurrence)
      |> Map.fetch!(:due_at)

    assert DateTime.compare(one_due, skip_due) == :lt
  end

  test "scheduler does not advance cursor when run submission fails" do
    now = DateTime.utc_now() |> DateTime.truncate(:second)
    now = %{now | second: 0, microsecond: {0, 0}}
    previous_due = DateTime.add(now, -120, :second)

    :ok =
      Storage.put_state(%State{
        pipeline_module: SchedulerDailyPipeline,
        schedule_id: :scheduler_daily,
        schedule_fingerprint: fingerprint_for(SchedulerDailyPipeline),
        last_due_at: previous_due
      })

    :ok = Scheduler.reload()

    :ok = Favn.TestSetup.setup_asset_modules([], reload_graph?: true)

    capture_log(fn ->
      assert :ok = Scheduler.tick()
    end)

    assert {:ok, %State{} = reloaded} = Storage.get_state(SchedulerDailyPipeline)
    assert reloaded.last_due_at == previous_due
    assert reloaded.last_submitted_due_at == nil
  end

  defp fingerprint_for(pipeline_module) do
    Scheduler.list_scheduled_pipelines()
    |> Enum.find(&(&1.module == pipeline_module))
    |> Map.fetch!(:schedule_fingerprint)
  end
end
