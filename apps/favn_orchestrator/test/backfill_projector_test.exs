defmodule FavnOrchestrator.Backfill.ProjectorTest do
  use ExUnit.Case, async: false

  alias Favn.Run.AssetResult
  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.Storage.Adapter.Memory
  alias FavnOrchestrator.TransitionWriter

  setup do
    Memory.reset()

    on_exit(fn ->
      Memory.reset()
    end)

    :ok
  end

  test "projects successful child transitions into ledger, parent status, and asset window state" do
    now = DateTime.utc_now()
    parent = parent_run("run_backfill_success")
    child = child_run(parent.id, "run_child_success", "day:2026-04-27")

    assert :ok = Storage.put_run(parent)

    assert :ok =
             Storage.put_backfill_window(
               backfill_window(parent.id, child.trigger.window_key, now)
             )

    assert :ok = TransitionWriter.persist_transition(child, :run_created, %{status: :pending})

    assert {:ok, running_window} =
             Storage.get_backfill_window(
               parent.id,
               MyApp.Pipelines.Daily,
               child.trigger.window_key
             )

    assert running_window.status == :running
    assert running_window.child_run_id == child.id
    assert running_window.latest_attempt_run_id == child.id
    assert running_window.attempt_count == 1

    terminal =
      RunState.transition(child,
        status: :ok,
        result: %{
          status: :ok,
          asset_results: [
            %AssetResult{
              ref: {MyApp.Assets.Gold, :asset},
              stage: 0,
              status: :ok,
              started_at: now,
              finished_at: DateTime.add(now, 1, :second),
              duration_ms: 1,
              meta: %{rows_written: 42, relation: "gold.daily"},
              error: nil,
              attempt_count: 1,
              max_attempts: 1,
              attempts: []
            }
          ],
          metadata: %{}
        }
      )

    assert :ok = TransitionWriter.persist_transition(terminal, :run_finished, %{status: :ok})

    assert {:ok, completed_window} =
             Storage.get_backfill_window(
               parent.id,
               MyApp.Pipelines.Daily,
               child.trigger.window_key
             )

    assert completed_window.status == :ok
    assert completed_window.last_success_run_id == child.id
    assert completed_window.finished_at == terminal.updated_at
    assert completed_window.attempt_count == 1

    assert {:ok, completed_parent} = Storage.get_run(parent.id)
    assert completed_parent.status == :ok

    assert {:ok, parent_events} = Storage.list_run_events(parent.id)
    assert Enum.map(parent_events, & &1.event_type) == [:backfill_finished]

    assert {:ok, asset_window} =
             Storage.get_asset_window_state(
               MyApp.Assets.Gold,
               :asset,
               child.trigger.window_key
             )

    assert asset_window.status == :ok
    assert asset_window.latest_run_id == child.id
    assert asset_window.latest_parent_run_id == parent.id
    assert asset_window.latest_success_run_id == child.id
    assert asset_window.latest_error == nil
    assert asset_window.rows_written == 42
    assert asset_window.window_start_at == completed_window.window_start_at
    assert asset_window.window_end_at == completed_window.window_end_at
  end

  test "projects failed child terminal status into partial parent status" do
    now = DateTime.utc_now()
    parent = parent_run("run_backfill_partial")
    ok_child = child_run(parent.id, "run_child_ok", "day:2026-04-26")
    failed_child = child_run(parent.id, "run_child_failed", "day:2026-04-27")

    assert :ok = Storage.put_run(parent)

    assert :ok =
             Storage.put_backfill_window(
               backfill_window(parent.id, ok_child.trigger.window_key, now)
             )

    assert :ok =
             Storage.put_backfill_window(
               backfill_window(
                 parent.id,
                 failed_child.trigger.window_key,
                 DateTime.add(now, 1, :day)
               )
             )

    assert :ok = TransitionWriter.persist_transition(ok_child, :run_created, %{})

    assert :ok =
             ok_child
             |> RunState.transition(
               status: :ok,
               result: %{status: :ok, asset_results: [], metadata: %{}}
             )
             |> TransitionWriter.persist_transition(:run_finished, %{status: :ok})

    assert {:ok, running_parent} = Storage.get_run(parent.id)
    assert running_parent.status == :running

    assert :ok = TransitionWriter.persist_transition(failed_child, :run_created, %{})

    failure =
      RunState.transition(failed_child,
        status: :error,
        error: %{message: "boom"},
        result: %{status: :error, asset_results: [], metadata: %{}}
      )

    assert :ok = TransitionWriter.persist_transition(failure, :run_failed, %{status: :error})

    assert {:ok, failed_window} =
             Storage.get_backfill_window(
               parent.id,
               MyApp.Pipelines.Daily,
               failed_child.trigger.window_key
             )

    assert failed_window.status == :error
    assert failed_window.last_error == %{message: "boom"}

    assert {:ok, partial_parent} = Storage.get_run(parent.id)
    assert partial_parent.status == :partial

    assert {:ok, parent_events} = Storage.list_run_events(parent.id)
    assert Enum.map(parent_events, & &1.event_type) == [:backfill_partial]
  end

  defp parent_run(run_id) do
    RunState.new(
      id: run_id,
      manifest_version_id: "mv_backfill_projector",
      manifest_content_hash: "hash_backfill_projector",
      asset_ref: {MyApp.Assets.Gold, :asset},
      target_refs: [{MyApp.Assets.Gold, :asset}],
      trigger: %{kind: :backfill, pipeline_module: MyApp.Pipelines.Daily},
      metadata: %{pipeline_submit_ref: MyApp.Pipelines.Daily},
      submit_kind: :backfill_pipeline
    )
    |> Map.put(:status, :running)
    |> RunState.with_snapshot_hash()
  end

  defp child_run(parent_run_id, run_id, window_key) do
    RunState.new(
      id: run_id,
      manifest_version_id: "mv_backfill_projector",
      manifest_content_hash: "hash_backfill_projector",
      asset_ref: {MyApp.Assets.Gold, :asset},
      target_refs: [{MyApp.Assets.Gold, :asset}],
      trigger: %{kind: :backfill, backfill_run_id: parent_run_id, window_key: window_key},
      metadata: %{pipeline_submit_ref: MyApp.Pipelines.Daily},
      submit_kind: :pipeline,
      parent_run_id: parent_run_id,
      root_run_id: parent_run_id,
      lineage_depth: 1
    )
  end

  defp backfill_window(backfill_run_id, window_key, start_at) do
    {:ok, window} =
      BackfillWindow.new(%{
        backfill_run_id: backfill_run_id,
        pipeline_module: MyApp.Pipelines.Daily,
        manifest_version_id: "mv_backfill_projector",
        window_kind: :day,
        window_start_at: start_at,
        window_end_at: DateTime.add(start_at, 1, :day),
        timezone: "Etc/UTC",
        window_key: window_key,
        status: :pending,
        created_at: start_at,
        updated_at: start_at
      })

    window
  end
end
