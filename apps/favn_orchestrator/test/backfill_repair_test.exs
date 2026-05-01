defmodule FavnOrchestrator.Backfill.RepairTest do
  use ExUnit.Case, async: false

  alias Favn.Run.AssetResult
  alias Favn.Window.Anchor
  alias Favn.Window.Key, as: WindowKey
  alias FavnOrchestrator.Backfill.AssetWindowState
  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.Storage.Adapter.Memory

  setup do
    Memory.reset()

    on_exit(fn ->
      Memory.reset()
    end)

    :ok
  end

  test "apply rebuilds deleted backfill read models from run snapshots" do
    anchor = anchor(~U[2026-04-27 00:00:00Z])
    window_key = WindowKey.encode(anchor.key)
    parent = parent_run("backfill_deleted")
    child = child_run(parent.id, "child_deleted", anchor, :ok)

    assert :ok = Storage.put_run(parent)
    assert :ok = Storage.put_run(child)
    assert {:ok, before_events} = Storage.list_run_events(parent.id)

    assert {:ok, report} = FavnOrchestrator.repair_backfill_projections(apply: true)

    assert report.counts.backfill_windows == 1
    assert report.counts.asset_window_states == 1

    assert {:ok, window} =
             Storage.get_backfill_window(parent.id, MyApp.Pipelines.Daily, window_key)

    assert window.status == :ok
    assert window.child_run_id == child.id
    assert window.last_success_run_id == child.id

    assert {:ok, state} =
             Storage.get_asset_window_state(MyApp.Assets.Gold, :asset, window_key)

    assert state.status == :ok
    assert state.latest_run_id == child.id
    assert state.rows_written == 42

    assert {:ok, ^parent} = Storage.get_run(parent.id)
    assert {:ok, ^before_events} = Storage.list_run_events(parent.id)
  end

  test "pipeline scoped apply replaces drifted projection rows" do
    anchor = anchor(~U[2026-04-28 00:00:00Z])
    window_key = WindowKey.encode(anchor.key)
    parent = parent_run("backfill_drifted")
    child = child_run(parent.id, "child_drifted", anchor, :ok)

    assert :ok = Storage.put_run(parent)
    assert :ok = Storage.put_run(child)
    assert :ok = Storage.put_backfill_window(drifted_window(parent.id, anchor))
    assert :ok = Storage.put_asset_window_state(drifted_asset_state(parent.id, anchor))

    assert {:ok, report} =
             FavnOrchestrator.repair_backfill_projections(
               apply: true,
               pipeline_module: MyApp.Pipelines.Daily
             )

    assert report.counts.backfill_windows == 1
    assert report.counts.asset_window_states == 1

    assert {:ok, repaired_window} =
             Storage.get_backfill_window(parent.id, MyApp.Pipelines.Daily, window_key)

    assert repaired_window.status == :ok
    assert repaired_window.latest_attempt_run_id == child.id
    assert repaired_window.last_error == nil

    assert {:ok, repaired_state} =
             Storage.get_asset_window_state(MyApp.Assets.Gold, :asset, window_key)

    assert repaired_state.status == :ok
    assert repaired_state.latest_run_id == child.id
    assert repaired_state.latest_error == nil
  end

  test "repair folds failed attempts and successful reruns for the same window" do
    anchor = anchor(~U[2026-04-29 00:00:00Z])
    window_key = WindowKey.encode(anchor.key)
    parent = parent_run("backfill_rerun")

    failed =
      child_run(parent.id, "child_failed_attempt", anchor, :error,
        updated_at: ~U[2026-04-29 01:00:00Z]
      )

    rerun =
      child_run(parent.id, "child_successful_rerun", anchor, :ok,
        updated_at: ~U[2026-04-29 02:00:00Z]
      )

    assert :ok = Storage.put_run(parent)
    assert :ok = Storage.put_run(rerun)
    assert :ok = Storage.put_run(failed)

    assert {:ok, report} = FavnOrchestrator.repair_backfill_projections(apply: true)

    assert report.counts.backfill_windows == 1
    assert report.counts.asset_window_states == 1

    assert {:ok, window} =
             Storage.get_backfill_window(parent.id, MyApp.Pipelines.Daily, window_key)

    assert window.child_run_id == failed.id
    assert window.status == :ok
    assert window.attempt_count == 2
    assert window.latest_attempt_run_id == rerun.id
    assert window.last_success_run_id == rerun.id
    assert window.last_error == nil
    assert window.errors == [%{message: "failed"}]

    assert {:ok, state} =
             Storage.get_asset_window_state(MyApp.Assets.Gold, :asset, window_key)

    assert state.status == :ok
    assert state.latest_run_id == rerun.id
    assert state.latest_success_run_id == rerun.id
    assert state.latest_error == nil
    assert state.errors == [%{message: "asset failed"}]
  end

  test "dry run reports missing metadata without writing read models" do
    parent = parent_run("backfill_missing_metadata")
    child = child_run(parent.id, "child_missing_metadata", nil, :ok)

    assert :ok = Storage.put_run(parent)
    assert :ok = Storage.put_run(child)

    assert {:ok, report} = FavnOrchestrator.repair_backfill_projections()

    assert report.apply == false
    assert report.counts.backfill_windows == 0
    assert [%{model: :backfill_window, reason: :missing_anchor_window_metadata}] = report.skips

    assert {:error, :not_found} =
             Storage.get_backfill_window(parent.id, MyApp.Pipelines.Daily, "day:2026-04-27")
  end

  defp parent_run(run_id) do
    RunState.new(
      id: run_id,
      manifest_version_id: "mv_backfill_repair",
      manifest_content_hash: "hash_backfill_repair",
      asset_ref: {MyApp.Assets.Gold, :asset},
      target_refs: [{MyApp.Assets.Gold, :asset}],
      trigger: %{kind: :backfill, pipeline_module: MyApp.Pipelines.Daily},
      metadata: %{pipeline_submit_ref: MyApp.Pipelines.Daily},
      submit_kind: :backfill_pipeline
    )
    |> Map.put(:status, :running)
    |> RunState.with_snapshot_hash()
  end

  defp child_run(parent_run_id, run_id, anchor, status, opts \\ []) do
    window_key = if anchor, do: WindowKey.encode(anchor.key), else: "day:2026-04-27"

    RunState.new(
      id: run_id,
      manifest_version_id: "mv_backfill_repair",
      manifest_content_hash: "hash_backfill_repair",
      asset_ref: {MyApp.Assets.Gold, :asset},
      target_refs: [{MyApp.Assets.Gold, :asset}],
      trigger: %{kind: :backfill, backfill_run_id: parent_run_id, window_key: window_key},
      metadata: child_metadata(anchor),
      submit_kind: :pipeline,
      parent_run_id: parent_run_id,
      root_run_id: parent_run_id,
      lineage_depth: 1
    )
    |> RunState.transition(status: status, result: result(status), error: error(status))
    |> maybe_put_updated_at(Keyword.get(opts, :updated_at))
  end

  defp child_metadata(nil), do: %{pipeline_submit_ref: MyApp.Pipelines.Daily}

  defp child_metadata(anchor) do
    %{
      pipeline_submit_ref: MyApp.Pipelines.Daily,
      pipeline_context: %{anchor_window: anchor}
    }
  end

  defp result(:ok) do
    %{
      status: :ok,
      asset_results: [
        %AssetResult{
          ref: {MyApp.Assets.Gold, :asset},
          stage: 0,
          status: :ok,
          meta: %{rows_written: 42},
          attempt_count: 1,
          max_attempts: 1,
          attempts: []
        }
      ],
      metadata: %{}
    }
  end

  defp result(:error) do
    %{
      status: :error,
      asset_results: [
        %AssetResult{
          ref: {MyApp.Assets.Gold, :asset},
          stage: 0,
          status: :error,
          meta: %{},
          error: %{message: "asset failed"},
          attempt_count: 1,
          max_attempts: 1,
          attempts: []
        }
      ],
      metadata: %{}
    }
  end

  defp error(:ok), do: nil

  defp error(_status), do: %{message: "failed"}

  defp maybe_put_updated_at(%RunState{} = run, nil), do: run

  defp maybe_put_updated_at(%RunState{} = run, %DateTime{} = updated_at) do
    run
    |> Map.put(:updated_at, updated_at)
    |> RunState.with_snapshot_hash()
  end

  defp anchor(start_at) do
    {:ok, anchor} = Anchor.new(:day, start_at, DateTime.add(start_at, 1, :day))
    anchor
  end

  defp drifted_window(parent_run_id, anchor) do
    {:ok, window} =
      BackfillWindow.new(%{
        backfill_run_id: parent_run_id,
        pipeline_module: MyApp.Pipelines.Daily,
        manifest_version_id: "mv_backfill_repair",
        window_kind: anchor.kind,
        window_start_at: anchor.start_at,
        window_end_at: anchor.end_at,
        timezone: anchor.timezone,
        window_key: WindowKey.encode(anchor.key),
        status: :error,
        latest_attempt_run_id: "stale_child",
        last_error: %{message: "stale"},
        updated_at: anchor.start_at
      })

    window
  end

  defp drifted_asset_state(parent_run_id, anchor) do
    {:ok, state} =
      AssetWindowState.new(%{
        asset_ref_module: MyApp.Assets.Gold,
        asset_ref_name: :asset,
        pipeline_module: MyApp.Pipelines.Daily,
        manifest_version_id: "mv_backfill_repair",
        window_kind: anchor.kind,
        window_start_at: anchor.start_at,
        window_end_at: anchor.end_at,
        timezone: anchor.timezone,
        window_key: WindowKey.encode(anchor.key),
        status: :error,
        latest_run_id: "stale_child",
        latest_parent_run_id: parent_run_id,
        latest_error: %{message: "stale"},
        updated_at: anchor.start_at
      })

    state
  end
end
