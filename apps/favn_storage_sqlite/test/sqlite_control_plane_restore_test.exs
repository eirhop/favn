defmodule FavnStorageSqlite.ControlPlaneRestoreTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL
  alias Favn.Manifest
  alias Favn.Manifest.Version
  alias Favn.Storage.Adapter.SQLite, as: Adapter
  alias FavnOrchestrator.Backfill.AssetWindowState
  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.Backfill.CoverageBaseline
  alias FavnOrchestrator.Projector
  alias FavnOrchestrator.RunState
  alias FavnStorageSqlite.Repo
  alias FavnStorageSqlite.Supervisor, as: SQLiteSupervisor

  setup do
    maybe_stop_process(Repo)

    unique = System.unique_integer([:positive, :monotonic])
    source_path = temp_path(unique, "source.db")
    restored_path = temp_path(unique, "restored.db")

    on_exit(fn ->
      maybe_stop_process(Repo)
      rm_sqlite_files(source_path)
      rm_sqlite_files(restored_path)
    end)

    {:ok, source_path: source_path, restored_path: restored_path, unique: unique}
  end

  test "restores stopped-backend control-plane state from copied SQLite database", %{
    source_path: source_path,
    restored_path: restored_path,
    unique: unique
  } do
    source_opts = adapter_opts(source_path, unique, :source, migration_mode: :auto)
    restored_opts = adapter_opts(restored_path, unique, :restored, migration_mode: :manual)

    {:ok, source_pid} = SQLiteSupervisor.start_link(source_opts)

    version = manifest_version("mv_restore_#{unique}")
    run = run_state("run_restore_#{unique}", version)
    event = run_event()
    scheduler_key = {MyApp.RestorePipeline, :daily}
    scheduler_state = %{last_due_at: timestamp(), version: 1}
    baseline = coverage_baseline("baseline_restore_#{unique}", run, timestamp())
    window = backfill_window("backfill_restore_#{unique}", baseline, timestamp())
    asset_state = asset_window_state(:restore_asset, window, timestamp())

    assert :ok = Adapter.put_manifest_version(version, source_opts)
    assert :ok = Adapter.set_active_manifest_version(version.manifest_version_id, source_opts)
    assert :ok = Adapter.put_run(run, source_opts)
    assert :ok = Adapter.append_run_event(run.id, event, source_opts)
    assert :ok = Adapter.put_scheduler_state(scheduler_key, scheduler_state, source_opts)
    assert :ok = Adapter.put_coverage_baseline(baseline, source_opts)
    assert :ok = Adapter.put_backfill_window(window, source_opts)
    assert :ok = Adapter.put_asset_window_state(asset_state, source_opts)

    assert {:ok, _} = SQL.query(Repo, "PRAGMA wal_checkpoint(TRUNCATE)", [])

    maybe_stop_pid(source_pid)
    File.cp!(source_path, restored_path)

    {:ok, restored_pid} = SQLiteSupervisor.start_link(restored_opts)

    assert {:ok, stored_version} =
             Adapter.get_manifest_version(version.manifest_version_id, restored_opts)

    assert stored_version.content_hash == version.content_hash

    assert {:ok, version.manifest_version_id} ==
             Adapter.get_active_manifest_version(restored_opts)

    assert {:ok, stored_run} = Adapter.get_run(run.id, restored_opts)
    assert stored_run.id == run.id
    assert stored_run.manifest_version_id == version.manifest_version_id
    assert stored_run.metadata.replay_submit_kind == :pipeline
    assert stored_run.metadata.replay_mode == :exact_replay
    assert stored_run.metadata.in_flight_execution_ids == ["exec_1", "exec_2"]

    projected = Projector.project_run(stored_run)
    assert projected.submit_ref == MyApp.RestorePipeline
    assert projected.replay_mode == :exact_replay
    assert projected.pipeline.resolved_refs == [{MyApp.RestoreAsset, :restore_asset}]

    assert {:ok, [stored_event]} = Adapter.list_run_events(run.id, restored_opts)
    assert stored_event.run_id == run.id
    assert stored_event.sequence == event.sequence
    assert stored_event.event_type == event.event_type

    assert {:ok, stored_scheduler_state} =
             Adapter.get_scheduler_state(scheduler_key, restored_opts)

    assert stored_scheduler_state.pipeline_module == MyApp.RestorePipeline
    assert stored_scheduler_state.schedule_id == :daily
    assert stored_scheduler_state.version == scheduler_state.version
    assert %DateTime{} = stored_scheduler_state.last_due_at
    assert stored_scheduler_state.last_due_at == scheduler_state.last_due_at

    assert {:ok, ^baseline} = Adapter.get_coverage_baseline(baseline.baseline_id, restored_opts)

    assert {:ok, ^window} =
             Adapter.get_backfill_window(
               window.backfill_run_id,
               window.pipeline_module,
               window.window_key,
               restored_opts
             )

    assert {:ok, ^asset_state} =
             Adapter.get_asset_window_state(
               asset_state.asset_ref_module,
               asset_state.asset_ref_name,
               asset_state.window_key,
               restored_opts
             )

    new_run = run_state("run_after_restore_#{unique}", version)
    assert :ok = Adapter.put_run(new_run, restored_opts)
    assert {:ok, stored_new_run} = Adapter.get_run(new_run.id, restored_opts)
    assert stored_new_run.id == new_run.id

    maybe_stop_pid(restored_pid)
  end

  defp adapter_opts(path, unique, role, extra_opts) do
    [
      database: path,
      name: Module.concat([__MODULE__, "Supervisor#{unique}#{role}"]),
      supervisor_name: Module.concat([__MODULE__, "Supervisor#{unique}#{role}"]),
      pool_size: 1
    ]
    |> Keyword.merge(extra_opts)
  end

  defp manifest_version(manifest_version_id) do
    manifest = %Manifest{
      assets: [
        %Manifest.Asset{
          ref: {MyApp.RestoreAsset, :restore_asset},
          module: MyApp.RestoreAsset,
          name: :restore_asset
        }
      ],
      pipelines: [
        %Manifest.Pipeline{
          module: MyApp.RestorePipeline,
          name: :daily,
          selectors: []
        }
      ]
    }

    {:ok, version} = Version.new(manifest, manifest_version_id: manifest_version_id)
    version
  end

  defp run_state(id, version) do
    RunState.new(
      id: id,
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      asset_ref: {MyApp.RestoreAsset, :restore_asset},
      metadata: %{
        in_flight_execution_ids: ["exec_1", "exec_2"],
        replay_submit_kind: :pipeline,
        replay_mode: :exact_replay,
        pipeline_submit_ref: MyApp.RestorePipeline,
        pipeline_target_refs: [{MyApp.RestoreAsset, :restore_asset}],
        pipeline_context: %{
          id: "daily",
          name: "daily",
          run_kind: :pipeline,
          resolved_refs: [{MyApp.RestoreAsset, :restore_asset}],
          deps: :all
        }
      },
      submit_kind: :rerun
    )
  end

  defp run_event do
    %{
      sequence: 1,
      event_type: :run_started,
      occurred_at: timestamp(),
      data: %{reason: "restore verification"}
    }
  end

  defp coverage_baseline(baseline_id, run, now) do
    start_at = DateTime.add(now, -86_400, :second)

    {:ok, baseline} =
      CoverageBaseline.new(%{
        baseline_id: baseline_id,
        pipeline_module: MyApp.RestorePipeline,
        source_key: "orders",
        segment_key_hash: "sha256:#{baseline_id}",
        window_kind: :day,
        timezone: "Etc/UTC",
        coverage_start_at: start_at,
        coverage_until: now,
        created_by_run_id: run.id,
        manifest_version_id: run.manifest_version_id,
        status: :ok,
        created_at: now,
        updated_at: now
      })

    baseline
  end

  defp backfill_window(backfill_run_id, baseline, now) do
    start_at = DateTime.add(now, -3_600, :second)
    window_key = "day:#{Date.to_iso8601(DateTime.to_date(start_at))}"

    {:ok, window} =
      BackfillWindow.new(%{
        backfill_run_id: backfill_run_id,
        child_run_id: "child_#{backfill_run_id}",
        pipeline_module: baseline.pipeline_module,
        manifest_version_id: baseline.manifest_version_id,
        coverage_baseline_id: baseline.baseline_id,
        window_kind: :day,
        window_start_at: start_at,
        window_end_at: now,
        timezone: "Etc/UTC",
        window_key: window_key,
        status: :ok,
        attempt_count: 1,
        latest_attempt_run_id: "child_#{backfill_run_id}",
        last_success_run_id: "child_#{backfill_run_id}",
        created_at: start_at,
        updated_at: now
      })

    window
  end

  defp asset_window_state(asset_ref_name, window, now) do
    {:ok, state} =
      AssetWindowState.new(%{
        asset_ref_module: MyApp.RestoreAsset,
        asset_ref_name: asset_ref_name,
        pipeline_module: window.pipeline_module,
        manifest_version_id: window.manifest_version_id,
        window_kind: window.window_kind,
        window_start_at: window.window_start_at,
        window_end_at: window.window_end_at,
        timezone: window.timezone,
        window_key: window.window_key,
        status: :ok,
        latest_run_id: window.latest_attempt_run_id,
        latest_parent_run_id: window.backfill_run_id,
        latest_success_run_id: window.last_success_run_id,
        updated_at: now
      })

    state
  end

  defp timestamp, do: DateTime.utc_now() |> DateTime.truncate(:microsecond)

  defp temp_path(unique, name) do
    Path.join(System.tmp_dir!(), "favn_sqlite_restore_#{unique}_#{name}")
  end

  defp maybe_stop_process(name) do
    case Process.whereis(name) do
      nil -> :ok
      pid -> maybe_stop_pid(pid)
    end
  end

  defp maybe_stop_pid(nil), do: :ok

  defp maybe_stop_pid(pid) when is_pid(pid) do
    if Process.alive?(pid) do
      Process.unlink(pid)
      Supervisor.stop(pid, :normal, 5_000)
    else
      :ok
    end
  catch
    :exit, _reason -> :ok
  end

  defp rm_sqlite_files(path) do
    File.rm(path)
    File.rm("#{path}-shm")
    File.rm("#{path}-wal")
  end
end
