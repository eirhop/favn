defmodule FavnStorageSqlite.AdapterTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Version
  alias Favn.Storage.Adapter.SQLite, as: Adapter
  alias FavnOrchestrator.Backfill.AssetWindowState
  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.Backfill.CoverageBaseline
  alias FavnOrchestrator.RunState
  alias FavnStorageSqlite.Repo

  alias Ecto.Adapters.SQL

  setup context do
    unique = System.unique_integer([:positive])
    db_path = Path.join(System.tmp_dir!(), "favn_storage_sqlite_#{unique}.db")
    supervisor_name = Module.concat([__MODULE__, "Supervisor#{unique}"])

    opts = [
      database: db_path,
      supervisor_name: supervisor_name,
      migration_mode: :auto,
      pool_size: 4
    ]

    supervisor_pid =
      if context[:without_started_adapter] != true do
        maybe_stop_process(FavnStorageSqlite.Repo)

        {:ok, pid} = FavnStorageSqlite.Supervisor.start_link(opts)
        pid
      else
        nil
      end

    on_exit(fn ->
      maybe_stop_pid(supervisor_pid)
      File.rm(db_path)
    end)

    {:ok, opts: opts}
  end

  test "stores manifest versions and supports activation", %{opts: opts} do
    version = manifest_version("mv_sqlite")
    duplicate = manifest_version("mv_sqlite_duplicate")

    assert :ok = Adapter.put_manifest_version(version, opts)
    assert :ok = Adapter.put_manifest_version(version, opts)
    assert :ok = Adapter.put_manifest_version(duplicate, opts)

    assert {:ok, stored} = Adapter.get_manifest_version("mv_sqlite", opts)
    assert stored.content_hash == version.content_hash
    assert %Manifest{} = stored.manifest
    assert [%Asset{ref: {MyApp.Asset, :asset}}] = stored.manifest.assets

    assert {:ok, canonical} =
             Adapter.get_manifest_version_by_content_hash(version.content_hash, opts)

    assert canonical.manifest_version_id == "mv_sqlite"

    assert {:error, :manifest_version_not_found} =
             Adapter.get_manifest_version("mv_sqlite_duplicate", opts)

    assert {:ok, listed} = Adapter.list_manifest_versions(opts)
    assert [%Version{manifest: %Manifest{assets: [%Asset{ref: {MyApp.Asset, :asset}}]}}] = listed

    assert :ok = Adapter.set_active_manifest_version("mv_sqlite", opts)
    assert {:ok, "mv_sqlite"} = Adapter.get_active_manifest_version(opts)
  end

  test "enforces run snapshot write semantics", %{opts: opts} do
    version = manifest_version("mv_sqlite")

    base =
      RunState.new(
        id: "run_sqlite_1",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: {MyApp.Asset, :asset}
      )

    assert :ok = Adapter.put_manifest_version(version, opts)
    assert :ok = Adapter.put_run(base, opts)
    assert :ok = Adapter.put_run(base, opts)

    stale = %{base | event_seq: 0} |> RunState.with_snapshot_hash()
    assert {:error, :stale_write} = Adapter.put_run(stale, opts)

    conflict = %{base | status: :error} |> RunState.with_snapshot_hash()
    assert {:error, :conflicting_snapshot} = Adapter.put_run(conflict, opts)

    assert {:ok, stored} = Adapter.get_run(base.id, opts)
    assert stored.status == :pending
  end

  test "persists run snapshots as JSON-safe DTO records", %{opts: opts} do
    version = manifest_version("mv_sqlite_dto")

    run =
      RunState.new(
        id: "run_sqlite_dto",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: {MyApp.Asset, :asset},
        metadata: %{password: "secret"}
      )

    assert :ok = Adapter.put_manifest_version(version, opts)
    assert :ok = Adapter.put_run(run, opts)

    assert {:ok, %{rows: [[payload]]}} =
             SQL.query(Repo, "SELECT run_blob FROM favn_runs WHERE run_id = ?1", [run.id])

    assert Jason.decode!(payload)["format"] == "favn.run_snapshot.storage.v1"
    refute payload =~ "__type__"
    refute payload =~ "__struct__"
    refute payload =~ "secret"
  end

  test "stores run events and scheduler cursor state", %{opts: opts} do
    event = %{
      sequence: 1,
      event_type: :run_started,
      occurred_at: DateTime.utc_now(),
      data: %{a: 1}
    }

    assert :ok = Adapter.append_run_event("run_sqlite_events", event, opts)
    assert :ok = Adapter.append_run_event("run_sqlite_events", event, opts)

    assert {:error, :conflicting_event_sequence} =
             Adapter.append_run_event(
               "run_sqlite_events",
               %{sequence: 1, event_type: :run_updated},
               opts
             )

    assert {:ok, [stored_event]} = Adapter.list_run_events("run_sqlite_events", opts)
    assert stored_event.sequence == 1
    assert stored_event.run_id == "run_sqlite_events"
    assert is_integer(stored_event.global_sequence)

    assert {:ok, [global_event]} =
             Adapter.list_global_run_events([after_global_sequence: nil, limit: 10], opts)

    assert global_event.run_id == "run_sqlite_events"

    assert {:ok, %{rows: [[event_payload]]}} =
             SQL.query(Repo, "SELECT event_blob FROM favn_run_events WHERE run_id = ?1", [
               "run_sqlite_events"
             ])

    assert Jason.decode!(event_payload)["format"] == "favn.run_event.storage.v1"
    refute event_payload =~ "__type__"
    refute event_payload =~ "__struct__"

    assert {:ok, []} =
             Adapter.list_global_run_events(
               [after_global_sequence: global_event.global_sequence, limit: 10],
               opts
             )

    assert {:error, :cursor_invalid} =
             Adapter.list_global_run_events([after_global_sequence: 999_999, limit: 10], opts)

    key = {MyApp.Pipeline, :daily}

    assert :ok =
             Adapter.put_scheduler_state(
               key,
               %{last_due_at: DateTime.utc_now(), version: 1},
               opts
             )

    assert {:error, :stale_scheduler_state} =
             Adapter.put_scheduler_state(key, %{version: 1}, opts)

    assert :ok =
             Adapter.put_scheduler_state(
               key,
               %{version: 2, last_due_at: DateTime.utc_now()},
               opts
             )

    assert {:ok, %{rows: [[state_payload]]}} =
             SQL.query(
               Repo,
               "SELECT state_blob FROM favn_scheduler_cursors WHERE schedule_id = ?1",
               [
                 "daily"
               ]
             )

    state_dto = Jason.decode!(state_payload)
    assert state_dto["format"] == "favn.scheduler_state.storage"
    assert state_dto["schema_version"] == 1
    assert is_map(state_dto["state"])
    assert is_binary(state_dto["state"]["last_due_at"])
    refute Map.has_key?(state_dto["state"], "pipeline_module")
    refute Map.has_key?(state_dto["state"], "schedule_id")
    refute Map.has_key?(state_dto["state"], "version")
    refute state_payload =~ "__type__"
    refute state_payload =~ "__struct__"

    assert {:ok, %Favn.Scheduler.State{schedule_id: :daily}} =
             Adapter.get_scheduler_state(key, opts)
  end

  test "read-model list APIs return invalid pagination errors", %{opts: opts} do
    filters = [limit: 0]

    assert {:error, :invalid_pagination} = Adapter.list_coverage_baselines(filters, opts)
    assert {:error, :invalid_pagination} = Adapter.list_backfill_windows(filters, opts)
    assert {:error, :invalid_pagination} = Adapter.list_asset_window_states(filters, opts)
  end

  test "replaces scoped backfill read models atomically", %{opts: opts} do
    unique = System.unique_integer([:positive])
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    start_at = DateTime.add(now, -86_400, :second)

    stale_baseline =
      coverage_baseline("baseline_sqlite_stale_#{unique}", MyApp.Pipeline, now, start_at)

    kept_baseline =
      coverage_baseline("baseline_sqlite_kept_#{unique}", MyApp.OtherPipeline, now, start_at)

    replacement_baseline =
      coverage_baseline("baseline_sqlite_new_#{unique}", MyApp.Pipeline, now, start_at)

    stale_window =
      backfill_window(
        "backfill_sqlite_stale_#{unique}",
        MyApp.Pipeline,
        "day:stale:#{unique}",
        stale_baseline,
        now,
        start_at
      )

    kept_window =
      backfill_window(
        "backfill_sqlite_kept_#{unique}",
        MyApp.OtherPipeline,
        "day:kept:#{unique}",
        kept_baseline,
        now,
        start_at
      )

    replacement_window =
      backfill_window(
        "backfill_sqlite_new_#{unique}",
        MyApp.Pipeline,
        "day:new:#{unique}",
        replacement_baseline,
        now,
        start_at
      )

    stale_state = asset_window_state(:stale_asset, MyApp.Pipeline, stale_window, now)
    kept_state = asset_window_state(:kept_asset, MyApp.OtherPipeline, kept_window, now)
    replacement_state = asset_window_state(:new_asset, MyApp.Pipeline, replacement_window, now)

    for item <- [stale_baseline, kept_baseline],
        do: assert(:ok = Adapter.put_coverage_baseline(item, opts))

    for item <- [stale_window, kept_window],
        do: assert(:ok = Adapter.put_backfill_window(item, opts))

    for item <- [stale_state, kept_state],
        do: assert(:ok = Adapter.put_asset_window_state(item, opts))

    assert :ok =
             Adapter.replace_backfill_read_models(
               [pipeline_module: MyApp.Pipeline],
               [replacement_baseline],
               [replacement_window],
               [replacement_state],
               opts
             )

    assert {:error, :not_found} = Adapter.get_coverage_baseline(stale_baseline.baseline_id, opts)
    assert {:ok, ^kept_baseline} = Adapter.get_coverage_baseline(kept_baseline.baseline_id, opts)

    assert {:ok, ^replacement_baseline} =
             Adapter.get_coverage_baseline(replacement_baseline.baseline_id, opts)

    assert {:error, :not_found} =
             Adapter.get_backfill_window(
               stale_window.backfill_run_id,
               stale_window.pipeline_module,
               stale_window.window_key,
               opts
             )

    assert {:ok, ^kept_window} =
             Adapter.get_backfill_window(
               kept_window.backfill_run_id,
               kept_window.pipeline_module,
               kept_window.window_key,
               opts
             )

    assert {:ok, ^replacement_window} =
             Adapter.get_backfill_window(
               replacement_window.backfill_run_id,
               replacement_window.pipeline_module,
               replacement_window.window_key,
               opts
             )

    assert {:error, :not_found} =
             Adapter.get_asset_window_state(
               stale_state.asset_ref_module,
               stale_state.asset_ref_name,
               stale_state.window_key,
               opts
             )

    assert {:ok, ^kept_state} =
             Adapter.get_asset_window_state(
               kept_state.asset_ref_module,
               kept_state.asset_ref_name,
               kept_state.window_key,
               opts
             )

    assert {:ok, ^replacement_state} =
             Adapter.get_asset_window_state(
               replacement_state.asset_ref_module,
               replacement_state.asset_ref_name,
               replacement_state.window_key,
               opts
             )
  end

  @tag without_started_adapter: true
  test "rejects manual mode startup when schema is missing" do
    unique = System.unique_integer([:positive])
    db_path = Path.join(System.tmp_dir!(), "favn_storage_sqlite_manual_#{unique}.db")

    opts = [
      database: db_path,
      supervisor_name: Module.concat([__MODULE__, "ManualSupervisor#{unique}"]),
      migration_mode: :manual
    ]

    parent = self()

    spawn(fn ->
      Process.flag(:trap_exit, true)

      result =
        try do
          FavnStorageSqlite.Supervisor.start_link(opts)
        catch
          :exit, reason -> {:exit, reason}
        end

      send(parent, {:manual_start_result, result})
    end)

    assert_receive {:manual_start_result, {:error, {%RuntimeError{message: message}, _}}}

    assert message =~ "favn sqlite schema is not ready"

    File.rm(db_path)
  end

  test "enforces run write conflicts under concurrent updates", %{opts: opts} do
    base = run_state("run_sqlite_concurrent", "mv_sqlite", "hash_sqlite")
    assert :ok = Adapter.put_run(base, opts)

    running = %{base | event_seq: 2, status: :running} |> RunState.with_snapshot_hash()
    failed = %{base | event_seq: 2, status: :error} |> RunState.with_snapshot_hash()

    results =
      concurrent_results(fn -> Adapter.put_run(running, opts) end, fn ->
        Adapter.put_run(failed, opts)
      end)

    assert Enum.sort(results) == [:ok, {:error, :conflicting_snapshot}]
  end

  test "enforces scheduler version checks under concurrent updates", %{opts: opts} do
    key = {MyApp.Pipeline, :daily}
    assert :ok = Adapter.put_scheduler_state(key, %{version: 1}, opts)

    results =
      concurrent_results(
        fn ->
          Adapter.put_scheduler_state(key, %{version: 2, last_due_at: DateTime.utc_now()}, opts)
        end,
        fn ->
          Adapter.put_scheduler_state(key, %{version: 2, last_due_at: DateTime.utc_now()}, opts)
        end
      )

    assert Enum.sort(results) == [:ok, {:error, :stale_scheduler_state}]
  end

  defp manifest_version(manifest_version_id) do
    manifest = %Manifest{
      assets: [
        %Favn.Manifest.Asset{ref: {MyApp.Asset, :asset}, module: MyApp.Asset, name: :asset}
      ]
    }

    {:ok, version} = Version.new(manifest, manifest_version_id: manifest_version_id)
    version
  end

  defp run_state(id, manifest_version_id, manifest_content_hash) do
    RunState.new(
      id: id,
      manifest_version_id: manifest_version_id,
      manifest_content_hash: manifest_content_hash,
      asset_ref: {MyApp.Asset, :asset}
    )
  end

  defp coverage_baseline(baseline_id, pipeline_module, now, start_at) do
    {:ok, baseline} =
      CoverageBaseline.new(%{
        baseline_id: baseline_id,
        pipeline_module: pipeline_module,
        source_key: "orders",
        segment_key_hash: "sha256:#{baseline_id}",
        window_kind: :day,
        timezone: "Etc/UTC",
        coverage_start_at: start_at,
        coverage_until: now,
        created_by_run_id: "run_#{baseline_id}",
        manifest_version_id: "mv_#{baseline_id}",
        status: :ok,
        created_at: now,
        updated_at: now
      })

    baseline
  end

  defp backfill_window(backfill_run_id, pipeline_module, window_key, baseline, now, start_at) do
    {:ok, window} =
      BackfillWindow.new(%{
        backfill_run_id: backfill_run_id,
        child_run_id: "child_#{window_key}",
        pipeline_module: pipeline_module,
        manifest_version_id: baseline.manifest_version_id,
        coverage_baseline_id: baseline.baseline_id,
        window_kind: :day,
        window_start_at: start_at,
        window_end_at: now,
        timezone: "Etc/UTC",
        window_key: window_key,
        status: :ok,
        attempt_count: 1,
        latest_attempt_run_id: "child_#{window_key}",
        last_success_run_id: "child_#{window_key}",
        created_at: start_at,
        updated_at: now
      })

    window
  end

  defp asset_window_state(asset_ref_name, pipeline_module, window, now) do
    {:ok, state} =
      AssetWindowState.new(%{
        asset_ref_module: MyApp.Asset,
        asset_ref_name: asset_ref_name,
        pipeline_module: pipeline_module,
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

  defp concurrent_results(fun_a, fun_b) do
    parent = self()

    task_a = Task.async(fn -> await_release(parent, :task_a, fun_a) end)
    task_b = Task.async(fn -> await_release(parent, :task_b, fun_b) end)

    assert_receive {:ready, :task_a}
    assert_receive {:ready, :task_b}

    send(task_a.pid, :go)
    send(task_b.pid, :go)

    [Task.await(task_a, 5_000), Task.await(task_b, 5_000)]
  end

  defp await_release(parent, label, fun) do
    send(parent, {:ready, label})

    receive do
      :go -> fun.()
    end
  end

  defp maybe_stop_process(name) when is_atom(name) do
    case Process.whereis(name) do
      nil -> :ok
      pid -> GenServer.stop(pid)
    end
  end

  defp maybe_stop_pid(pid) when is_pid(pid) do
    if Process.alive?(pid) do
      try do
        Supervisor.stop(pid, :normal, 5_000)
      catch
        :exit, _reason -> :ok
      end
    else
      :ok
    end
  end

  defp maybe_stop_pid(nil), do: :ok
end
