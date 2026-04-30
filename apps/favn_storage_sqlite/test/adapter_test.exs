defmodule FavnStorageSqlite.AdapterTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Version
  alias Favn.Storage.Adapter.SQLite, as: Adapter
  alias FavnOrchestrator.RunState

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

    assert :ok = Adapter.put_manifest_version(version, opts)
    assert :ok = Adapter.put_manifest_version(version, opts)

    assert {:ok, stored} = Adapter.get_manifest_version("mv_sqlite", opts)
    assert stored.content_hash == version.content_hash
    assert %Manifest{} = stored.manifest
    assert [%Asset{ref: {MyApp.Asset, :asset}}] = stored.manifest.assets

    assert {:ok, listed} = Adapter.list_manifest_versions(opts)
    assert [%Version{manifest: %Manifest{assets: [%Asset{ref: {MyApp.Asset, :asset}}]}}] = listed

    assert :ok = Adapter.set_active_manifest_version("mv_sqlite", opts)
    assert {:ok, "mv_sqlite"} = Adapter.get_active_manifest_version(opts)
  end

  test "enforces run snapshot write semantics", %{opts: opts} do
    base =
      RunState.new(
        id: "run_sqlite_1",
        manifest_version_id: "mv_sqlite",
        manifest_content_hash: "hash_sqlite",
        asset_ref: {MyApp.Asset, :asset}
      )

    assert :ok = Adapter.put_run(base, opts)
    assert :ok = Adapter.put_run(base, opts)

    stale = %{base | event_seq: 0} |> RunState.with_snapshot_hash()
    assert {:error, :stale_write} = Adapter.put_run(stale, opts)

    conflict = %{base | status: :error} |> RunState.with_snapshot_hash()
    assert {:error, :conflicting_snapshot} = Adapter.put_run(conflict, opts)

    assert {:ok, stored} = Adapter.get_run(base.id, opts)
    assert stored.status == :pending
  end

  test "stores run events and scheduler cursor state", %{opts: opts} do
    event = %{
      sequence: 1,
      event_type: :run_started,
      occurred_at: DateTime.utc_now(),
      data: %{a: 1}
    }

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

    assert {:ok, %Favn.Scheduler.State{schedule_id: :daily}} =
             Adapter.get_scheduler_state(key, opts)
  end

  test "read-model list APIs return invalid pagination errors", %{opts: opts} do
    filters = [limit: 0]

    assert {:error, :invalid_pagination} = Adapter.list_coverage_baselines(filters, opts)
    assert {:error, :invalid_pagination} = Adapter.list_backfill_windows(filters, opts)
    assert {:error, :invalid_pagination} = Adapter.list_asset_window_states(filters, opts)
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
