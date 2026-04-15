defmodule FavnStorageSqlite.AdapterTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest
  alias Favn.Manifest.Version
  alias FavnOrchestrator.RunState
  alias FavnStorageSqlite.Adapter

  setup do
    unique = System.unique_integer([:positive])
    db_path = Path.join(System.tmp_dir!(), "favn_storage_sqlite_#{unique}.db")
    supervisor_name = Module.concat([__MODULE__, "Supervisor#{unique}"])

    opts = [database: db_path, supervisor_name: supervisor_name, migration_mode: :auto]

    assert {:ok, child_spec} = Adapter.child_spec(opts)
    start_supervised!(child_spec)

    on_exit(fn ->
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

    assert :ok = Adapter.put_scheduler_state(key, %{last_due_at: DateTime.utc_now()}, opts)

    assert {:ok, %Favn.Scheduler.State{schedule_id: :daily}} =
             Adapter.get_scheduler_state(key, opts)
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
end
