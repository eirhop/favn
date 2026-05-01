defmodule FavnOrchestrator.Storage.MemoryAdapterTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest
  alias Favn.Manifest.Version
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.Storage.Adapter.Memory

  setup do
    Memory.reset()
    :ok
  end

  test "stores manifest versions idempotently and supports activation" do
    version = manifest_version("mv_a")

    assert :ok = Storage.put_manifest_version(version)
    assert :ok = Storage.put_manifest_version(version)

    assert {:ok, stored} = Storage.get_manifest_version("mv_a")
    assert stored.content_hash == version.content_hash

    assert :ok = Storage.set_active_manifest_version("mv_a")
    assert {:ok, "mv_a"} = Storage.get_active_manifest_version()
  end

  test "rejects run snapshot stale and conflicting writes" do
    base =
      RunState.new(
        id: "run_1",
        manifest_version_id: "mv_a",
        manifest_content_hash: "hash",
        asset_ref: {MyApp.Asset, :asset}
      )

    assert :ok = Storage.put_run(base)
    assert :ok = Storage.put_run(base)

    stale = %{base | event_seq: 0} |> RunState.with_snapshot_hash()
    assert {:error, :stale_write} = Storage.put_run(stale)

    conflict = %{base | status: :error} |> RunState.with_snapshot_hash()
    assert {:error, :conflicting_snapshot} = Storage.put_run(conflict)
  end

  test "accepts higher-seq writes and returns latest stored run" do
    base =
      RunState.new(
        id: "run_2",
        manifest_version_id: "mv_a",
        manifest_content_hash: "hash",
        asset_ref: {MyApp.Asset, :asset}
      )

    assert :ok = Storage.put_run(base)

    newer =
      %{base | event_seq: base.event_seq + 1, status: :running} |> RunState.with_snapshot_hash()

    assert :ok = Storage.put_run(newer)

    assert {:ok, stored} = Storage.get_run(base.id)
    assert stored.event_seq == 2
  end

  test "normalizes and validates run events" do
    event = %{sequence: 1, event_type: :run_started, occurred_at: DateTime.utc_now()}

    assert :ok = Storage.append_run_event("run_1", event)
    assert :ok = Storage.append_run_event("run_1", event)

    assert {:ok, [stored]} = Storage.list_run_events("run_1")
    assert stored.run_id == "run_1"
    assert stored.sequence == 1

    assert {:error, {:invalid_run_event_field, :sequence, 0}} =
             Storage.append_run_event("run_1", %{sequence: 0, event_type: :run_started})

    assert {:error, :conflicting_event_sequence} =
             Storage.append_run_event("run_1", %{sequence: 1, event_type: :run_updated})
  end

  test "validates scheduler state payload" do
    key = {MyApp.Pipeline, :daily}
    now = DateTime.utc_now()

    assert :ok = Storage.put_scheduler_state(key, %{last_due_at: now, version: 1})
    assert {:ok, stored} = Storage.get_scheduler_state(key)
    assert stored.last_due_at == now

    assert {:error, {:invalid_scheduler_field, :last_due_at, "bad"}} =
             Storage.put_scheduler_state(key, %{last_due_at: "bad", version: 2})
  end

  test "uses nil scheduler schedule ids as exact keys" do
    daily_key = {MyApp.Pipeline, :daily}
    hourly_key = {MyApp.Pipeline, :hourly}
    nil_key = {MyApp.Pipeline, nil}

    assert :ok = Storage.put_scheduler_state(daily_key, %{version: 1})
    assert :ok = Storage.put_scheduler_state(hourly_key, %{version: 1})

    assert {:ok, %Favn.Scheduler.State{schedule_id: :daily}} =
             Storage.get_scheduler_state(daily_key)

    assert {:ok, %Favn.Scheduler.State{schedule_id: :hourly}} =
             Storage.get_scheduler_state(hourly_key)

    assert {:ok, nil} = Storage.get_scheduler_state(nil_key)

    assert :ok = Storage.put_scheduler_state(nil_key, %{version: 1})
    assert {:ok, %Favn.Scheduler.State{schedule_id: nil}} = Storage.get_scheduler_state(nil_key)
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
