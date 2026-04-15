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
