defmodule Favn.SQLiteStorageBootstrapTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Version
  alias Favn.Run
  alias Favn.Storage
  alias Favn.Storage.Adapter.SQLite, as: Adapter
  alias FavnOrchestrator.Storage, as: OrchestratorStorage

  setup do
    state = Favn.TestSetup.capture_state()

    on_exit(fn ->
      Favn.TestSetup.restore_state(state, clear_storage_adapter_env?: true)
    end)

    :ok
  end

  test "adapter child_spec boots repo and migrations for runtime use" do
    db_path =
      Path.join(
        System.tmp_dir!(),
        "favn_sqlite_bootstrap_#{System.unique_integer([:positive, :monotonic])}.db"
      )

    on_exit(fn -> File.rm(db_path) end)

    :ok = Favn.TestSetup.configure_storage_adapter(Adapter, database: db_path, pool_size: 1)
    {:ok, child_spec} = Adapter.child_spec(database: db_path, pool_size: 1)
    start_supervised!(child_spec)

    assert :ok = OrchestratorStorage.put_manifest_version(manifest_version("manifest_v1"))

    run = sample_run("bootstrap-run", :running)
    assert :ok = Storage.put_run(run)
    assert {:ok, stored} = Storage.get_run("bootstrap-run")
    assert stored.id == "bootstrap-run"
  end

  test "sqlite adapter does not declare orchestrator as a runtime application" do
    assert {:ok, _apps} = Application.ensure_all_started(:favn_storage_sqlite)
    refute :favn_orchestrator in Application.spec(:favn_storage_sqlite, :applications)
  end

  defp sample_run(id, status) do
    now = DateTime.utc_now() |> DateTime.truncate(:second)

    %Run{
      id: id,
      manifest_version_id: "manifest_v1",
      manifest_content_hash: manifest_content_hash(),
      asset_ref: {Favn.SQLiteStorageBootstrapTest, :sample_asset},
      target_refs: [],
      plan: nil,
      status: status,
      event_seq: 0,
      started_at: now,
      finished_at: if(status in [:ok, :error, :cancelled, :timed_out], do: now, else: nil)
    }
  end

  defp manifest_version(manifest_version_id) do
    manifest = %Manifest{
      assets: [
        %Asset{
          ref: {Favn.SQLiteStorageBootstrapTest, :sample_asset},
          module: Favn.SQLiteStorageBootstrapTest,
          name: :sample_asset
        }
      ]
    }

    {:ok, version} = Version.new(manifest, manifest_version_id: manifest_version_id)
    version
  end

  defp manifest_content_hash, do: manifest_version("manifest_v1").content_hash
end
