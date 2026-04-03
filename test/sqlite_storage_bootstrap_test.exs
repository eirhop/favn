defmodule Favn.SQLiteStorageBootstrapTest do
  use ExUnit.Case, async: false

  alias Favn.Run
  alias Favn.Storage
  alias Favn.Storage.Adapter.SQLite

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

    :ok = Favn.TestSetup.configure_storage_adapter(SQLite, database: db_path, pool_size: 1)
    {:ok, child_spec} = SQLite.child_spec(database: db_path, pool_size: 1)
    start_supervised!(child_spec)

    run = sample_run("bootstrap-run", :running)
    assert :ok = Storage.put_run(run)
    assert {:ok, stored} = Storage.get_run("bootstrap-run")
    assert stored.id == "bootstrap-run"
  end

  defp sample_run(id, status) do
    now = DateTime.utc_now() |> DateTime.truncate(:second)

    %Run{
      id: id,
      target_refs: [],
      plan: nil,
      status: status,
      event_seq: 0,
      started_at: now,
      finished_at: if(status in [:ok, :error, :cancelled, :timed_out], do: now, else: nil)
    }
  end
end
