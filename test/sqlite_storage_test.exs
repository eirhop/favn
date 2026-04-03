defmodule Favn.SQLiteStorageTest do
  use ExUnit.Case, async: false

  alias Favn.Run
  alias Favn.Storage
  alias Favn.Storage.SQLite.Migrations
  alias Favn.Storage.SQLite.Repo

  setup do
    state = Favn.TestSetup.capture_state()

    db_path =
      Path.join(
        System.tmp_dir!(),
        "favn_sqlite_#{System.unique_integer([:positive, :monotonic])}.db"
      )

    :ok =
      Favn.TestSetup.configure_storage_adapter(Favn.Storage.Adapter.SQLite,
        database: db_path,
        pool_size: 1
      )
    start_supervised!({Repo, database: db_path, pool_size: 1, busy_timeout: 5_000})
    :ok = Migrations.migrate!(Repo)

    on_exit(fn ->
      Favn.TestSetup.restore_state(state, clear_storage_adapter_env?: true)
      File.rm(db_path)
    end)

    :ok
  end

  test "persists and fetches runs" do
    run = sample_run("sqlite-run-1", :running)

    assert :ok = Storage.put_run(run)
    assert {:ok, fetched} = Storage.get_run("sqlite-run-1")
    assert fetched.id == run.id
    assert fetched.status == :running
  end

  test "lists runs newest first by latest persisted write, not by id" do
    same_started_at = DateTime.utc_now() |> DateTime.truncate(:second)
    first = sample_run("zzz-first-id", :ok, same_started_at)
    second = sample_run("aaa-second-id", :error, same_started_at)

    assert :ok = Storage.put_run(first)
    assert :ok = Storage.put_run(second)

    assert {:ok, all_runs} = Storage.list_runs()
    assert Enum.map(all_runs, & &1.id) == ["aaa-second-id", "zzz-first-id"]

    assert {:ok, errored} = Storage.list_runs(status: :error)
    assert Enum.map(errored, & &1.id) == ["aaa-second-id"]

    assert {:ok, limited} = Storage.list_runs(limit: 1)
    assert Enum.map(limited, & &1.id) == ["aaa-second-id"]
  end

  test "returns :not_found for missing run id" do
    assert {:error, :not_found} = Storage.get_run("missing-sqlite-run")
  end

  test "does not keep run_write_orders helper table after migrations" do
    assert {:ok, %{rows: [[0]]}} =
             Ecto.Adapters.SQL.query(
               Repo,
               "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'run_write_orders'",
               []
             )
  end

  test "concurrent writes preserve adapter ordering in list_runs/1" do
    base_started_at = DateTime.utc_now() |> DateTime.truncate(:second)

    tasks =
      for index <- 1..12 do
        Task.async(fn ->
          run_id = "concurrent-run-#{index}"
          run = sample_run(run_id, :ok, base_started_at)
          assert :ok = Storage.put_run(run)
          run_id
        end)
      end

    _ids = Enum.map(tasks, &Task.await(&1, 5_000))

    assert {:ok, runs} = Storage.list_runs()

    listed_ids =
      runs
      |> Enum.map(& &1.id)
      |> Enum.filter(&String.starts_with?(&1, "concurrent-run-"))

    assert length(listed_ids) == 12

    assert {:ok, %{rows: rows}} =
             Ecto.Adapters.SQL.query(
               Repo,
               "SELECT id FROM runs WHERE id LIKE 'concurrent-run-%' ORDER BY updated_seq DESC, updated_at_us DESC, id DESC",
               []
             )

    assert listed_ids == Enum.map(rows, &hd/1)

    assert {:ok, %{rows: [[counter_value]]}} =
             Ecto.Adapters.SQL.query(
               Repo,
               "SELECT value FROM favn_counters WHERE name = 'run_write_order'",
               []
             )

    assert counter_value >= 12
  end

  test "updating the same run id advances sequence and moves run to front" do
    base_started_at = DateTime.utc_now() |> DateTime.truncate(:second)
    first = sample_run("run-a", :running, base_started_at)
    second = sample_run("run-b", :running, base_started_at)

    assert :ok = Storage.put_run(first)
    assert :ok = Storage.put_run(second)

    assert {:ok, initial_runs} = Storage.list_runs()
    assert Enum.map(initial_runs, & &1.id) == ["run-b", "run-a"]

    updated_first = %{first | status: :ok, finished_at: base_started_at}
    assert :ok = Storage.put_run(updated_first)

    assert {:ok, reordered_runs} = Storage.list_runs()
    assert Enum.map(reordered_runs, & &1.id) == ["run-a", "run-b"]
  end

  defp sample_run(id, status, started_at \\ DateTime.utc_now()) do
    now = DateTime.truncate(started_at, :second)

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
