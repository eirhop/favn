defmodule Favn.SQLiteStorageTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL
  alias Favn.Run
  alias Favn.Run.AssetResult
  alias Favn.Scheduler.State, as: SchedulerState
  alias Favn.Storage
  alias Favn.Storage.SQLite.Migrations
  alias Favn.Storage.SQLite.Repo
  alias Favn.Window.Key

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
             SQL.query(
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
             SQL.query(
               Repo,
               "SELECT id FROM runs WHERE id LIKE 'concurrent-run-%' ORDER BY updated_seq DESC, updated_at_us DESC, id DESC",
               []
             )

    assert listed_ids == Enum.map(rows, &hd/1)

    assert {:ok, %{rows: [[counter_value]]}} =
             SQL.query(
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

    updated_first = %{
      first
      | status: :ok,
        finished_at: base_started_at,
        event_seq: first.event_seq + 1
    }

    assert :ok = Storage.put_run(updated_first)

    assert {:ok, reordered_runs} = Storage.list_runs()
    assert Enum.map(reordered_runs, & &1.id) == ["run-a", "run-b"]
  end

  test "same-seq same-hash write is idempotent" do
    run =
      sample_run("sqlite-idempotent", :running, DateTime.utc_now(),
        event_seq: 1,
        params: %{payload: 1}
      )

    assert :ok = Storage.put_run(run)

    assert {:ok, %{rows: [[first_seq]]}} =
             SQL.query(Repo, "SELECT updated_seq FROM runs WHERE id = ?1", [run.id])

    assert :ok = Storage.put_run(run)

    assert {:ok, %{rows: [[second_seq]]}} =
             SQL.query(Repo, "SELECT updated_seq FROM runs WHERE id = ?1", [run.id])

    assert first_seq == second_seq
  end

  test "same-seq different-hash write returns conflicting snapshot" do
    run =
      sample_run("sqlite-conflict", :running, DateTime.utc_now(),
        event_seq: 1,
        params: %{payload: 1}
      )

    assert :ok = Storage.put_run(run)

    conflict =
      sample_run("sqlite-conflict", :running, DateTime.utc_now(),
        event_seq: 1,
        params: %{payload: 2}
      )

    assert {:error, {:store_error, :conflicting_snapshot}} = Storage.put_run(conflict)
  end

  test "lower-seq write returns stale write" do
    newer =
      sample_run("sqlite-stale", :running, DateTime.utc_now(),
        event_seq: 2,
        params: %{payload: 2}
      )

    assert :ok = Storage.put_run(newer)

    older =
      sample_run("sqlite-stale", :running, DateTime.utc_now(),
        event_seq: 1,
        params: %{payload: 1}
      )

    assert {:error, {:store_error, :stale_write}} = Storage.put_run(older)
  end

  test "higher-seq write updates run" do
    first =
      sample_run("sqlite-higher", :running, DateTime.utc_now(),
        event_seq: 1,
        params: %{payload: 1}
      )

    second =
      sample_run("sqlite-higher", :running, DateTime.utc_now(),
        event_seq: 2,
        params: %{payload: 2}
      )

    assert :ok = Storage.put_run(first)
    assert :ok = Storage.put_run(second)
    assert {:ok, fetched} = Storage.get_run("sqlite-higher")
    assert fetched.event_seq == 2
    assert fetched.params == %{payload: 2}
  end

  test "latest window state advances only on successful node results" do
    ref = {Favn.SQLiteStorageTest, :daily}

    key =
      Key.new!(
        :day,
        DateTime.from_naive!(~N[2026-04-13 00:00:00], "Etc/UTC"),
        "Etc/UTC"
      )

    success = node_result(ref, :ok)
    failed = node_result(ref, :error)

    run_ok =
      sample_run("sqlite-latest-1", :ok, DateTime.utc_now(),
        event_seq: 1,
        node_results: %{{ref, key} => success}
      )

    run_error =
      sample_run("sqlite-latest-2", :error, DateTime.utc_now(),
        event_seq: 1,
        node_results: %{{ref, key} => failed}
      )

    assert :ok = Storage.put_run(run_ok)
    assert :ok = Storage.put_run(run_error)

    assert {:ok, %{rows: rows}} =
             SQL.query(
               Repo,
               "SELECT status, last_run_id FROM window_latest_results WHERE ref_module = ?1 AND ref_name = ?2 AND window_key = ?3",
               [Atom.to_string(elem(ref, 0)), Atom.to_string(elem(ref, 1)), Key.encode(key)]
             )

    assert rows == [["ok", "sqlite-latest-1"]]
  end

  test "persists and fetches scheduler state rows" do
    state = %SchedulerState{
      pipeline_module: Favn.Test.Fixtures.Pipelines.SchedulerDailyPipeline,
      schedule_id: :scheduler_daily,
      schedule_fingerprint: "fingerprint-v1",
      last_evaluated_at: DateTime.utc_now() |> DateTime.truncate(:second),
      last_due_at: DateTime.utc_now() |> DateTime.truncate(:second),
      last_submitted_due_at: DateTime.utc_now() |> DateTime.truncate(:second),
      in_flight_run_id: "run-123",
      queued_due_at: DateTime.utc_now() |> DateTime.truncate(:second)
    }

    assert :ok = Favn.Scheduler.Storage.put_state(state)

    assert {:ok, %SchedulerState{} = fetched} =
             Favn.Scheduler.Storage.get_state(state.pipeline_module)

    assert fetched.pipeline_module == state.pipeline_module
    assert fetched.schedule_id == state.schedule_id
    assert fetched.schedule_fingerprint == state.schedule_fingerprint
    assert fetched.in_flight_run_id == state.in_flight_run_id
  end

  test "persists scheduler states for multiple schedule ids in same pipeline" do
    pipeline = Favn.Test.Fixtures.Pipelines.SchedulerDailyPipeline

    first = %SchedulerState{
      pipeline_module: pipeline,
      schedule_id: :daily,
      schedule_fingerprint: "daily-v1"
    }

    second = %SchedulerState{
      pipeline_module: pipeline,
      schedule_id: :hourly,
      schedule_fingerprint: "hourly-v1"
    }

    assert :ok = Favn.Scheduler.Storage.put_state(first)
    assert :ok = Favn.Scheduler.Storage.put_state(second)

    assert {:ok, %SchedulerState{schedule_id: :daily}} =
             Favn.Scheduler.Storage.get_state(pipeline, :daily)

    assert {:ok, %SchedulerState{schedule_id: :hourly}} =
             Favn.Scheduler.Storage.get_state(pipeline, :hourly)

    assert {:ok, %SchedulerState{schedule_id: :hourly}} =
             Favn.Scheduler.Storage.get_state(pipeline)
  end

  test "malformed scheduler datetime columns are treated as nil" do
    assert {:ok, _} =
             SQL.query(
               Repo,
               """
                INSERT INTO scheduler_states (
                 pipeline_module,
                 schedule_id,
                 schedule_fingerprint,
                 last_evaluated_at,
                 last_due_at,
                 last_submitted_due_at,
                 in_flight_run_id,
                 queued_due_at,
                 updated_at
               ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
               """,
               [
                 "Elixir.Favn.Test.Fixtures.Pipelines.SchedulerDailyPipeline",
                 "scheduler_daily",
                 "fingerprint-bad",
                 "not-a-datetime",
                 "also-bad",
                 "still-bad",
                 "run-123",
                 "bad-too",
                 "bad-updated"
               ]
             )

    assert {:ok, %SchedulerState{} = fetched} =
             Favn.Scheduler.Storage.get_state(Favn.Test.Fixtures.Pipelines.SchedulerDailyPipeline)

    assert fetched.last_evaluated_at == nil
    assert fetched.last_due_at == nil
    assert fetched.last_submitted_due_at == nil
    assert fetched.queued_due_at == nil
    assert fetched.updated_at == nil
  end

  defp sample_run(id, status, started_at \\ DateTime.utc_now(), opts \\ []) do
    now = DateTime.truncate(started_at, :second)

    event_seq = Keyword.get(opts, :event_seq, 0)
    params = Keyword.get(opts, :params, %{})
    node_results = Keyword.get(opts, :node_results, %{})
    asset_results = Keyword.get(opts, :asset_results, %{})

    %Run{
      id: id,
      target_refs: [],
      plan: nil,
      status: status,
      submit_kind: :asset,
      replay_mode: :none,
      event_seq: event_seq,
      started_at: now,
      finished_at: if(status in [:ok, :error, :cancelled, :timed_out], do: now, else: nil),
      params: params,
      retry_policy: %{max_attempts: 1, delay_ms: 0, retry_on: []},
      node_results: node_results,
      asset_results: asset_results
    }
  end

  defp node_result(ref, status) do
    now = DateTime.utc_now() |> DateTime.truncate(:second)

    %AssetResult{
      ref: ref,
      stage: 0,
      status: status,
      started_at: now,
      finished_at: now,
      duration_ms: 0,
      attempt_count: 1,
      max_attempts: 1
    }
  end
end
