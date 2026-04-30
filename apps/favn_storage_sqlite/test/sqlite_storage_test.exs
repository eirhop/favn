defmodule Favn.SQLiteStorageTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL
  alias Favn.Run
  alias Favn.Scheduler.State, as: SchedulerState
  alias Favn.Storage
  alias Favn.Storage.Adapter.SQLite, as: Adapter
  alias FavnOrchestrator.Backfill.AssetWindowState
  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.Backfill.CoverageBaseline
  alias FavnOrchestrator.Storage, as: OrchestratorStorage
  alias FavnStorageSqlite.Migrations
  alias FavnStorageSqlite.Repo

  setup do
    state = Favn.TestSetup.capture_state()

    db_path =
      Path.join(
        System.tmp_dir!(),
        "favn_sqlite_#{System.unique_integer([:positive, :monotonic])}.db"
      )

    :ok =
      Favn.TestSetup.configure_storage_adapter(Adapter,
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
               "SELECT run_id FROM favn_runs WHERE run_id LIKE 'concurrent-run-%' ORDER BY updated_seq DESC, run_id DESC",
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
        event_seq: first.event_seq + 2
    }

    assert :ok = Storage.put_run(updated_first)

    assert {:ok, reordered_runs} = Storage.list_runs()
    assert Enum.map(reordered_runs, & &1.id) == ["run-a", "run-b"]
  end

  test "same-seq same-hash write is idempotent" do
    fixed_now = DateTime.utc_now() |> DateTime.truncate(:second)

    run =
      sample_run("sqlite-idempotent", :ok, fixed_now,
        event_seq: 1,
        params: %{payload: 1}
      )

    assert :ok = Storage.put_run(run)

    assert {:ok, %{rows: [[first_seq]]}} =
             SQL.query(Repo, "SELECT updated_seq FROM favn_runs WHERE run_id = ?1", [run.id])

    assert :ok = Storage.put_run(run)

    assert {:ok, %{rows: [[second_seq]]}} =
             SQL.query(Repo, "SELECT updated_seq FROM favn_runs WHERE run_id = ?1", [run.id])

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

  test "persists run result payloads" do
    run_ok =
      sample_run("sqlite-latest-1", :ok, DateTime.utc_now(),
        event_seq: 1,
        params: %{result_payload: 1}
      )

    run_error =
      sample_run("sqlite-latest-2", :error, DateTime.utc_now(),
        event_seq: 1,
        params: %{result_payload: 2}
      )

    assert :ok = Storage.put_run(run_ok)
    assert :ok = Storage.put_run(run_error)

    assert {:ok, fetched_ok} = Storage.get_run("sqlite-latest-1")
    assert fetched_ok.params == %{result_payload: 1}
    assert fetched_ok.status == :ok

    assert {:ok, fetched_error} = Storage.get_run("sqlite-latest-2")
    assert fetched_error.params == %{result_payload: 2}
    assert fetched_error.status == :error
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
      queued_due_at: DateTime.utc_now() |> DateTime.truncate(:second),
      version: 1
    }

    assert :ok =
             OrchestratorStorage.put_scheduler_state(
               {state.pipeline_module, state.schedule_id},
               Map.from_struct(state)
             )

    assert {:ok, %SchedulerState{} = fetched} =
             OrchestratorStorage.get_scheduler_state({state.pipeline_module, state.schedule_id})

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
      schedule_fingerprint: "daily-v1",
      version: 1
    }

    second = %SchedulerState{
      pipeline_module: pipeline,
      schedule_id: :hourly,
      schedule_fingerprint: "hourly-v1",
      version: 1
    }

    assert :ok =
             OrchestratorStorage.put_scheduler_state(
               {pipeline, :daily},
               Map.from_struct(first)
             )

    assert :ok =
             OrchestratorStorage.put_scheduler_state(
               {pipeline, :hourly},
               Map.from_struct(second)
             )

    assert {:ok, %SchedulerState{schedule_id: :daily}} =
             OrchestratorStorage.get_scheduler_state({pipeline, :daily})

    assert {:ok, %SchedulerState{schedule_id: :hourly}} =
             OrchestratorStorage.get_scheduler_state({pipeline, :hourly})

    assert {:ok, nil} = OrchestratorStorage.get_scheduler_state({pipeline, nil})
  end

  test "malformed scheduler state blobs return decode errors" do
    assert {:ok, _} =
             SQL.query(
               Repo,
               """
                INSERT INTO favn_scheduler_cursors (
                 pipeline_module,
                 schedule_id,
                 version,
                 updated_at,
                 state_blob
               ) VALUES (?1, ?2, ?3, ?4, ?5)
               """,
               [
                 "Elixir.Favn.Test.Fixtures.Pipelines.SchedulerDailyPipeline",
                 "scheduler_daily",
                 1,
                 DateTime.utc_now(),
                 <<0, 1, 2, 3>>
               ]
             )

    assert {:error, {:payload_decode_failed, _reason}} =
             OrchestratorStorage.get_scheduler_state(
               {Favn.Test.Fixtures.Pipelines.SchedulerDailyPipeline, :scheduler_daily}
             )
  end

  test "persists coverage baselines and filters by pipeline and status" do
    now = DateTime.utc_now() |> DateTime.truncate(:second)
    ok_baseline = sample_coverage_baseline("baseline-ok", :ok, now)
    pending_baseline = sample_coverage_baseline("baseline-pending", :pending, now)

    assert :ok = OrchestratorStorage.put_coverage_baseline(ok_baseline)
    assert :ok = OrchestratorStorage.put_coverage_baseline(pending_baseline)

    assert {:ok, ^ok_baseline} = OrchestratorStorage.get_coverage_baseline("baseline-ok")
    assert {:error, :not_found} = OrchestratorStorage.get_coverage_baseline("missing-baseline")

    assert {:ok, baseline_page} =
             OrchestratorStorage.list_coverage_baselines(
               pipeline_module: Favn.SQLiteStorageTest.Pipeline,
               status: :ok
             )

    assert [^ok_baseline] = baseline_page.items
  end

  test "persists backfill windows and filters by status, pipeline, and window" do
    now = DateTime.utc_now() |> DateTime.truncate(:second)
    running_window = sample_backfill_window("window-running", :running, now)
    ok_window = sample_backfill_window("window-ok", :ok, DateTime.add(now, 86_400, :second))

    assert :ok = OrchestratorStorage.put_backfill_window(running_window)
    assert :ok = OrchestratorStorage.put_backfill_window(ok_window)

    assert {:ok, ^running_window} =
             OrchestratorStorage.get_backfill_window(
               "backfill-run-1",
               Favn.SQLiteStorageTest.Pipeline,
               "window-running"
             )

    assert {:error, :not_found} =
             OrchestratorStorage.get_backfill_window(
               "backfill-run-1",
               Favn.SQLiteStorageTest.Pipeline,
               "missing-window"
             )

    assert {:ok, window_page} =
             OrchestratorStorage.list_backfill_windows(
               pipeline_module: Favn.SQLiteStorageTest.Pipeline,
               window_key: "window-running",
               status: :running
             )

    assert [^running_window] = window_page.items
  end

  test "paginates backfill windows with limit and offset metadata" do
    now = DateTime.utc_now() |> DateTime.truncate(:second)
    first_window = sample_backfill_window("window-first", :running, now)

    second_window =
      sample_backfill_window("window-second", :running, DateTime.add(now, 86_400, :second))

    assert :ok = OrchestratorStorage.put_backfill_window(first_window)
    assert :ok = OrchestratorStorage.put_backfill_window(second_window)

    assert {:ok, first_page} = OrchestratorStorage.list_backfill_windows(limit: 1)
    assert [^first_window] = first_page.items
    assert first_page.limit == 1
    assert first_page.offset == 0
    assert first_page.has_more? == true
    assert first_page.next_offset == 1

    assert {:ok, second_page} = OrchestratorStorage.list_backfill_windows(limit: 1, offset: 1)
    assert [^second_window] = second_page.items
    assert second_page.has_more? == false
    assert second_page.next_offset == nil
  end

  test "persists asset window states and filters by status, pipeline, and window" do
    now = DateTime.utc_now() |> DateTime.truncate(:second)
    running_state = sample_asset_window_state(:orders, "window-running", :running, now)

    ok_state =
      sample_asset_window_state(:customers, "window-ok", :ok, DateTime.add(now, 86_400, :second))

    assert :ok = OrchestratorStorage.put_asset_window_state(running_state)
    assert :ok = OrchestratorStorage.put_asset_window_state(ok_state)

    assert {:ok, ^running_state} =
             OrchestratorStorage.get_asset_window_state(
               Favn.SQLiteStorageTest.Asset,
               :orders,
               "window-running"
             )

    assert {:error, :not_found} =
             OrchestratorStorage.get_asset_window_state(
               Favn.SQLiteStorageTest.Asset,
               :orders,
               "missing-window"
             )

    assert {:ok, state_page} =
             OrchestratorStorage.list_asset_window_states(
               pipeline_module: Favn.SQLiteStorageTest.Pipeline,
               window_key: "window-running",
               status: :running
             )

    assert [^running_state] = state_page.items
  end

  test "decodes legacy backfill window kind aliases through constructors" do
    now = DateTime.utc_now() |> DateTime.truncate(:second)
    baseline = sample_coverage_baseline("baseline-legacy-kind", :ok, now)
    window = sample_backfill_window("window-legacy-kind", :running, now)
    state = sample_asset_window_state(:orders, "asset-legacy-kind", :running, now)

    assert :ok = OrchestratorStorage.put_coverage_baseline(baseline)
    assert :ok = OrchestratorStorage.put_backfill_window(window)
    assert :ok = OrchestratorStorage.put_asset_window_state(state)

    assert {:ok, _} =
             SQL.query(Repo, "UPDATE favn_pipeline_coverage_baselines SET window_kind = ?1", [
               "daily"
             ])

    assert {:ok, _} =
             SQL.query(Repo, "UPDATE favn_backfill_windows SET window_kind = ?1", ["daily"])

    assert {:ok, _} =
             SQL.query(Repo, "UPDATE favn_asset_window_states SET window_kind = ?1", ["daily"])

    assert {:ok, ^baseline} = OrchestratorStorage.get_coverage_baseline(baseline.baseline_id)

    assert {:ok, ^window} =
             OrchestratorStorage.get_backfill_window(
               window.backfill_run_id,
               window.pipeline_module,
               window.window_key
             )

    assert {:ok, ^state} =
             OrchestratorStorage.get_asset_window_state(
               state.asset_ref_module,
               state.asset_ref_name,
               state.window_key
             )
  end

  test "rejects invalid persisted backfill read-model statuses" do
    now = DateTime.utc_now() |> DateTime.truncate(:second)
    baseline = sample_coverage_baseline("baseline-invalid-status", :ok, now)
    window = sample_backfill_window("window-invalid-status", :running, now)
    state = sample_asset_window_state(:orders, "asset-invalid-status", :running, now)

    assert :ok = OrchestratorStorage.put_coverage_baseline(baseline)
    assert :ok = OrchestratorStorage.put_backfill_window(window)
    assert :ok = OrchestratorStorage.put_asset_window_state(state)

    assert {:ok, _} =
             SQL.query(
               Repo,
               "UPDATE favn_pipeline_coverage_baselines SET status = ?1 WHERE baseline_id = ?2",
               ["bogus", baseline.baseline_id]
             )

    assert {:ok, _} =
             SQL.query(
               Repo,
               "UPDATE favn_backfill_windows SET status = ?1 WHERE window_key = ?2",
               [
                 "bogus",
                 window.window_key
               ]
             )

    assert {:ok, _} =
             SQL.query(
               Repo,
               "UPDATE favn_asset_window_states SET status = ?1 WHERE window_key = ?2",
               [
                 "bogus",
                 state.window_key
               ]
             )

    assert {:error, {:invalid_status, "bogus"}} =
             OrchestratorStorage.get_coverage_baseline(baseline.baseline_id)

    assert {:error, {:invalid_status, "bogus"}} =
             OrchestratorStorage.get_backfill_window(
               window.backfill_run_id,
               window.pipeline_module,
               window.window_key
             )

    assert {:error, {:invalid_status, "bogus"}} =
             OrchestratorStorage.get_asset_window_state(
               state.asset_ref_module,
               state.asset_ref_name,
               state.window_key
             )
  end

  test "rejects invalid persisted backfill read-model window kinds" do
    now = DateTime.utc_now() |> DateTime.truncate(:second)
    baseline = sample_coverage_baseline("baseline-invalid-kind", :ok, now)
    window = sample_backfill_window("window-invalid-kind", :running, now)
    state = sample_asset_window_state(:orders, "asset-invalid-kind", :running, now)

    assert :ok = OrchestratorStorage.put_coverage_baseline(baseline)
    assert :ok = OrchestratorStorage.put_backfill_window(window)
    assert :ok = OrchestratorStorage.put_asset_window_state(state)

    assert {:ok, _} =
             SQL.query(
               Repo,
               "UPDATE favn_pipeline_coverage_baselines SET window_kind = ?1 WHERE baseline_id = ?2",
               ["fortnight", baseline.baseline_id]
             )

    assert {:ok, _} =
             SQL.query(
               Repo,
               "UPDATE favn_backfill_windows SET window_kind = ?1 WHERE window_key = ?2",
               [
                 "fortnight",
                 window.window_key
               ]
             )

    assert {:ok, _} =
             SQL.query(
               Repo,
               "UPDATE favn_asset_window_states SET window_kind = ?1 WHERE window_key = ?2",
               [
                 "fortnight",
                 state.window_key
               ]
             )

    assert {:error, {:invalid_window_kind, "fortnight"}} =
             OrchestratorStorage.get_coverage_baseline(baseline.baseline_id)

    assert {:error, {:invalid_window_kind, "fortnight"}} =
             OrchestratorStorage.get_backfill_window(
               window.backfill_run_id,
               window.pipeline_module,
               window.window_key
             )

    assert {:error, {:invalid_window_kind, "fortnight"}} =
             OrchestratorStorage.get_asset_window_state(
               state.asset_ref_module,
               state.asset_ref_name,
               state.window_key
             )
  end

  test "rejects unknown persisted backfill identity atoms" do
    now = DateTime.utc_now() |> DateTime.truncate(:second)
    baseline = sample_coverage_baseline("baseline-unknown-atom", :ok, now)
    state = sample_asset_window_state(:orders, "asset-unknown-atom", :running, now)
    unknown_pipeline = "Elixir.Favn.SQLiteStorageTest.UnknownPipeline"
    unknown_asset_name = "unknown_asset_name_#{System.unique_integer([:positive])}"

    assert :ok = OrchestratorStorage.put_coverage_baseline(baseline)
    assert :ok = OrchestratorStorage.put_asset_window_state(state)

    assert {:ok, _} =
             SQL.query(
               Repo,
               "UPDATE favn_pipeline_coverage_baselines SET pipeline_module = ?1 WHERE baseline_id = ?2",
               [unknown_pipeline, baseline.baseline_id]
             )

    assert {:error, {:unknown_atom, ^unknown_pipeline}} =
             OrchestratorStorage.get_coverage_baseline(baseline.baseline_id)

    assert {:ok, _} =
             SQL.query(
               Repo,
               "UPDATE favn_asset_window_states SET asset_ref_name = ?1 WHERE window_key = ?2",
               [unknown_asset_name, state.window_key]
             )

    assert {:error, {:unknown_atom, ^unknown_asset_name}} =
             OrchestratorStorage.list_asset_window_states()
  end

  defp sample_run(id, status, started_at \\ DateTime.utc_now(), opts \\ []) do
    now = DateTime.truncate(started_at, :second)

    event_seq = Keyword.get(opts, :event_seq, 0)
    params = Keyword.get(opts, :params, %{})

    %Run{
      id: id,
      manifest_version_id: "manifest_v1",
      manifest_content_hash: "manifest_hash_v1",
      asset_ref: {Favn.SQLiteStorageTest, :sample_asset},
      target_refs: [],
      plan: nil,
      status: status,
      submit_kind: :asset,
      replay_mode: :none,
      event_seq: event_seq,
      started_at: now,
      finished_at: if(status in [:ok, :error, :cancelled, :timed_out], do: now, else: nil),
      params: params,
      retry_policy: %{max_attempts: 1, delay_ms: 0, retry_on: []}
    }
  end

  defp sample_coverage_baseline(baseline_id, status, now) do
    {:ok, baseline} =
      CoverageBaseline.new(%{
        baseline_id: baseline_id,
        pipeline_module: Favn.SQLiteStorageTest.Pipeline,
        source_key: "orders-api",
        segment_key_hash: "sha256:orders",
        segment_key_redacted: "orders-***",
        window_kind: :daily,
        timezone: "Etc/UTC",
        coverage_start_at: DateTime.add(now, -86_400, :second),
        coverage_until: now,
        created_by_run_id: "baseline-run-1",
        manifest_version_id: "manifest_v1",
        status: status,
        errors: [%{reason: :sample_error}],
        metadata: %{row_count: 12},
        created_at: now,
        updated_at: now
      })

    baseline
  end

  defp sample_backfill_window(window_key, status, window_start_at) do
    window_end_at = DateTime.add(window_start_at, 86_400, :second)

    {:ok, window} =
      BackfillWindow.new(%{
        backfill_run_id: "backfill-run-1",
        child_run_id: "child-#{window_key}",
        pipeline_module: Favn.SQLiteStorageTest.Pipeline,
        manifest_version_id: "manifest_v1",
        coverage_baseline_id: "baseline-ok",
        window_kind: :daily,
        window_start_at: window_start_at,
        window_end_at: window_end_at,
        timezone: "Etc/UTC",
        window_key: window_key,
        status: status,
        attempt_count: 2,
        latest_attempt_run_id: "attempt-#{window_key}",
        last_success_run_id: if(status == :ok, do: "success-#{window_key}", else: nil),
        last_error: %{reason: :sample_error},
        started_at: window_start_at,
        finished_at: if(status == :running, do: nil, else: window_end_at),
        created_at: window_start_at,
        updated_at: window_end_at,
        errors: [%{attempt: 1, reason: :sample_error}],
        metadata: %{source: "sqlite-test"}
      })

    window
  end

  defp sample_asset_window_state(asset_name, window_key, status, window_start_at) do
    window_end_at = DateTime.add(window_start_at, 86_400, :second)

    {:ok, state} =
      AssetWindowState.new(%{
        asset_ref_module: Favn.SQLiteStorageTest.Asset,
        asset_ref_name: asset_name,
        pipeline_module: Favn.SQLiteStorageTest.Pipeline,
        manifest_version_id: "manifest_v1",
        window_kind: :daily,
        window_start_at: window_start_at,
        window_end_at: window_end_at,
        timezone: "Etc/UTC",
        window_key: window_key,
        status: status,
        latest_run_id: "asset-run-#{window_key}",
        latest_parent_run_id: "backfill-run-1",
        latest_success_run_id: if(status == :ok, do: "asset-success-#{window_key}", else: nil),
        latest_error: %{reason: :sample_error},
        rows_written: 42,
        errors: [%{reason: :sample_error}],
        metadata: %{partition: window_key},
        updated_at: window_end_at
      })

    state
  end
end
