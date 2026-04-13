defmodule Favn.PostgresStorageIntegrationTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL
  alias Favn.Run
  alias Favn.Run.AssetResult
  alias Favn.Scheduler.State, as: SchedulerState
  alias Favn.Scheduler.Storage, as: SchedulerStorage
  alias Favn.Storage
  alias Favn.Storage.Adapter.Postgres
  alias Favn.Storage.Postgres.Migrations
  alias Favn.Storage.Postgres.Repo
  alias Favn.Window.Key

  setup do
    case System.get_env("FAVN_TEST_POSTGRES_URL") do
      nil ->
        {:ok, pg_enabled?: false}

      url ->
        state = Favn.TestSetup.capture_state()
        repo_opts = [url: url, pool_size: 2]

        start_supervised!({Repo, repo_opts})
        :ok = Migrations.migrate!(Repo)

        assert {:ok, _} =
                 SQL.query(
                   Repo,
                   "TRUNCATE TABLE favn_asset_window_latest, favn_run_nodes, favn_runs, favn_scheduler_cursors RESTART IDENTITY",
                   []
                 )

        :ok =
          Favn.TestSetup.configure_storage_adapter(Postgres,
            repo_mode: :external,
            repo: Repo,
            migration_mode: :manual
          )

        on_exit(fn ->
          Favn.TestSetup.restore_state(state, clear_storage_adapter_env?: true)
        end)

        {:ok, repo_opts: repo_opts, pg_enabled?: true}
    end
  end

  test "same-seq idempotent and conflict semantics", ctx do
    if ctx[:pg_enabled?] do
      run = sample_run("pg-int-1", 1, %{payload: 1})

      assert :ok = Storage.put_run(run)
      assert :ok = Storage.put_run(run)

      conflict = sample_run("pg-int-1", 1, %{payload: 2})
      assert {:error, {:store_error, :conflicting_snapshot}} = Storage.put_run(conflict)

      newer = sample_run("pg-int-1", 2, %{payload: 3})
      assert :ok = Storage.put_run(newer)

      stale = sample_run("pg-int-1", 1, %{payload: 4})
      assert {:error, {:store_error, :stale_write}} = Storage.put_run(stale)
    else
      assert true
    end
  end

  test "repeated writes with successful nodes remain safe", ctx do
    if ctx[:pg_enabled?] do
      run = sample_run("pg-int-2", 1, %{payload: 1}, with_success_node?: true)
      assert :ok = Storage.put_run(run)

      updated = sample_run("pg-int-2", 2, %{payload: 2}, with_success_node?: true)
      assert :ok = Storage.put_run(updated)

      assert {:ok, fetched} = Storage.get_run("pg-int-2")
      assert fetched.event_seq == 2
    else
      assert true
    end
  end

  test "external repo mode persists and lists runs", ctx do
    if ctx[:pg_enabled?] do
      assert :ok = Storage.put_run(sample_run("pg-int-3", 1, %{kind: :a}))
      assert :ok = Storage.put_run(sample_run("pg-int-4", 1, %{kind: :b}))

      assert {:ok, runs} = Storage.list_runs()
      assert Enum.map(runs, & &1.id) == ["pg-int-4", "pg-int-3"]
    else
      assert true
    end
  end

  test "manual readiness fails when migration version row is missing", ctx do
    if ctx[:pg_enabled?] do
      assert true == Migrations.schema_ready?(Repo)

      assert {:ok, _} = SQL.query(Repo, "DELETE FROM schema_migrations", [])

      assert false == Migrations.schema_ready?(Repo)

      :ok = Migrations.migrate!(Repo)
      assert true == Migrations.schema_ready?(Repo)
    else
      assert true
    end
  end

  test "scheduler cursors support multiple schedule ids per pipeline", ctx do
    if ctx[:pg_enabled?] do
      pipeline = Favn.Test.Fixtures.Pipelines.SchedulerDailyPipeline

      first = %SchedulerState{
        pipeline_module: pipeline,
        schedule_id: :daily,
        schedule_fingerprint: "daily-v1",
        updated_at: DateTime.utc_now() |> DateTime.truncate(:microsecond)
      }

      second = %SchedulerState{
        pipeline_module: pipeline,
        schedule_id: :hourly,
        schedule_fingerprint: "hourly-v1",
        updated_at: DateTime.utc_now() |> DateTime.truncate(:microsecond)
      }

      assert :ok = SchedulerStorage.put_state(first)
      assert :ok = SchedulerStorage.put_state(second)

      assert {:ok, %SchedulerState{schedule_id: :daily}} =
               SchedulerStorage.get_state(pipeline, :daily)

      assert {:ok, %SchedulerState{schedule_id: :hourly}} =
               SchedulerStorage.get_state(pipeline, :hourly)
    else
      assert true
    end
  end

  test "nil schedule_id fallback returns latest cursor for pipeline", ctx do
    if ctx[:pg_enabled?] do
      pipeline = Favn.Test.Fixtures.Pipelines.SchedulerDailyPipeline

      first = %SchedulerState{
        pipeline_module: pipeline,
        schedule_id: :daily,
        schedule_fingerprint: "daily-v1",
        updated_at:
          DateTime.add(DateTime.utc_now(), -60, :second) |> DateTime.truncate(:microsecond)
      }

      second = %SchedulerState{
        pipeline_module: pipeline,
        schedule_id: :hourly,
        schedule_fingerprint: "hourly-v1",
        updated_at: DateTime.utc_now() |> DateTime.truncate(:microsecond)
      }

      assert :ok = SchedulerStorage.put_state(first)
      assert :ok = SchedulerStorage.put_state(second)

      assert {:ok, %SchedulerState{schedule_id: :hourly}} = SchedulerStorage.get_state(pipeline)
    else
      assert true
    end
  end

  defp sample_run(id, event_seq, params, opts \\ []) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    node_results =
      if Keyword.get(opts, :with_success_node?, false) do
        key = Key.new!(:day, DateTime.from_naive!(~N[2026-04-13 00:00:00], "Etc/UTC"), "Etc/UTC")
        ref = {Favn.PostgresStorageIntegrationTest, :asset}

        result = %AssetResult{
          ref: ref,
          stage: 0,
          status: :ok,
          started_at: now,
          finished_at: now,
          duration_ms: 1,
          attempt_count: 1,
          max_attempts: 1
        }

        %{{ref, key} => result}
      else
        %{}
      end

    %Run{
      id: id,
      target_refs: [],
      status: :running,
      submit_kind: :asset,
      replay_mode: :none,
      event_seq: event_seq,
      started_at: now,
      params: params,
      retry_policy: %{max_attempts: 1, delay_ms: 0, retry_on: []},
      node_results: node_results,
      asset_results: %{}
    }
  end
end
