defmodule Favn.PipelineSQLiteSmokeTest do
  use ExUnit.Case, async: false

  alias Favn.Storage.SQLite.Migrations
  alias Favn.Storage.SQLite.Repo
  alias Favn.Test.Fixtures.Assets.Runner.RunnerAssets
  alias Favn.Test.Fixtures.Pipelines.RunnerSlowPipeline

  setup do
    state = Favn.TestSetup.capture_state()

    db_path =
      Path.join(
        System.tmp_dir!(),
        "favn_pipeline_sqlite_#{System.unique_integer([:positive, :monotonic])}.db"
      )

    :ok =
      Favn.TestSetup.configure_storage_adapter(Favn.Storage.Adapter.SQLite,
        database: db_path,
        pool_size: 1
      )

    start_supervised!({Repo, database: db_path, pool_size: 1, busy_timeout: 5_000})
    :ok = Migrations.migrate!(Repo)
    :ok = Favn.TestSetup.setup_asset_modules([RunnerAssets], reload_graph?: true)

    on_exit(fn ->
      Favn.TestSetup.restore_state(state, clear_storage_adapter_env?: true, reload_graph?: true)
      File.rm(db_path)
    end)

    :ok
  end

  test "persists pipeline provenance for run and rerun in sqlite" do
    assert {:ok, run_id} =
             Favn.run_pipeline(RunnerSlowPipeline, params: %{requested_by: "sqlite-smoke"})

    assert {:ok, source_run} = Favn.await_run(run_id, timeout: 5_000)

    assert source_run.pipeline.run_kind == :pipeline
    assert source_run.pipeline.resolved_refs == [{RunnerAssets, :slow_asset}]
    assert source_run.pipeline.deps == :none

    assert {:ok, rerun_id} = Favn.rerun_run(run_id)
    assert {:ok, rerun} = Favn.await_run(rerun_id, timeout: 5_000)

    assert rerun.submit_kind == :rerun
    assert rerun.pipeline.run_kind == :pipeline
    assert rerun.pipeline.resolved_refs == source_run.pipeline.resolved_refs
  end

  test "persists backfill pipeline provenance in sqlite" do
    Code.compile_string("""
    defmodule SQLiteBackfillPipelineAssets do
      use Favn.Assets

      @asset true
      @window Favn.Window.daily()
      def source(_ctx), do: :ok

      @asset true
      @window Favn.Window.daily()
      @depends :source
      def target(_ctx), do: :ok
    end

    defmodule SQLiteBackfillPipeline do
      use Favn.Pipeline

      pipeline :sqlite_backfill_pipeline do
        asset {SQLiteBackfillPipelineAssets, :target}
      end
    end
    """)

    :ok = Favn.TestSetup.setup_asset_modules([SQLiteBackfillPipelineAssets], reload_graph?: true)

    range = %{
      kind: :day,
      start_at: DateTime.from_naive!(~N[2025-02-01 00:00:00], "Etc/UTC"),
      end_at: DateTime.from_naive!(~N[2025-02-04 00:00:00], "Etc/UTC"),
      timezone: "Etc/UTC"
    }

    assert {:ok, run_id} = Favn.backfill_pipeline(SQLiteBackfillPipeline, range: range)
    assert {:ok, run} = Favn.await_run(run_id, timeout: 5_000)

    assert run.submit_kind == :backfill_pipeline
    assert run.pipeline.run_kind == :pipeline_backfill
    assert run.backfill.range == range
    assert run.pipeline.backfill_range == range
    assert length(run.pipeline.anchor_ranges) == 3
  end
end
