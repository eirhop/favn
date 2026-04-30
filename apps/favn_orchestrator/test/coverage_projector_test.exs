defmodule FavnOrchestrator.Backfill.CoverageProjectorTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.Storage.Adapter.Memory
  alias FavnOrchestrator.TransitionWriter

  setup do
    Memory.reset()

    on_exit(fn ->
      Memory.reset()
    end)

    :ok
  end

  test "successful full-load pipeline run creates a coverage baseline" do
    run = pipeline_run("run_coverage_full_load")
    coverage_until = ~U[2026-04-28 00:00:00Z]
    coverage_start_at = ~U[2026-04-01 00:00:00Z]

    assert :ok = TransitionWriter.persist_transition(run, :run_created, %{status: :pending})

    terminal =
      RunState.transition(run,
        status: :ok,
        result: %{
          status: :ok,
          metadata: %{
            coverage: %{
              source_key: "orders_api",
              segment_key_hash: "sha256:segment",
              segment_key_redacted: "segment_***",
              coverage_start_at: coverage_start_at,
              coverage_until: coverage_until,
              window_kind: :day,
              timezone: "Etc/UTC",
              coverage_mode: :full_load,
              metadata: %{row_count: 42}
            }
          }
        }
      )

    assert :ok = TransitionWriter.persist_transition(terminal, :run_finished, %{status: :ok})

    assert {:ok, [baseline]} =
             Storage.list_coverage_baselines(pipeline_module: MyApp.Pipelines.Daily)

    assert baseline.baseline_id =~ ~r/^baseline_[0-9a-f]{64}$/
    assert baseline.pipeline_module == MyApp.Pipelines.Daily
    assert baseline.source_key == "orders_api"
    assert baseline.segment_key_hash == "sha256:segment"
    assert baseline.segment_key_redacted == "segment_***"
    assert baseline.coverage_start_at == coverage_start_at
    assert baseline.coverage_until == coverage_until
    assert baseline.window_kind == :day
    assert baseline.timezone == "Etc/UTC"
    assert baseline.created_by_run_id == run.id
    assert baseline.manifest_version_id == run.manifest_version_id
    assert baseline.status == :ok
    assert baseline.metadata == %{row_count: 42, coverage_mode: :full_load}
    assert baseline.created_at == terminal.updated_at
    assert baseline.updated_at == terminal.updated_at
  end

  test "missing coverage metadata does nothing" do
    run = pipeline_run("run_coverage_missing")

    assert :ok = TransitionWriter.persist_transition(run, :run_created, %{status: :pending})

    terminal =
      RunState.transition(run,
        status: :ok,
        result: %{status: :ok, metadata: %{}}
      )

    assert :ok = TransitionWriter.persist_transition(terminal, :run_finished, %{status: :ok})
    assert {:ok, []} = Storage.list_coverage_baselines([])
  end

  test "coverage metadata ISO8601 timestamps are normalized" do
    run = pipeline_run("run_coverage_string_timestamps")

    assert :ok = TransitionWriter.persist_transition(run, :run_created, %{status: :pending})

    terminal =
      RunState.transition(run,
        status: :ok,
        result: %{
          status: :ok,
          metadata: %{
            coverage: %{
              source_key: "orders_api",
              segment_key_hash: "sha256:segment",
              coverage_start_at: "2026-04-01T00:00:00Z",
              coverage_until: "2026-04-28T00:00:00Z",
              window_kind: "daily",
              timezone: "Etc/UTC"
            }
          }
        }
      )

    assert :ok = TransitionWriter.persist_transition(terminal, :run_finished, %{status: :ok})

    assert {:ok, [baseline]} = Storage.list_coverage_baselines(source_key: "orders_api")
    assert baseline.coverage_start_at == ~U[2026-04-01 00:00:00Z]
    assert baseline.coverage_until == ~U[2026-04-28 00:00:00Z]
    assert baseline.window_kind == :day
  end

  test "raw segment or source identity is rejected without failing the run transition" do
    run = pipeline_run("run_coverage_raw_identity")

    assert :ok = TransitionWriter.persist_transition(run, :run_created, %{status: :pending})

    terminal =
      RunState.transition(run,
        status: :ok,
        result: %{
          status: :ok,
          metadata: %{
            coverage: %{
              source_key: "orders_api",
              segment_key_hash: "sha256:segment",
              segment_id: "raw-segment-123",
              coverage_until: ~U[2026-04-28 00:00:00Z],
              window_kind: :day,
              timezone: "Etc/UTC"
            }
          }
        }
      )

    assert :ok = TransitionWriter.persist_transition(terminal, :run_finished, %{status: :ok})
    assert {:ok, stored_run} = Storage.get_run(run.id)
    assert stored_run.status == :ok
    assert {:ok, []} = Storage.list_coverage_baselines([])
  end

  test "lookup and list filters find projected baselines" do
    run = pipeline_run("run_coverage_lookup")

    assert :ok = TransitionWriter.persist_transition(run, :run_created, %{status: :pending})

    terminal =
      RunState.transition(run,
        status: :ok,
        result: %{
          status: :ok,
          metadata: %{
            coverage: %{
              source_key: "orders_api",
              segment_key_hash: "sha256:segment",
              coverage_until: ~U[2026-04-28 00:00:00Z],
              window_kind: :day,
              timezone: "Etc/UTC",
              status: :ok
            }
          }
        }
      )

    assert :ok = TransitionWriter.persist_transition(terminal, :run_finished, %{status: :ok})
    assert {:ok, [baseline]} = Storage.list_coverage_baselines(source_key: "orders_api")
    assert {:ok, ^baseline} = Storage.get_coverage_baseline(baseline.baseline_id)

    assert {:ok, [^baseline]} =
             Storage.list_coverage_baselines(
               pipeline_module: MyApp.Pipelines.Daily,
               status: :ok,
               source_key: "orders_api",
               segment_key_hash: "sha256:segment"
             )
  end

  defp pipeline_run(run_id) do
    RunState.new(
      id: run_id,
      manifest_version_id: "mv_coverage_projector",
      manifest_content_hash: "hash_coverage_projector",
      asset_ref: {MyApp.Assets.Gold, :asset},
      target_refs: [{MyApp.Assets.Gold, :asset}],
      trigger: %{kind: :pipeline},
      metadata: %{pipeline_submit_ref: MyApp.Pipelines.Daily},
      submit_kind: :pipeline
    )
  end
end
