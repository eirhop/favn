defmodule FavnOrchestrator.RunReadModel.OperatorRunOverviewTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Persistence.Results.AssetAttemptOverview
  alias FavnOrchestrator.Persistence.Results.BackfillWindow
  alias FavnOrchestrator.Persistence.Results.ExecutionGroupOverview
  alias FavnOrchestrator.Persistence.Results.OperatorRunOverview
  alias FavnOrchestrator.Persistence.Results.PlannedAssetStep
  alias FavnOrchestrator.Persistence.Results.OperatorRunOverviewNormalizer
  alias FavnOrchestrator.Persistence.Results.RunSummary
  alias FavnOrchestrator.RunReadModel

  @started_at ~U[2026-07-20 10:00:00Z]
  @finished_at ~U[2026-07-20 10:05:00Z]

  test "keeps requested counts separate from effective runtime windows" do
    effective_window = %{
      key: "runtime:expanded",
      label: "Jun 24 – Jul 2",
      kind: :day,
      start_at: ~U[2026-06-24 00:00:00Z],
      end_at: ~U[2026-07-02 00:00:00Z],
      timezone: "Europe/Oslo"
    }

    july_window = %{
      key: "runtime:july",
      label: "Jul 2026",
      kind: :month,
      start_at: ~U[2026-07-01 00:00:00Z],
      end_at: ~U[2026-08-01 00:00:00Z],
      timezone: "Europe/Oslo"
    }

    oslo_effective_window = shift_window(effective_window, "Europe/Oslo")
    oslo_july_window = shift_window(july_window, "Europe/Oslo")

    projection = %OperatorRunOverview{
      overview: %ExecutionGroupOverview{
        workspace_id: "workspace",
        root_run_id: "root",
        status: :succeeded,
        run_count: 2,
        pending_count: 0,
        running_count: 0,
        succeeded_count: 2,
        failed_count: 0,
        latest_event_id: 12,
        started_at: @started_at,
        finished_at: @finished_at,
        updated_at: @finished_at
      },
      root_run: run("root", "root", :backfill_pipeline),
      runs: [run("root", "root", :backfill_pipeline), run("child", "root", :pipeline)],
      requested_windows: [],
      requested_windows_truncated?: false,
      requested_window_counts: %{total: 1, completed: 1, failed: 0},
      attempts: [
        %AssetAttemptOverview{
          workspace_id: "workspace",
          root_run_id: "root",
          run_id: "child",
          asset_step_id: "gold-expanded",
          asset_ref: "MyApp.Gold:orders",
          window_identity: effective_window.key,
          window: effective_window,
          status: :ok,
          started_at: @started_at,
          finished_at: @finished_at
        },
        %AssetAttemptOverview{
          workspace_id: "workspace",
          root_run_id: "root",
          run_id: "child",
          asset_step_id: "gold-july",
          asset_ref: "MyApp.Gold:orders",
          window_identity: july_window.key,
          window: july_window,
          status: :skipped_fresh,
          started_at: @started_at,
          finished_at: @started_at
        }
      ],
      planned_steps: [
        %PlannedAssetStep{
          root_run_id: "root",
          run_id: "child",
          node_identity: "already-attempted",
          asset_ref: "MyApp.Gold:orders",
          window_identity: "persisted-plan-key",
          window: %{oslo_effective_window | key: "plan:june"},
          stage: 0
        },
        %PlannedAssetStep{
          root_run_id: "root",
          run_id: "child",
          node_identity: "already-skipped",
          asset_ref: "MyApp.Gold:orders",
          window_identity: "persisted-july-key",
          window: %{oslo_july_window | key: "plan:july"},
          stage: 0
        },
        %PlannedAssetStep{
          root_run_id: "root",
          run_id: "child",
          node_identity: "waiting",
          asset_ref: "MyApp.Silver:orders",
          window_identity: "none",
          window: nil,
          stage: 0
        }
      ],
      planned_steps_truncated?: false,
      asset_counts_by_run: %{
        "child" => %{
          total: 3,
          completed: 2,
          succeeded: 1,
          skipped: 1,
          failed: 0,
          running: 0,
          queued: 0,
          planned: 1
        }
      },
      attempt_counts: %{
        total: 3,
        completed: 2,
        succeeded: 1,
        skipped: 1,
        failed: 0,
        running: 0,
        queued: 0,
        planned: 1,
        effective_windows: 2
      },
      attempts_truncated?: false,
      runs_truncated?: true,
      target_refs: ["MyApp.Gold:orders"]
    }

    detail = RunReadModel.from_operator_run_overview(projection)

    assert detail.summary.requested_window_counts == %{total: 1, completed: 1, failed: 0}
    assert detail.summary.effective_window_count == 2
    assert detail.summary.succeeded_asset_attempts == 1
    assert detail.summary.skipped_asset_attempts == 1
    assert detail.summary.planned_asset_attempts == 1
    assert detail.summary.progress.label == "1/1 requested windows complete"
    assert detail.summary.started_at == @started_at
    assert detail.summary.finished_at == @finished_at
    assert detail.summary.duration_ms == 300_000
    assert detail.requested_windows == []
    assert detail.windows == [effective_window, july_window]
    assert detail.has_non_windowed_assets?
    assert [attempt, skipped, planned] = detail.asset_attempts
    assert attempt.id == "child:gold-expanded"
    assert attempt.asset_step_id == "gold-expanded"
    assert skipped.id == "child:gold-july"
    assert planned.id == "planned:child:waiting"
    assert planned.status == :planned
    assert Enum.count(detail.asset_attempts, &(&1.asset_ref == "MyApp.Gold:orders")) == 2
    assert detail.child_run_details_truncated?
    assert [%{asset_counts: %{total: 3, skipped: 1, planned: 1}}] = detail.child_runs

    truncated_detail =
      projection
      |> Map.put(:attempts_truncated?, true)
      |> RunReadModel.from_operator_run_overview()

    assert Enum.all?(truncated_detail.asset_attempts, &(&1.status != :planned))
    assert truncated_detail.windows == [effective_window, july_window]

    assert detail.root_run.runner_releases == %{
             "default" => FavnTestSupport.runner_release_id()
           }

    assert Enum.all?(detail.child_runs, fn run ->
             run.runner_releases == %{"default" => FavnTestSupport.runner_release_id()}
           end)

    refute Map.has_key?(detail, :events)
  end

  test "accepts empty and named runner release maps" do
    named_projection =
      projection_with_runner_releases(%{"default" => FavnTestSupport.runner_release_id()})

    empty_projection = projection_with_runner_releases(%{})

    named_detail = RunReadModel.from_operator_run_overview(named_projection)
    empty_detail = RunReadModel.from_operator_run_overview(empty_projection)

    assert named_detail.root_run.runner_releases == %{
             "default" => FavnTestSupport.runner_release_id()
           }

    assert empty_detail.root_run.runner_releases == %{}
  end

  test "normalizes persisted failed windows to the public error status" do
    projection = projection_with_runner_releases(%{})

    failed_window = %BackfillWindow{
      workspace_id: "workspace",
      backfill_id: "root",
      window_id: "window-1",
      window_key: "2026-07-20",
      window_start: @started_at,
      window_end: @finished_at,
      status: :failed,
      run_id: "child",
      attempt_count: 1,
      last_error: %{"message" => "failed"},
      payload: %{},
      version: 1
    }

    projection = %{
      projection
      | requested_windows: [failed_window],
        requested_window_counts: %{total: 1, completed: 1, failed: 1}
    }

    detail = RunReadModel.from_operator_run_overview(projection)
    assert [%{status: :error}] = detail.backfill_failures
  end

  test "canonicalizes complete attempt windows and rejects incomplete attempt or planned maps" do
    projection = projection_with_runner_releases(%{})

    persisted_window = %{
      "key" => "runtime:expanded",
      "label" => "Jul 20",
      "kind" => "day",
      "start_at" => @started_at,
      "end_at" => @finished_at,
      "timezone" => "Etc/UTC"
    }

    projection = %{projection | attempts: [attempt(persisted_window)]}
    detail = RunReadModel.from_operator_run_overview(projection)

    assert [window] = detail.windows

    assert window == %{
             key: "runtime:expanded",
             label: "Jul 20",
             kind: :day,
             start_at: @started_at,
             end_at: @finished_at,
             timezone: "Etc/UTC"
           }

    malformed = %{projection | attempts: [attempt(%{})]}

    assert {:error, :invalid_operator_run_overview} =
             RunReadModel.from_operator_run_overview(malformed)

    malformed_planned = %{
      projection
      | attempts: [],
        planned_steps: [
          %PlannedAssetStep{
            root_run_id: "root",
            run_id: "root",
            node_identity: "gold-expanded",
            asset_ref: "MyApp.Gold:orders",
            window_identity: "runtime:expanded",
            window: %{},
            stage: 0,
            execution_pool: "default"
          }
        ]
    }

    assert {:error, :invalid_operator_run_overview} =
             RunReadModel.from_operator_run_overview(malformed_planned)
  end

  test "rejects runner release maps above the canonical pool bound" do
    release_id = FavnTestSupport.runner_release_id()
    runner_releases = Map.new(1..65, &{"pool-#{&1}", release_id})
    projection = projection_with_runner_releases(runner_releases)

    assert {:error, :invalid_operator_run_overview} =
             RunReadModel.from_operator_run_overview(projection)
  end

  test "rejects malformed persisted overview status, list, and timestamp fields" do
    projection = projection_with_runner_releases(%{})

    assert {:error, :invalid_operator_run_overview} =
             OperatorRunOverviewNormalizer.normalize_operator_overview(%{
               projection
               | root_run: %{projection.root_run | status: :unknown}
             })

    assert {:error, :invalid_operator_run_overview} =
             OperatorRunOverviewNormalizer.normalize_operator_overview(%{
               projection
               | runs: [:bad]
             })

    assert {:error, :invalid_operator_run_overview} =
             OperatorRunOverviewNormalizer.normalize_operator_overview(%{
               projection
               | root_run: %{projection.root_run | inserted_at: "not-a-timestamp"}
             })
  end

  defp projection_with_runner_releases(runner_releases) do
    %OperatorRunOverview{
      overview: %ExecutionGroupOverview{
        workspace_id: "workspace",
        root_run_id: "root",
        status: :succeeded,
        run_count: 1,
        pending_count: 0,
        running_count: 0,
        succeeded_count: 1,
        failed_count: 0,
        latest_event_id: 12,
        updated_at: @finished_at
      },
      root_run: run("root", "root", :pipeline, runner_releases),
      runs: [run("root", "root", :pipeline, runner_releases)],
      requested_windows: [],
      requested_windows_truncated?: false,
      requested_window_counts: %{total: 0, completed: 0, failed: 0},
      attempts: [],
      attempt_counts: %{
        total: 0,
        completed: 0,
        succeeded: 0,
        skipped: 0,
        failed: 0,
        running: 0,
        queued: 0,
        planned: 0,
        effective_windows: 0
      },
      attempts_truncated?: false,
      runs_truncated?: false,
      target_refs: []
    }
  end

  defp attempt(window) do
    %AssetAttemptOverview{
      workspace_id: "workspace",
      root_run_id: "root",
      run_id: "root",
      asset_step_id: "gold-expanded",
      asset_ref: "MyApp.Gold:orders",
      window_identity: "runtime:expanded",
      window: window,
      status: :ok,
      started_at: @started_at,
      finished_at: @finished_at
    }
  end

  defp run(
         run_id,
         root_run_id,
         submit_kind,
         runner_releases \\ %{
           "default" => FavnTestSupport.runner_release_id()
         }
       ) do
    %RunSummary{
      workspace_id: "workspace",
      run_id: run_id,
      root_run_id: root_run_id,
      deployment_id: "deployment-v1",
      status: :ok,
      event_sequence: 6,
      submit_kind: submit_kind,
      trigger_type: :manual,
      manifest_version_id: "manifest-v1",
      runner_releases: runner_releases,
      submitted_event_id: 5,
      latest_event_id: 6,
      inserted_at: @started_at,
      updated_at: @finished_at,
      terminal_at: @finished_at
    }
  end

  defp shift_window(window, timezone) do
    database = Favn.Timezone.database!()

    window
    |> Map.update!(:start_at, &DateTime.shift_zone!(&1, timezone, database))
    |> Map.update!(:end_at, &DateTime.shift_zone!(&1, timezone, database))
  end
end
