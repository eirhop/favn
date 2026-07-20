defmodule FavnOrchestrator.RunReadModel.OperatorRunOverviewTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Persistence.Results.AssetAttemptOverview
  alias FavnOrchestrator.Persistence.Results.ExecutionGroupOverview
  alias FavnOrchestrator.Persistence.Results.OperatorRunOverview
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
      timezone: "Etc/UTC"
    }

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
        }
      ],
      attempt_counts: %{
        total: 1,
        completed: 1,
        failed: 0,
        running: 0,
        queued: 0,
        effective_windows: 1
      },
      attempts_truncated?: false,
      runs_truncated?: true,
      target_refs: ["MyApp.Gold:orders"]
    }

    detail = RunReadModel.from_operator_run_overview(projection)

    assert detail.summary.requested_window_counts == %{total: 1, completed: 1, failed: 0}
    assert detail.summary.effective_window_count == 1
    assert detail.summary.progress.label == "1/1 requested windows complete"
    assert detail.requested_windows == []
    assert detail.windows == [effective_window]
    assert [attempt] = detail.asset_attempts
    assert attempt.id == "child:gold-expanded"
    assert attempt.asset_step_id == "gold-expanded"
    assert detail.child_run_details_truncated?
    refute Map.has_key?(detail, :events)
  end

  defp run(run_id, root_run_id, submit_kind) do
    %RunSummary{
      workspace_id: "workspace",
      run_id: run_id,
      root_run_id: root_run_id,
      status: :ok,
      event_sequence: 6,
      submit_kind: submit_kind,
      manifest_version_id: "manifest-v1",
      inserted_at: @started_at,
      updated_at: @finished_at,
      terminal_at: @finished_at
    }
  end
end
