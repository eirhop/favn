defmodule FavnOrchestrator.RunReadModel.ExecutionGroupOverviewTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Persistence.Results.ExecutionGroupOverview
  alias FavnOrchestrator.RunReadModel

  test "expands an active compact overview into the complete public summary" do
    updated_at = DateTime.utc_now()

    overview = %ExecutionGroupOverview{
      workspace_id: "workspace-1",
      root_run_id: "run-1",
      status: :running,
      run_count: 3,
      pending_count: 1,
      running_count: 1,
      succeeded_count: 1,
      failed_count: 0,
      latest_event_id: 10,
      source_publication_id: 10,
      updated_at: updated_at
    }

    summary = RunReadModel.from_execution_group_overview(overview)

    assert summary.id == "run-1"
    assert summary.root_execution_group_id == "run-1"
    assert summary.status == :running
    assert summary.health == :active
    assert summary.active?
    assert summary.completed_asset_attempts == 1
    assert summary.succeeded_asset_attempts == 1
    assert summary.skipped_asset_attempts == 0
    assert summary.planned_asset_attempts == 0
    assert summary.summary_totals.asset_attempts.queued == 1
    assert summary.progress.label == "1 / 3 asset attempts"
    assert summary.last_activity_at == updated_at
  end

  test "normalizes a failed compact overview and counts all terminal attempts as completed" do
    updated_at = DateTime.utc_now()

    overview = %ExecutionGroupOverview{
      workspace_id: "workspace-1",
      root_run_id: "run-2",
      status: :failed,
      run_count: 2,
      pending_count: 0,
      running_count: 0,
      succeeded_count: 1,
      failed_count: 1,
      latest_event_id: 20,
      source_publication_id: 20,
      updated_at: updated_at
    }

    summary = RunReadModel.from_execution_group_overview(overview)

    assert summary.status == :error
    assert summary.root_status == :error
    assert summary.health == :error
    refute summary.active?
    assert summary.completed_asset_attempts == 2
    assert summary.failure_count == 1
    assert summary.finished_at == updated_at
  end

  test "uses backfill ledger status, window counts, and child asset attempts for a parent" do
    updated_at = DateTime.utc_now()

    overview = %ExecutionGroupOverview{
      workspace_id: "workspace-1",
      root_run_id: "run-parent",
      status: :succeeded,
      run_count: 1,
      pending_count: 0,
      running_count: 0,
      succeeded_count: 1,
      failed_count: 0,
      latest_event_id: 30,
      source_publication_id: 30,
      updated_at: updated_at,
      backfill_status: :running,
      asset_counts: %{
        total: 5,
        completed: 2,
        succeeded: 1,
        skipped: 1,
        failed: 0,
        running: 1,
        queued: 1,
        planned: 1
      },
      window_counts: %{
        total: 3,
        planned: 0,
        ready: 1,
        active: 1,
        succeeded: 1,
        failed: 0,
        cancelled: 0
      }
    }

    summary = RunReadModel.from_execution_group_overview(overview)

    assert summary.status == :running
    assert summary.root_status == :ok
    assert summary.active?
    assert summary.total_windows == 3
    assert summary.completed_windows == 1
    assert summary.running_asset_attempts == 1
    assert summary.queued_asset_attempts == 1
    assert summary.skipped_asset_attempts == 1
    assert summary.planned_asset_attempts == 1
    assert summary.summary_totals.windows.running == 1
    assert is_nil(summary.duration_ms)
  end

  test "distinguishes cancelled and partially cancelled terminal backfills" do
    base = %ExecutionGroupOverview{
      workspace_id: "workspace-1",
      root_run_id: "run-parent",
      status: :succeeded,
      run_count: 3,
      pending_count: 0,
      running_count: 0,
      succeeded_count: 3,
      failed_count: 0,
      latest_event_id: 40,
      source_publication_id: 40,
      updated_at: DateTime.utc_now(),
      backfill_status: :completed
    }

    all_cancelled =
      RunReadModel.from_execution_group_overview(%{
        base
        | window_counts: %{
            total: 2,
            planned: 0,
            ready: 0,
            active: 0,
            succeeded: 0,
            failed: 0,
            cancelled: 2
          }
      })

    mixed =
      RunReadModel.from_execution_group_overview(%{
        base
        | window_counts: %{
            total: 2,
            planned: 0,
            ready: 0,
            active: 0,
            succeeded: 1,
            failed: 0,
            cancelled: 1
          }
      })

    assert all_cancelled.status == :cancelled
    assert all_cancelled.health == :ok
    assert mixed.status == :partial
    assert mixed.health == :warning
  end
end
