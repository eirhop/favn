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
end
