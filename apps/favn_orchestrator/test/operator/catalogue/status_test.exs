defmodule FavnOrchestrator.Operator.Catalogue.StatusTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Operator.Catalogue.Status
  alias FavnOrchestrator.Persistence.Results.TargetStatus

  @now ~U[2026-07-20 10:00:00Z]

  test "normalizes persisted run statuses without losing the raw latest status" do
    for {run_status, catalogue_status} <- [
          ok: :healthy,
          skipped_fresh: :healthy,
          queued: :running,
          retrying: :running,
          error: :failed,
          blocked: :failed,
          timed_out: :failed
        ] do
      projected =
        Status.put(%{}, %TargetStatus{
          workspace_id: "workspace",
          deployment_id: "deployment",
          target_kind: :asset,
          target_id: "asset:gold",
          status: run_status,
          run_id: "run-#{run_status}",
          updated_at: @now
        })

      assert projected.status == catalogue_status
      assert projected.latest_run_status == run_status
      assert projected.latest_run_id == "run-#{run_status}"
    end
  end
end
