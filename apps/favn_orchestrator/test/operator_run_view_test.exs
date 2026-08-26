defmodule FavnOrchestrator.OperatorRunViewTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.OperatorRunView
  alias FavnOrchestrator.Persistence.Results.RunFlowCandidate
  alias FavnOrchestrator.Persistence.Results.RunFlowSnapshot
  alias FavnOrchestrator.Persistence.Results.RunSubmission
  alias FavnOrchestrator.Persistence.Results.RunViewHeader

  test "orders storage-selected attempts without carrying detail payloads" do
    observed = [
      %RunFlowCandidate{
        run_id: "run-one",
        asset_step_id: "step-total",
        asset_ref: "crm.total",
        window_identity: "window-one",
        status: :queued,
        stage: 1
      },
      %RunFlowCandidate{
        run_id: "run-one",
        asset_step_id: "step-orders",
        asset_ref: "crm.orders",
        window_identity: "window-one",
        status: :running,
        started_at: ~U[2026-08-23 10:00:00Z],
        stage: 0
      }
    ]

    assert [orders, total] = OperatorRunView.public_assets(observed)
    assert %{id: "step-orders", state: :running, stage: 0} = orders
    assert %{id: "step-total", state: :queued, stage: 1} = total

    # A queued step is a real attempt with no start time, not a placeholder.
    assert total.started_at == nil

    refute Map.has_key?(Map.from_struct(orders), :output_metadata)
    refute Map.has_key?(Map.from_struct(orders), :error)
  end

  test "submission projection remains available while exact run state is not admitted" do
    submission =
      struct(RunSubmission,
        workspace_id: "workspace-one",
        run_id: "run-one",
        target_kind: "pipeline",
        target_id: "crm.daily",
        status: :queued,
        attempt: 0,
        enqueued_at: ~U[2026-08-23 10:00:00Z],
        updated_at: ~U[2026-08-23 10:00:00Z]
      )

    assert %{status_label: "Queued", status_tone: :info, active?: true} =
             OperatorRunView.project_submission(submission)
  end

  for count <- [0, 90, 1_000, 1_001] do
    test "Flow returns a bounded lean list for #{count} attempts" do
      count = unquote(count)

      observed =
        if count == 0 do
          []
        else
          Enum.map(1..count, fn index ->
            padded = String.pad_leading(Integer.to_string(index), 4, "0")

            %RunFlowCandidate{
              run_id: "run-one",
              asset_step_id: "step-#{padded}",
              asset_ref: "crm.asset_#{padded}",
              window_identity: "none",
              status: :queued
            }
          end)
        end

      flow =
        OperatorRunView.from_snapshot(%RunFlowSnapshot{
          header: header(count),
          observed: observed,
          overflow?: count > 1_000
        })

      assert length(flow.assets) == min(count, 1_000)
      assert flow.overflow? == count > 1_000
      assert Enum.all?(flow.assets, &(&1.state == :queued))
    end
  end

  defp header(count) do
    %RunViewHeader{
      run_id: "run-one",
      root_run_id: "run-one",
      status: :pending,
      started_at: ~U[2026-08-23 10:00:00Z],
      updated_at: ~U[2026-08-23 10:00:00Z],
      counts: %{
        total: count,
        completed: 0,
        succeeded: 0,
        skipped: 0,
        failed: 0,
        running: 0,
        queued: count,
        planned: 0
      }
    }
  end
end
