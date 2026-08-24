defmodule FavnOrchestrator.OperatorRunViewTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.OperatorRunView
  alias FavnOrchestrator.Persistence.Results.RunFlowCandidate
  alias FavnOrchestrator.Persistence.Results.RunFlowSnapshot
  alias FavnOrchestrator.Persistence.Results.RunSubmission
  alias FavnOrchestrator.Persistence.Results.RunViewHeader

  test "combines storage-selected observed and planned rows without carrying detail payloads" do
    planned = [
      %RunFlowCandidate{
        run_id: "run-one",
        planned_id: "planned-total",
        node_key: %{"ref" => "crm.total"},
        asset_ref: "crm.total",
        window_identity: "window-one",
        status: :planned
      }
    ]

    observed = [
      %RunFlowCandidate{
        run_id: "run-one",
        asset_step_id: "step-orders",
        asset_ref: "crm.orders",
        window_identity: "window-one",
        status: :running,
        started_at: ~U[2026-08-23 10:00:00Z]
      }
    ]

    assert [orders, total] = OperatorRunView.merge_assets(planned, observed)
    assert %{id: "step-orders", state: :running, detail?: true} = orders
    assert %{id: "planned-total", state: :planned, detail?: false} = total
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
    test "Flow returns a bounded lean list for #{count} planned assets" do
      count = unquote(count)

      planned =
        if count == 0 do
          []
        else
          Enum.map(1..count, fn index ->
            padded = String.pad_leading(Integer.to_string(index), 4, "0")

            %RunFlowCandidate{
              run_id: "run-one",
              planned_id: "planned-#{padded}",
              node_key: %{"ordinal" => index},
              asset_ref: "crm.asset_#{padded}",
              window_identity: "none",
              status: :planned
            }
          end)
        end

      flow =
        OperatorRunView.from_snapshot(%RunFlowSnapshot{
          header: header(count),
          planned: planned,
          observed: [],
          overflow?: count > 1_000
        })

      assert length(flow.assets) == min(count, 1_000)
      assert flow.overflow? == count > 1_000
      assert Enum.all?(flow.assets, &(&1.state == :planned and not &1.detail?))
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
        queued: 0,
        planned: count
      }
    }
  end
end
