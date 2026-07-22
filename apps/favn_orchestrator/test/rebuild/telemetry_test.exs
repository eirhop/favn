defmodule FavnOrchestrator.Rebuild.TelemetryTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Rebuild.Plan
  alias FavnOrchestrator.Rebuild.Telemetry

  test "plan telemetry reports bounded outcome and size measurements" do
    handler_id = "rebuild-plan-telemetry-#{System.unique_integer([:positive])}"
    test_pid = self()

    :ok =
      :telemetry.attach(
        handler_id,
        [:favn, :orchestrator, :rebuild, :plan],
        fn event, measurements, metadata, _config ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    plan = %Plan{
      plan_id: "plan-1",
      plan_hash: String.duplicate("a", 64),
      expires_at: ~U[2026-07-22 14:00:00Z],
      payload: %{actions: [%{}], item_count: 3, window_count: 2}
    }

    assert {:ok, ^plan} =
             Telemetry.plan(%{workspace_id: "workspace-1"}, "asset:orders", fn ->
               {:ok, plan}
             end)

    assert_received {:telemetry, [:favn, :orchestrator, :rebuild, :plan], measurements, metadata}

    assert measurements.action_count == 1
    assert measurements.item_count == 3
    assert measurements.window_count == 2
    assert is_integer(measurements.duration)
    assert metadata.outcome == :ok
    assert metadata.workspace_id == "workspace-1"
    assert metadata.target_id == "asset:orders"
  end
end
