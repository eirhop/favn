defmodule FavnOrchestrator.RunManager.PlanCapacityTest do
  use ExUnit.Case, async: true

  alias Favn.Plan
  alias FavnOrchestrator.RunManager.PlanCapacity
  alias FavnOrchestrator.RunState

  test "reserves a conservative decoded-plan budget and releases it by run" do
    run_a = run("run-a", 1_024)
    run_b = run("run-b", 2_048)
    bytes_a = PlanCapacity.allocation_bytes(run_a)
    bytes_b = PlanCapacity.allocation_bytes(run_b)
    capacity = PlanCapacity.new(max_active_run_plan_bytes: bytes_a + bytes_b - 1)

    assert {:ok, reserved} = PlanCapacity.reserve(capacity, {"ws", "run-a"}, run_a)

    assert {:error,
            {:run_plan_capacity_exhausted,
             %{
               required_bytes: ^bytes_b,
               allocated_bytes: ^bytes_a,
               max_bytes: max_bytes
             }}} = PlanCapacity.reserve(reserved, {"ws", "run-b"}, run_b)

    assert max_bytes == bytes_a + bytes_b - 1

    released = PlanCapacity.release(reserved, {"ws", "run-a"})
    assert released.allocated_bytes == 0
    assert {:ok, _reserved} = PlanCapacity.reserve(released, {"ws", "run-b"}, run_b)
  end

  test "rejects a single plan that can never fit on the node" do
    run = run("oversized", 1_024)
    bytes = PlanCapacity.allocation_bytes(run)
    capacity = PlanCapacity.new(max_active_run_plan_bytes: bytes - 1)

    assert {:error, {:run_plan_exceeds_node_capacity, ^bytes, max_bytes}} =
             PlanCapacity.validate_run(capacity, run)

    assert max_bytes == bytes - 1
  end

  defp run(id, payload_bytes) do
    %RunState{
      id: id,
      workspace_id: "ws",
      plan: %Plan{nodes: %{"node" => :binary.copy("x", payload_bytes)}}
    }
  end
end
