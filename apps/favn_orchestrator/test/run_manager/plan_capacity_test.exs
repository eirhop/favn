defmodule FavnOrchestrator.RunManager.PlanCapacityTest do
  use ExUnit.Case, async: false

  alias Favn.Plan
  alias FavnOrchestrator.RunManager.PlanCapacity
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.MemoryCapacity
  alias FavnOrchestrator.MemoryCapacity.Coordinator
  alias FavnOrchestrator.MemoryCapacity.Ledger

  defmodule Provider do
    @gib 1_024 * 1_024 * 1_024

    def snapshot(_opts) do
      {:ok,
       %{
         source: :cgroup_v2,
         limit_bytes: 2 * @gib,
         usage_bytes: 128 * 1_024 * 1_024,
         headroom_bytes: 2 * @gib - 128 * 1_024 * 1_024
       }}
    end
  end

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

  test "resizes an admitted run to its measured retained execution state" do
    run = run("resized", 1_024)
    initial_bytes = PlanCapacity.allocation_bytes(run)
    measured_bytes = PlanCapacity.retained_term_bytes({run, :binary.copy("x", 4_096)})
    capacity = PlanCapacity.new(max_active_run_plan_bytes: measured_bytes)

    assert {:ok, reserved} = PlanCapacity.reserve(capacity, {"ws", run.id}, run)
    assert reserved.allocated_bytes == initial_bytes

    assert {:ok, resized} =
             PlanCapacity.resize(reserved, {"ws", run.id}, measured_bytes)

    assert resized.allocated_bytes == measured_bytes

    assert {:error, {:run_plan_exceeds_node_capacity, required, ^measured_bytes}} =
             PlanCapacity.resize(resized, {"ws", run.id}, measured_bytes + 1)

    assert required == measured_bytes + 1
  end

  test "adopts one transferred construction lease and releases it with the run" do
    ledger = unique_name(:ledger)
    coordinator = unique_name(:coordinator)
    table = unique_name(:table)

    children = [
      %{
        id: :ledger,
        start: {Ledger, :start_link, [[name: ledger, table: table]]},
        restart: :temporary
      },
      %{
        id: :coordinator,
        start:
          {Coordinator, :start_link, [[name: coordinator, ledger: ledger, provider: Provider]]},
        restart: :permanent
      }
    ]

    start_supervised!(%{
      id: unique_name(:supervisor),
      start: {Supervisor, :start_link, [children, [strategy: :rest_for_one]]}
    })

    run = run("transferred", 4_096)
    bytes = PlanCapacity.allocation_bytes(run)
    assert {:ok, token} = MemoryCapacity.acquire(bytes * 2, server: coordinator)
    assert :ok = MemoryCapacity.transfer(token, bytes * 2, 0, server: coordinator)

    capacity =
      PlanCapacity.new(
        max_active_run_plan_bytes: bytes * 2,
        coordinate_memory: true,
        memory_server: coordinator
      )

    assert {:ok, reserved} =
             PlanCapacity.reserve(capacity, {"ws", run.id}, run,
               memory_capacity_token: token,
               transferred_retained_bytes: bytes * 2
             )

    assert MemoryCapacity.diagnostics(server: coordinator).reserved_bytes == bytes * 2

    assert {:ok, settled} = PlanCapacity.resize(reserved, {"ws", run.id}, bytes)
    assert MemoryCapacity.diagnostics(server: coordinator).reserved_bytes == bytes

    released = PlanCapacity.release(settled, {"ws", run.id})
    assert released.allocated_bytes == 0
    assert MemoryCapacity.diagnostics(server: coordinator).active_leases == 0
  end

  defp run(id, payload_bytes) do
    %RunState{
      id: id,
      workspace_id: "ws",
      plan: %Plan{nodes: %{"node" => :binary.copy("x", payload_bytes)}}
    }
  end

  defp unique_name(kind),
    do: String.to_atom("#{__MODULE__}.#{kind}.#{System.unique_integer([:positive])}")
end
