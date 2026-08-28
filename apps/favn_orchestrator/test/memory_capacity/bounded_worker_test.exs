defmodule FavnOrchestrator.MemoryCapacity.BoundedWorkerTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.MemoryCapacity.BoundedWorker

  test "timeout kills the worker and drains its result and monitor messages" do
    assert {:error, :worker_timeout} =
             BoundedWorker.run(
               fn ->
                 receive do
                   :never -> :ok
                 end
               end,
               16 * 1_024 * 1_024,
               timeout: 1
             )

    refute_receive {{BoundedWorker, _pid}, _result}
    refute_receive {:DOWN, _reference, :process, _pid, _reason}
  end

  test "serialized result is decoded only after its bounded worker exits" do
    assert {:ok, %{value: "bounded"}} =
             BoundedWorker.run_serialized(
               fn -> {:ok, %{value: "bounded"}} end,
               16 * 1_024 * 1_024,
               8 * 1_024 * 1_024
             )

    refute_receive {{BoundedWorker, _pid}, _result}
    refute_receive {:DOWN, _reference, :process, _pid, _reason}
  end

  test "serialized result rejects a caller copy above its retained budget" do
    assert {:error, :manifest_memory_budget_exceeded} =
             BoundedWorker.run_serialized(
               fn -> String.duplicate("x", 1_024) end,
               16 * 1_024 * 1_024,
               1
             )
  end
end
