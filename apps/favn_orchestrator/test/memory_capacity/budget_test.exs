defmodule FavnOrchestrator.MemoryCapacity.BudgetTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.MemoryCapacity.Budget

  @mib 1_024 * 1_024

  test "persisted run decode budget covers amplification and serialized handoff" do
    assert {:ok, %{result_bytes: 64 * @mib, working_bytes: 80 * @mib}} =
             Budget.run_decode(1 * @mib, 512 * @mib)

    assert {:ok, %{result_bytes: 256 * @mib, working_bytes: 320 * @mib}} =
             Budget.run_decode(16 * @mib, 512 * @mib)
  end

  test "persisted run decode rejects work above the retained run-plan ceiling" do
    assert {:error, :manifest_memory_budget_exceeded} =
             Budget.run_decode(33 * @mib, 512 * @mib)
  end
end
