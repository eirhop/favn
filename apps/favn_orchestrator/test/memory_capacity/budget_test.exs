defmodule FavnOrchestrator.MemoryCapacity.BudgetTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.MemoryCapacity.Budget

  @mib 1_024 * 1_024

  test "live index terms clamp to the worker ceiling without widening persisted byte limits" do
    assert Budget.index(65 * @mib) == Budget.index_max()
    assert {:error, :manifest_memory_budget_exceeded} = Budget.persisted_index(65 * @mib)
  end

  test "persisted run decode budget covers amplification and serialized handoff" do
    assert {:ok, first} = Budget.run_decode(1 * @mib, 512 * @mib)
    assert first == %{result_bytes: 64 * @mib, working_bytes: 80 * @mib}

    assert {:ok, second} = Budget.run_decode(16 * @mib, 512 * @mib)
    assert second == %{result_bytes: 256 * @mib, working_bytes: 320 * @mib}
  end

  test "persisted run decode rejects work above the retained run-plan ceiling" do
    assert {:error, :manifest_memory_budget_exceeded} =
             Budget.run_decode(33 * @mib, 512 * @mib)
  end
end
