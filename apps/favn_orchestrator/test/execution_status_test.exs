defmodule FavnOrchestrator.ExecutionStatusTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.ExecutionStatus

  test "normalizes only known persisted status names" do
    assert ExecutionStatus.normalize("running") == :running
    assert ExecutionStatus.normalize(:ok) == :ok
    assert ExecutionStatus.normalize("not-a-status") == "not-a-status"
  end

  test "classifies atom and string forms consistently" do
    assert ExecutionStatus.terminal?(:skipped_fresh)
    assert ExecutionStatus.terminal?("skipped_fresh")
    assert ExecutionStatus.failed?("timed_out")
    assert ExecutionStatus.running?("retrying")
    assert ExecutionStatus.queued?(nil)
    assert ExecutionStatus.active?("pending")
    refute ExecutionStatus.active?(:ok)
  end
end
