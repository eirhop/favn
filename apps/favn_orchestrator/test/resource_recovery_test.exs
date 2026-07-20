defmodule FavnOrchestrator.ResourceRecoveryTest do
  use ExUnit.Case, async: true

  alias Favn.Resource.Ref
  alias FavnOrchestrator.Persistence.Results.ResourceRecoveryCandidate
  alias FavnOrchestrator.ResourceRecovery

  test "recovery run identity is deterministic across claim order" do
    resource = Ref.new!(:connection, :warehouse)

    first = candidate("candidate-a", resource)
    second = candidate("candidate-b", resource)

    left = ResourceRecovery.recovery_run_id("workspace", "source", [first, second], resource)
    right = ResourceRecovery.recovery_run_id("workspace", "source", [second, first], resource)

    assert left == right
    assert String.starts_with?(left, "resource-recovery-")
  end

  defp candidate(id, resource) do
    %ResourceRecoveryCandidate{
      candidate_id: id,
      source_run_id: "source",
      node_key: {{__MODULE__, :asset}, id},
      resource: resource,
      reason: :blocked
    }
  end
end
