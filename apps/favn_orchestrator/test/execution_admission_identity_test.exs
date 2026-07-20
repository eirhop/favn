defmodule FavnOrchestrator.ExecutionAdmission.IdentityTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.ExecutionAdmission.Identity

  test "lease identities are stable within an attempt and distinct across retries" do
    first = Identity.lease_id("run-1", "step-1", 2, 1)

    assert first == Identity.lease_id("run-1", "step-1", 2, 1)
    refute first == Identity.lease_id("run-1", "step-1", 2, 2)
    refute first == Identity.lease_id("run-1", "step-1", 3, 1)
  end
end
