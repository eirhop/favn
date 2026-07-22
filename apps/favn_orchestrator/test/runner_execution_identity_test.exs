defmodule FavnOrchestrator.RunnerExecutionIdentityTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Persistence.Identity
  alias FavnOrchestrator.RunExecutionOwnership
  alias FavnOrchestrator.RunnerExecutionIdentity
  alias FavnOrchestrator.RunState

  test "bounds maximum-length valid identity contributions" do
    id =
      RunnerExecutionIdentity.build(
        String.duplicate("r", Identity.max_bytes()),
        String.duplicate("s", Identity.max_bytes()),
        1
      )

    assert Identity.valid?(id)
  end

  test "is deterministic and distinguishes every identity contribution" do
    identity = RunnerExecutionIdentity.build("run", "asset-step", 1)

    assert RunnerExecutionIdentity.build("run", "asset-step", 1) == identity
    refute RunnerExecutionIdentity.build("other-run", "asset-step", 1) == identity
    refute RunnerExecutionIdentity.build("run", "other-step", 1) == identity
    refute RunnerExecutionIdentity.build("run", "asset-step", 2) == identity
  end

  test "rejects oversized and mismatched identities returned by runner clients" do
    run =
      RunState.new(
        id: "run",
        manifest_version_id: "manifest",
        manifest_content_hash: "hash",
        required_runner_release_id: FavnTestSupport.runner_release_id(),
        asset_ref: {__MODULE__, :asset}
      )

    ownership = RunExecutionOwnership.new(run, asset_step_id: "step", attempt: 1)

    assert {:error, oversized} =
             RunExecutionOwnership.validate_runner_execution_id(
               ownership,
               String.duplicate("x", Identity.max_bytes() + 1)
             )

    assert oversized.details == %{
             field: :runner_execution_id,
             actual_bytes: 256,
             max_bytes: 255
           }

    assert {:error, mismatched} =
             RunExecutionOwnership.validate_runner_execution_id(ownership, "different-id")

    assert mismatched.details == %{
             field: :runner_execution_id,
             actual_bytes: 12,
             max_bytes: 255
           }

    assert :ok =
             RunExecutionOwnership.validate_runner_execution_id(
               ownership,
               ownership.dispatch_id
             )
  end
end
