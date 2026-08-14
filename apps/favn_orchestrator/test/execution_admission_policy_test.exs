defmodule FavnOrchestrator.ExecutionAdmissionPolicyTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.ExecutionAdmission
  alias FavnOrchestrator.RunState

  test "derives pool and global limits only from the run's deployment snapshot" do
    run = run_state(policy())

    assert [
             %{kind: :pool, key: "partner_api", limit: 3},
             %{kind: :global, key: "global", limit: 8}
           ] =
             ExecutionAdmission.admission_scopes(run, %{
               asset_step_id: "asset:partner",
               execution_pool: :partner_api
             })
  end

  test "an unknown or malformed snapshotted pool still fails safely" do
    assert {:error, {:unknown_execution_pool, :missing}} =
             ExecutionAdmission.acquire(run_state(policy()), %{
               asset_step_id: "asset:missing",
               execution_pool: :missing
             })

    assert {:error, {:unknown_execution_pool, :partner_api}} =
             ExecutionAdmission.acquire(run_state(%{"partner_api" => %{}}), %{
               asset_step_id: "asset:invalid-policy",
               execution_pool: :partner_api
             })
  end

  defp policy do
    %{
      "global" => %{"max_concurrency" => 8, "circuit_breaker" => nil},
      "partner_api" => %{"max_concurrency" => 3, "circuit_breaker" => nil}
    }
  end

  defp run_state(policy) do
    RunState.new(
      id: "run-execution-pool-policy",
      workspace_id: "workspace-execution-pool-policy",
      deployment_id: "deployment-execution-pool-policy",
      manifest_version_id: "manifest-execution-pool-policy",
      manifest_content_hash: "sha256:execution-pool-policy",
      runner_releases: %{"default" => FavnTestSupport.runner_release_id()},
      asset_ref: {__MODULE__.Asset, :asset},
      target_refs: [{__MODULE__.Asset, :asset}],
      metadata: %{execution_pool_policy: policy}
    )
    |> RunState.transition(status: :running)
  end
end
