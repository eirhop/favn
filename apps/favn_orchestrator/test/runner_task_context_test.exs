Code.require_file("../../favn_test_support/fixtures/runner_task_persistence.exs", __DIR__)

defmodule FavnOrchestrator.RunnerTaskContextTest do
  use ExUnit.Case, async: true
  alias FavnOrchestrator.RunnerTaskContext
  alias FavnOrchestrator.Persistence.Results.MaterializationClaim
  alias FavnOrchestrator.Persistence.Results.ResourceCircuitPermit
  alias FavnTestSupport.RunnerTaskPersistence, as: Fixture

  test "pipeline and sequential continuations preserve claim and permit authority" do
    version = Fixture.version()
    ref = hd(version.manifest.assets).ref

    claim =
      %MaterializationClaim{
        workspace_id: "workspace",
        claim_key: "claim",
        deployment_id: "deployment",
        target_kind: :asset,
        target_id: Favn.TargetIdentity.for_asset(ref),
        evidence_generation_id: "generation",
        partition_key: "latest",
        run_id: "run",
        owner_id: "owner",
        fencing_token: 7,
        status: :claimed,
        expires_at: ~U[2026-09-04 15:00:00Z],
        version: 1
      }
      |> Map.from_struct()

    sequential = %{kind: :sequential, materialization_claim: claim}

    pipeline = %{
      kind: :pipeline,
      materialization_claim: claim,
      decision: %{decision: :run, reason: :forced, node_key: {ref, nil}, freshness_key: "latest"},
      freshness_key: "latest",
      resource_circuit_permits: [
        %ResourceCircuitPermit{
          resource: Favn.Resource.Ref.new!(:connection, "warehouse"),
          owner_id: "permit-owner",
          probe?: true
        }
      ],
      freshness_checkpoint: %{
        version: 1,
        revision: 2,
        sequence: 3,
        stage: 0,
        attempt: 1,
        payload_hash: :crypto.hash(:sha256, "checkpoint")
      }
    }

    for context <- [%{}, %{kind: :sequential, materialization_claim: nil}, sequential, pipeline] do
      assert {:ok, encoded} = RunnerTaskContext.encode(context)
      assert {:ok, ^context} = RunnerTaskContext.decode(encoded, version)
    end

    assert {:error, _} =
             RunnerTaskContext.encode(put_in(sequential.materialization_claim.fencing_token, "7"))

    assert {:error, _} =
             RunnerTaskContext.encode(
               put_in(pipeline.resource_circuit_permits, [
                 %{owner_id: "owner", probe?: "yes", resource: :connection}
               ])
             )

    assert {:error, _} = RunnerTaskContext.encode(Map.put(pipeline, :freshness_context, %{}))
  end

  test "task context identity cannot redirect settlement to another claim or workspace" do
    context = %{
      materialization_claim: %{
        workspace_id: "one",
        run_id: "run",
        claim_key: "claim",
        fencing_token: 3
      }
    }

    command = %{
      workspace_context: %{workspace_id: "one"},
      run_id: "run",
      write_claim_key: "claim",
      write_claim_fence: 3
    }

    assert RunnerTaskContext.matches_task?(context, command)

    refute RunnerTaskContext.matches_task?(
             context,
             put_in(command.workspace_context.workspace_id, "two")
           )

    refute RunnerTaskContext.matches_task?(context, %{command | write_claim_fence: 4})
  end
end
