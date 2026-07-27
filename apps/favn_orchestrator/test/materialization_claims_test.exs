defmodule FavnOrchestrator.MaterializationClaimsTest do
  use ExUnit.Case, async: false

  alias Favn.Contracts.RunnerResult
  alias FavnOrchestrator.AssetFreshnessState
  alias FavnOrchestrator.MaterializationClaims
  alias FavnOrchestrator.Persistence.Results.MaterializationDecision
  alias FavnOrchestrator.Persistence.Runtime, as: PersistenceRuntime
  alias FavnOrchestrator.Persistence.Stores

  defmodule Store do
    def finish(command) do
      send(self(), {:finish_materialization, command})

      {:ok,
       %MaterializationDecision{
         claim_key: command.claim_key,
         status: :materialized
       }}
    end
  end

  setup do
    stores =
      struct(Stores,
        registry: Store,
        runs: Store,
        run_ownership: Store,
        scheduler: Store,
        admission: Store,
        resource_circuits: Store,
        target_generations: Store,
        rebuilds: Store,
        target_operation_locks: Store,
        materialization: Store,
        backfills: Store,
        operator_reads: Store,
        logs: Store,
        identity: Store,
        maintenance: Store
      )

    runtime = %PersistenceRuntime{backend: __MODULE__, options: [], stores: stores}
    start_supervised!({PersistenceRuntime, runtime})

    :ok
  end

  test "normalizes structured runtime-input lineage before persistence" do
    generation_id = Ecto.UUID.generate()

    claim = %{
      claim_key: "claim-runtime-input-lineage",
      workspace_id: "workspace-runtime-input-lineage",
      run_id: "run-runtime-input-lineage",
      asset_step_id: "step-runtime-input-lineage",
      node_key: {{__MODULE__.Asset, :asset}, nil},
      input_fingerprint: String.duplicate("a", 64),
      input_versions: [],
      input_generations: [],
      manifest_version_id: "manifest-runtime-input-lineage",
      manifest_content_hash: String.duplicate("b", 64),
      execution_package_hash: String.duplicate("c", 64),
      runtime_input_lineage: %{
        node_key: {{__MODULE__.Input, :asset}, nil},
        resolver: __MODULE__.Inputs,
        input_identity: "landing/input.json",
        payload_fingerprint: String.duplicate("d", 64),
        source_run_id: nil,
        source_node_key: nil,
        source_payload_fingerprint: nil
      },
      target_generation_id: generation_id,
      evidence_generation_id: generation_id,
      owner_id: "owner-runtime-input-lineage",
      fencing_token: 1,
      version: 1
    }

    result = %RunnerResult{status: :ok}

    freshness_state = %AssetFreshnessState{
      asset_ref_module: __MODULE__.Asset,
      asset_ref_name: :asset,
      freshness_key: "latest",
      freshness_version: "freshness-runtime-input-lineage",
      evidence_generation_id: generation_id,
      status: :ok,
      updated_at: DateTime.utc_now()
    }

    assert :ok = MaterializationClaims.complete(claim, result, freshness_state)
    assert_received {:finish_materialization, command}

    assert command.payload["runtime_input_lineage"] == %{
             "node_key" => [
               %{
                 "module" => "Elixir.FavnOrchestrator.MaterializationClaimsTest.Input",
                 "name" => "asset"
               },
               nil
             ],
             "resolver" => "Elixir.FavnOrchestrator.MaterializationClaimsTest.Inputs",
             "input_identity" => "landing/input.json",
             "payload_fingerprint" => String.duplicate("d", 64),
             "source_run_id" => nil,
             "source_node_key" => nil,
             "source_payload_fingerprint" => nil
           }

    assert {:ok, _json} = Jason.encode(command.payload)
  end
end
