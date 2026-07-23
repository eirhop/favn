defmodule FavnOrchestrator.Storage.MaterializationClaimCodecTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.MaterializationClaim
  alias FavnOrchestrator.Storage.MaterializationClaimCodec

  @format "favn.materialization_claim.storage.v2"

  test "encodes materialization claims as versioned JSON DTOs" do
    claim = claim()

    assert {:ok, payload} = MaterializationClaimCodec.encode(claim)
    assert {:ok, dto} = Jason.decode(payload)
    assert dto["format"] == @format
    assert dto["schema_version"] == 2
    assert dto["claim_key"] == claim.claim_key
    assert dto["asset_ref_module"] == Atom.to_string(claim.asset_ref_module)
    assert dto["asset_ref_name"] == Atom.to_string(claim.asset_ref_name)
    assert dto["status"] == "succeeded"
    assert dto["target_generation_id"] == claim.target_generation_id
    assert dto["evidence_generation_id"] == claim.evidence_generation_id
    assert dto["metadata"] == %{"result_status" => "ok"}
    assert dto["node_key"] == %{"type" => "string", "value" => "node:claim_codec"}

    assert {:ok, restored} = MaterializationClaimCodec.decode(payload)
    assert restored.claim_key == claim.claim_key
    assert restored.status == :succeeded
    assert restored.metadata == %{"result_status" => "ok"}
  end

  test "encodes structured node keys without inspect-based tuple payloads" do
    claim = %{
      claim()
      | node_key: {{__MODULE__.Asset, :orders}, %{"window" => "2026-05-21"}}
    }

    assert {:ok, payload} = MaterializationClaimCodec.encode(claim)
    assert {:ok, dto} = Jason.decode(payload)

    assert dto["node_key"] == %{
             "type" => "asset_node",
             "ref" => %{
               "module" => "Elixir.FavnOrchestrator.Storage.MaterializationClaimCodecTest.Asset",
               "name" => "orders"
             },
             "identity" => %{"type" => "json", "value" => %{"window" => "2026-05-21"}}
           }

    assert {:ok, restored} = MaterializationClaimCodec.decode(payload)
    assert restored.node_key == claim.node_key
  end

  test "new JSON payloads decode without loading consumer modules" do
    unloaded_module = String.to_atom("Elixir.ExternalApp.JsonMaterializationClaimAsset")
    refute Code.ensure_loaded?(unloaded_module)

    claim = %{
      claim()
      | asset_ref_module: unloaded_module,
        node_key: {{unloaded_module, :asset}, nil}
    }

    assert {:ok, payload} = MaterializationClaimCodec.encode(claim)
    assert {:ok, restored} = MaterializationClaimCodec.decode(payload)
    refute Code.ensure_loaded?(unloaded_module)
    assert restored.asset_ref_module == unloaded_module
    assert restored.node_key == {{unloaded_module, :asset}, nil}
  end

  test "rejects unknown module text from a persisted DTO" do
    module =
      "Elixir.ExternalApp.UnloadedClaimAsset#{System.unique_integer([:positive])}"

    assert {:ok, payload} = MaterializationClaimCodec.encode(claim())

    payload =
      payload
      |> Jason.decode!()
      |> Map.put("asset_ref_module", module)
      |> Jason.encode!()

    assert {:error, {:unknown_module, ^module}} = MaterializationClaimCodec.decode(payload)
    assert_raise ArgumentError, fn -> String.to_existing_atom(module) end
  end

  test "rejects malformed module identities and claim lifetimes" do
    assert {:ok, payload} = MaterializationClaimCodec.encode(claim())

    assert {:error, {:invalid_module, "Elixir.Bad;System.halt()"}} =
             payload
             |> Jason.decode!()
             |> Map.put("asset_ref_module", "Elixir.Bad;System.halt()")
             |> Jason.encode!()
             |> MaterializationClaimCodec.decode()

    assert {:error, {:invalid_materialization_claim_range, :expires_at, _, _}} =
             claim()
             |> Map.from_struct()
             |> Map.put(:expires_at, ~U[2026-05-20 23:59:59Z])
             |> MaterializationClaim.new()

    assert {:error, {:invalid_materialization_claim_field, :run_id, ""}} =
             claim()
             |> Map.from_struct()
             |> Map.put(:run_id, "")
             |> MaterializationClaim.new()
  end

  test "rejects non-JSON payloads" do
    assert {:error, {:invalid_materialization_claim_json, %Jason.DecodeError{}}} =
             MaterializationClaimCodec.decode("not json")
  end

  test "rejects malformed JSON with tuple error shape" do
    assert {:error, {:invalid_materialization_claim_json, %Jason.DecodeError{}}} =
             MaterializationClaimCodec.decode("{")
  end

  test "rejects unknown DTO format clearly" do
    payload = Jason.encode!(%{"format" => "other", "schema_version" => 2})

    assert {:error, {:invalid_materialization_claim_dto, _dto}} =
             MaterializationClaimCodec.decode(payload)
  end

  test "rejects unsupported DTO schema versions clearly" do
    payload = Jason.encode!(%{"format" => @format, "schema_version" => 3})

    assert {:error, {:unsupported_materialization_claim_schema_version, 3}} =
             MaterializationClaimCodec.decode(payload)
  end

  defp claim do
    {:ok, claim} =
      MaterializationClaim.new(%{
        claim_key: "claim_codec",
        asset_ref_module: __MODULE__.Asset,
        asset_ref_name: :orders,
        freshness_key: "window:test",
        input_fingerprint: "sha256:test",
        run_id: "run_codec",
        asset_step_id: "step_codec",
        node_key: "node:claim_codec",
        runner_execution_id: "runner_codec",
        manifest_version_id: "manifest_codec",
        manifest_content_hash: "hash_codec",
        target_generation_id: "018f47a0-7b0d-4b1a-8d8b-e18a9a987654",
        evidence_generation_id: "018f47a0-7b0d-4b1a-8d8b-e18a9a987654",
        freshness_version: "freshness_codec",
        status: :succeeded,
        claimed_at: ~U[2026-05-21 00:00:00Z],
        heartbeat_at: ~U[2026-05-21 00:00:30Z],
        expires_at: ~U[2026-05-21 01:00:00Z],
        finished_at: ~U[2026-05-21 00:01:00Z],
        metadata: %{result_status: :ok}
      })

    claim
  end
end
