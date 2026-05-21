defmodule FavnOrchestrator.Storage.MaterializationClaimCodecTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.MaterializationClaim
  alias FavnOrchestrator.Storage.MaterializationClaimCodec

  @format "favn.materialization_claim.storage.v1"
  @external_module_payload "g3QAAAAUdwVlcnJvcncDbmlsdwZzdGF0dXN3CXN1Y2NlZWRlZHcIbWV0YWRhdGF0AAAAAXcNcmVzdWx0X3N0YXR1c3cCb2t3Cl9fc3RydWN0X193LEVsaXhpci5GYXZuT3JjaGVzdHJhdG9yLk1hdGVyaWFsaXphdGlvbkNsYWltdxNtYW5pZmVzdF92ZXJzaW9uX2lkdwNuaWx3BnJ1bl9pZHcDbmlsdxVtYW5pZmVzdF9jb250ZW50X2hhc2h3A25pbHcRZnJlc2huZXNzX3ZlcnNpb253A25pbHcLZmluaXNoZWRfYXR0AAAADXcLbWljcm9zZWNvbmRoAmEAYQB3BnNlY29uZGEAdwhjYWxlbmRhcncTRWxpeGlyLkNhbGVuZGFyLklTT3cFbW9udGhhBXcKX19zdHJ1Y3RfX3cPRWxpeGlyLkRhdGVUaW1ldwNkYXlhFXcEeWVHcmIAAAfqdwZtaW51dGVhAXcEaG91cmEAdwl0aW1lX3pvbmVtAAAAB0V0Yy9VVEN3CXpvbmVfYWJicm0AAAADVVRDdwp1dGNfb2Zmc2V0YQB3CnN0ZF9vZmZzZXRhAHcKZXhwaXJlc19hdHQAAAANdwttaWNyb3NlY29uZGgCYQBhAHcGc2Vjb25kYQB3CGNhbGVuZGFydxNFbGl4aXIuQ2FsZW5kYXIuSVNPdwVtb250aGEFdwpfX3N0cnVjdF9fdw9FbGl4aXIuRGF0ZVRpbWV3A2RheWEVdwR5ZWFyYgAAB+p3Bm1pbnV0ZWEAdwRob3VyYQF3CXRpbWVfem9uZW0AAAAHRXRjL1VUQ3cJem9uZV9hYmJybQAAAANVVEN3CnV0Y19vZmZzZXRhAHcKc3RkX29mZnNldGEAdwpjbGFpbWVkX2F0dAAAAA13C21pY3Jvc2Vjb25kaAJhAGEAdwZzZWNvbmRhAHcIY2FsZW5kYXJ3E0VsaXhpci5DYWxlbmRhci5JU093BW1vbnRoYQV3Cl9fc3RydWN0X193D0VsaXhpci5EYXRlVGltZXcDZGF5YRV3BHllYXJiAAAH6ncGbWludXRlYQB3BGhvdXJhAHcJdGltZV96b25lbQAAAAdFdGMvVVRDdwl6b25lX2FiYnJtAAAAA1VUQ3cKdXRjX29mZnNldGEAdwpzdGRfb2Zmc2V0YQB3CWNsYWltX2tleW0AAAAOZXh0ZXJuYWwtY2xhaW13EGFzc2V0X3JlZl9tb2R1bGV3LEVsaXhpci5FeHRlcm5hbEFwcC5NYXRlcmlhbGl6YXRpb25DbGFpbUFzc2V0dw5hc3NldF9yZWZfbmFtZXcFYXNzZXR3DWZyZXNobmVzc19rZXltAAAAC3dpbmRvdzp0ZXN0dw1hc3NldF9zdGVwX2lkdwNuaWx3CG5vZGVfa2V5aAJoAncsRWxpeGlyLkV4dGVybmFsQXBwLk1hdGVyaWFsaXphdGlvbkNsYWltQXNzZXR3BWFzc2V0dAAAAAF3BndpbmRvd20AAAAEdGVzdHcRaW5wdXRfZmluZ2VycHJpbnRtAAAAC3NoYTI1Njp0ZXN0dxNydW5uZXJfZXhlY3V0aW9uX2lkdwNuaWx3DGhlYXJ0YmVhdF9hdHcDbmls"

  test "encodes materialization claims as versioned JSON DTOs" do
    claim = claim()

    assert {:ok, payload} = MaterializationClaimCodec.encode(claim)
    assert {:ok, dto} = Jason.decode(payload)
    assert dto["format"] == @format
    assert dto["schema_version"] == 1
    assert dto["claim_key"] == claim.claim_key
    assert dto["asset_ref_module"] == Atom.to_string(claim.asset_ref_module)
    assert dto["asset_ref_name"] == Atom.to_string(claim.asset_ref_name)
    assert dto["status"] == "succeeded"
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

  test "decodes legacy Base64 ETF claims" do
    claim = claim()
    legacy_payload = Base.encode64(:erlang.term_to_binary(claim))

    assert {:ok, restored} = MaterializationClaimCodec.decode(legacy_payload)
    assert restored == claim
  end

  test "decodes persisted claims containing unloaded consumer module atoms" do
    assert {:ok, restored} = MaterializationClaimCodec.decode(@external_module_payload)

    assert Atom.to_string(restored.asset_ref_module) ==
             "Elixir.ExternalApp.MaterializationClaimAsset"

    assert {{module, :asset}, %{window: "test"}} = restored.node_key

    assert Atom.to_string(module) == "Elixir.ExternalApp.MaterializationClaimAsset"
  end

  test "rejects invalid base64 payloads with tuple error shape" do
    assert {:error, :invalid_materialization_claim_payload} =
             MaterializationClaimCodec.decode("not valid base64")
  end

  test "rejects malformed JSON with tuple error shape" do
    assert {:error, {:invalid_materialization_claim_json, %Jason.DecodeError{}}} =
             MaterializationClaimCodec.decode("{")
  end

  test "rejects unknown DTO format clearly" do
    payload = Jason.encode!(%{"format" => "other", "schema_version" => 1})

    assert {:error, {:invalid_materialization_claim_dto, _dto}} =
             MaterializationClaimCodec.decode(payload)
  end

  test "rejects unsupported DTO schema versions clearly" do
    payload = Jason.encode!(%{"format" => @format, "schema_version" => 2})

    assert {:error, {:unsupported_materialization_claim_schema_version, 2}} =
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
