defmodule FavnOrchestrator.Storage.MaterializationClaimCodecTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.MaterializationClaim
  alias FavnOrchestrator.Storage.MaterializationClaimCodec

  @external_module_payload "g3QAAAAUdwVlcnJvcncDbmlsdwZzdGF0dXN3CXN1Y2NlZWRlZHcIbWV0YWRhdGF0AAAAAXcNcmVzdWx0X3N0YXR1c3cCb2t3Cl9fc3RydWN0X193LEVsaXhpci5GYXZuT3JjaGVzdHJhdG9yLk1hdGVyaWFsaXphdGlvbkNsYWltdxNtYW5pZmVzdF92ZXJzaW9uX2lkdwNuaWx3BnJ1bl9pZHcDbmlsdxVtYW5pZmVzdF9jb250ZW50X2hhc2h3A25pbHcRZnJlc2huZXNzX3ZlcnNpb253A25pbHcLZmluaXNoZWRfYXR0AAAADXcLbWljcm9zZWNvbmRoAmEAYQB3BnNlY29uZGEAdwhjYWxlbmRhcncTRWxpeGlyLkNhbGVuZGFyLklTT3cFbW9udGhhBXcKX19zdHJ1Y3RfX3cPRWxpeGlyLkRhdGVUaW1ldwNkYXlhFXcEeWVHcmIAAAfqdwZtaW51dGVhAXcEaG91cmEAdwl0aW1lX3pvbmVtAAAAB0V0Yy9VVEN3CXpvbmVfYWJicm0AAAADVVRDdwp1dGNfb2Zmc2V0YQB3CnN0ZF9vZmZzZXRhAHcKZXhwaXJlc19hdHQAAAANdwttaWNyb3NlY29uZGgCYQBhAHcGc2Vjb25kYQB3CGNhbGVuZGFydxNFbGl4aXIuQ2FsZW5kYXIuSVNPdwVtb250aGEFdwpfX3N0cnVjdF9fdw9FbGl4aXIuRGF0ZVRpbWV3A2RheWEVdwR5ZWFyYgAAB+p3Bm1pbnV0ZWEAdwRob3VyYQF3CXRpbWVfem9uZW0AAAAHRXRjL1VUQ3cJem9uZV9hYmJybQAAAANVVEN3CnV0Y19vZmZzZXRhAHcKc3RkX29mZnNldGEAdwpjbGFpbWVkX2F0dAAAAA13C21pY3Jvc2Vjb25kaAJhAGEAdwZzZWNvbmRhAHcIY2FsZW5kYXJ3E0VsaXhpci5DYWxlbmRhci5JU093BW1vbnRoYQV3Cl9fc3RydWN0X193D0VsaXhpci5EYXRlVGltZXcDZGF5YRV3BHllYXJiAAAH6ncGbWludXRlYQB3BGhvdXJhAHcJdGltZV96b25lbQAAAAdFdGMvVVRDdwl6b25lX2FiYnJtAAAAA1VUQ3cKdXRjX29mZnNldGEAdwpzdGRfb2Zmc2V0YQB3CWNsYWltX2tleW0AAAAOZXh0ZXJuYWwtY2xhaW13EGFzc2V0X3JlZl9tb2R1bGV3LEVsaXhpci5FeHRlcm5hbEFwcC5NYXRlcmlhbGl6YXRpb25DbGFpbUFzc2V0dw5hc3NldF9yZWZfbmFtZXcFYXNzZXR3DWZyZXNobmVzc19rZXltAAAAC3dpbmRvdzp0ZXN0dw1hc3NldF9zdGVwX2lkdwNuaWx3CG5vZGVfa2V5aAJoAncsRWxpeGlyLkV4dGVybmFsQXBwLk1hdGVyaWFsaXphdGlvbkNsYWltQXNzZXR3BWFzc2V0dAAAAAF3BndpbmRvd20AAAAEdGVzdHcRaW5wdXRfZmluZ2VycHJpbnRtAAAAC3NoYTI1Njp0ZXN0dxNydW5uZXJfZXhlY3V0aW9uX2lkdwNuaWx3DGhlYXJ0YmVhdF9hdHcDbmls"

  test "round-trips materialization claims" do
    {:ok, claim} =
      MaterializationClaim.new(%{
        claim_key: "claim_codec",
        asset_ref_module: __MODULE__.Asset,
        asset_ref_name: :orders,
        freshness_key: "window:test",
        input_fingerprint: "sha256:test",
        status: :succeeded,
        claimed_at: ~U[2026-05-21 00:00:00Z],
        expires_at: ~U[2026-05-21 01:00:00Z],
        finished_at: ~U[2026-05-21 00:01:00Z],
        metadata: %{result_status: :ok}
      })

    assert {:ok, payload} = MaterializationClaimCodec.encode(claim)
    assert {:ok, restored} = MaterializationClaimCodec.decode(payload)
    assert restored.claim_key == claim.claim_key
    assert restored.status == :succeeded
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
end
