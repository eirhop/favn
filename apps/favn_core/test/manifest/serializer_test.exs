defmodule Favn.Manifest.SerializerTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest.Build
  alias Favn.Manifest.Serializer

  test "encodes canonical json with sorted keys" do
    manifest = %{schema_version: 1, runner_contract_version: 1, z: 1, a: 2}

    assert {:ok, encoded} = Serializer.encode_manifest(manifest)
    assert encoded == ~s|{"a":2,"runner_contract_version":1,"schema_version":1,"z":1}|
  end

  test "drops build-only keys from encoded payload" do
    manifest = %{
      schema_version: 1,
      runner_contract_version: 1,
      generated_at: DateTime.utc_now(),
      diagnostics: [%{message: "warn"}],
      assets: []
    }

    assert {:ok, encoded} = Serializer.encode_manifest(manifest)
    refute encoded =~ "generated_at"
    refute encoded =~ "diagnostics"
  end

  test "uses build manifest payload when build struct is provided" do
    build =
      Build.new(%{schema_version: 1, runner_contract_version: 1, assets: []},
        diagnostics: ["ignored"]
      )

    assert {:ok, encoded} = Serializer.encode_manifest(build)
    assert {:ok, decoded} = Serializer.decode_manifest(encoded)
    assert decoded["schema_version"] == 1
    refute Map.has_key?(decoded, "diagnostics")
  end
end
