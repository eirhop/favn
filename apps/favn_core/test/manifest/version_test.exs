defmodule Favn.Manifest.VersionTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest.Compatibility
  alias Favn.Manifest.Version

  test "builds pinned manifest version with id and content hash" do
    manifest = %{schema_version: 1, runner_contract_version: 1, assets: []}

    assert {:ok, %Version{} = version} =
             Version.new(manifest,
               manifest_version_id: "mv_test_001",
               inserted_at: ~U[2026-01-01 00:00:00Z]
             )

    assert version.manifest_version_id == "mv_test_001"
    assert version.schema_version == Compatibility.current_schema_version()
    assert version.runner_contract_version == Compatibility.current_runner_contract_version()
    assert version.serialization_format == "json-v1"
    assert version.inserted_at == ~U[2026-01-01 00:00:00Z]
    assert is_binary(version.content_hash)
    assert byte_size(version.content_hash) == 64
  end

  test "fails when schema version is unsupported" do
    manifest = %{schema_version: 1, runner_contract_version: 1, assets: []}

    assert {:error, {:unsupported_schema_version, 2, 1}} =
             Version.new(manifest, schema_version: 2)
  end
end
