defmodule Favn.Manifest.CompatibilityTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest.Compatibility

  test "accepts current schema and runner contract versions" do
    manifest = %{schema_version: 1, runner_contract_version: 1, assets: []}
    assert :ok = Compatibility.validate_manifest(manifest)
  end

  test "rejects unsupported schema version" do
    manifest = %{schema_version: 9, runner_contract_version: 1, assets: []}

    assert {:error, {:unsupported_schema_version, 9, 1}} =
             Compatibility.validate_manifest(manifest)
  end

  test "rejects unsupported runner contract version" do
    manifest = %{schema_version: 1, runner_contract_version: 7, assets: []}

    assert {:error, {:unsupported_runner_contract_version, 7, 1}} =
             Compatibility.validate_manifest(manifest)
  end

  test "rejects missing schema version" do
    manifest = %{runner_contract_version: 1, assets: []}

    assert {:error, {:missing_manifest_field, :schema_version}} =
             Compatibility.validate_manifest(manifest)
  end

  test "rejects missing runner contract version" do
    manifest = %{schema_version: 1, assets: []}

    assert {:error, {:missing_manifest_field, :runner_contract_version}} =
             Compatibility.validate_manifest(manifest)
  end

  test "rejects non-map compatibility input with tagged error" do
    assert {:error, {:invalid_manifest_input, :invalid}} =
             Compatibility.validate_manifest(:invalid)
  end
end
