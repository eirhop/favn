defmodule Favn.Manifest.CompatibilityTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest.Compatibility

  test "accepts current schema and runner contract versions" do
    manifest = %{schema_version: 4, runner_contract_version: 4, assets: []}
    assert :ok = Compatibility.validate_manifest(manifest)
  end

  test "rejects the previous schema version" do
    manifest = %{schema_version: 3, runner_contract_version: 4, assets: []}

    assert {:error, {:unsupported_schema_version, 3, 4}} =
             Compatibility.validate_manifest(manifest)
  end

  test "rejects unsupported schema version" do
    manifest = %{schema_version: 9, runner_contract_version: 4, assets: []}

    assert {:error, {:unsupported_schema_version, 9, 4}} =
             Compatibility.validate_manifest(manifest)
  end

  test "rejects unsupported runner contract version" do
    manifest = %{schema_version: 4, runner_contract_version: 7, assets: []}

    assert {:error, {:unsupported_runner_contract_version, 7, 4}} =
             Compatibility.validate_manifest(manifest)
  end

  test "rejects namespaced SQL definitions with the previous runner contract" do
    manifest = %{
      schema_version: 4,
      runner_contract_version: 3,
      assets: [
        %{
          type: :sql,
          sql_execution: %{
            sql_definitions: [
              %{name: :orders, relation_defaults: %{catalog: "raw", schema: "sales"}}
            ]
          }
        }
      ]
    }

    assert {:error, {:unsupported_runner_contract_version, 3, 4}} =
             Compatibility.validate_manifest(manifest)
  end

  test "rejects missing schema version" do
    manifest = %{runner_contract_version: 4, assets: []}

    assert {:error, {:missing_manifest_field, :schema_version}} =
             Compatibility.validate_manifest(manifest)
  end

  test "rejects missing runner contract version" do
    manifest = %{schema_version: 4, assets: []}

    assert {:error, {:missing_manifest_field, :runner_contract_version}} =
             Compatibility.validate_manifest(manifest)
  end

  test "rejects non-map compatibility input with tagged error" do
    assert {:error, {:invalid_manifest_input, :invalid}} =
             Compatibility.validate_manifest(:invalid)
  end
end
