defmodule Favn.Manifest.CompatibilityTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest.Compatibility

  test "accepts current schema and runner contract versions" do
    manifest = current_manifest()
    assert :ok = Compatibility.validate_manifest(manifest)
  end

  test "rejects the previous schema version" do
    manifest = current_manifest(%{schema_version: 9})

    assert {:error, {:unsupported_schema_version, 9, 10}} =
             Compatibility.validate_manifest(manifest)
  end

  test "rejects unsupported schema version" do
    manifest = current_manifest(%{schema_version: 11})

    assert {:error, {:unsupported_schema_version, 11, 10}} =
             Compatibility.validate_manifest(manifest)
  end

  test "rejects unsupported runner contract version" do
    manifest = current_manifest(%{runner_contract_version: 11})

    assert {:error, {:unsupported_runner_contract_version, 11, 10}} =
             Compatibility.validate_manifest(manifest)
  end

  test "requires one valid execution package hash for every SQL asset" do
    ref = {MyApp.SQLAsset, :asset}

    manifest =
      current_manifest(%{assets: [%{ref: ref, type: :sql, execution_package_hash: nil}]})

    assert {:error, {:missing_execution_package_hash, ^ref}} =
             Compatibility.validate_manifest(manifest)

    invalid = put_in(manifest, [:assets, Access.at(0), :execution_package_hash], "short")

    assert {:error, {:invalid_execution_package_hash, ^ref, "short"}} =
             Compatibility.validate_manifest(invalid)
  end

  test "rejects execution package hashes on non-SQL assets" do
    ref = {MyApp.ElixirAsset, :asset}

    manifest =
      current_manifest(%{
        assets: [
          %{ref: ref, type: :elixir, execution_package_hash: String.duplicate("a", 64)}
        ]
      })

    assert {:error, {:unexpected_execution_package_hash, ^ref}} =
             Compatibility.validate_manifest(manifest)
  end

  test "rejects the previous runner contract version" do
    manifest = current_manifest(%{runner_contract_version: 9})

    assert {:error, {:unsupported_runner_contract_version, 9, 10}} =
             Compatibility.validate_manifest(manifest)
  end

  test "rejects missing schema version" do
    manifest = Map.delete(current_manifest(), :schema_version)

    assert {:error, {:missing_manifest_field, :schema_version}} =
             Compatibility.validate_manifest(manifest)
  end

  test "rejects missing runner contract version" do
    manifest = Map.delete(current_manifest(), :runner_contract_version)

    assert {:error, {:missing_manifest_field, :runner_contract_version}} =
             Compatibility.validate_manifest(manifest)
  end

  test "rejects missing runner release identity" do
    manifest = Map.delete(current_manifest(), :required_runner_release_id)

    assert {:error, {:missing_manifest_field, :required_runner_release_id}} =
             Compatibility.validate_manifest(manifest)
  end

  test "rejects a non-canonical runner release identity" do
    manifest = current_manifest(%{required_runner_release_id: "rr_NOT_CANONICAL"})

    assert {:error, {:invalid_required_runner_release_id, "rr_NOT_CANONICAL"}} =
             Compatibility.validate_manifest(manifest)
  end

  test "rejects non-map compatibility input with tagged error" do
    assert {:error, {:invalid_manifest_input, :invalid}} =
             Compatibility.validate_manifest(:invalid)
  end

  defp current_manifest(overrides \\ %{}) do
    %{assets: []}
    |> Map.merge(overrides)
    |> FavnTestSupport.with_manifest_contract()
    |> Map.merge(overrides)
  end
end
