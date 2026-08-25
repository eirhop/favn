defmodule Favn.Manifest.CompatibilityTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest.Asset
  alias Favn.Manifest.Compatibility
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.Schedule
  alias Favn.Manifest.Serializer
  alias Favn.SQL.PartitionSpec

  test "accepts current schema and runner contract versions" do
    manifest = current_manifest()
    assert :ok = Compatibility.validate_manifest(manifest)
  end

  test "rejects the previous schema version" do
    manifest = current_manifest(%{schema_version: 18})

    assert {:error, {:unsupported_schema_version, 18, 19}} =
             Compatibility.validate_manifest(manifest)
  end

  test "rejects unsupported schema version" do
    manifest = current_manifest(%{schema_version: 20})

    assert {:error, {:unsupported_schema_version, 20, 19}} =
             Compatibility.validate_manifest(manifest)
  end

  test "requires execution-pool policy in the manifest contract" do
    manifest = Map.delete(current_manifest(), :execution_pools)

    assert {:error, {:missing_manifest_field, :execution_pools}} =
             Compatibility.validate_manifest(manifest)
  end

  test "requires connection circuit policy in the current manifest contract" do
    manifest = Map.delete(current_manifest(), :connection_circuits)

    assert {:error, {:missing_manifest_field, :connection_circuits}} =
             Compatibility.validate_manifest(manifest)
  end

  test "rejects asset references to pools absent from the policy catalogue" do
    ref = {MyApp.PooledAsset, :asset}

    asset = %Asset{
      ref: ref,
      module: MyApp.PooledAsset,
      name: :asset,
      type: :elixir,
      execution_pool: :partner_api
    }

    assert {:error, {:unknown_execution_pool_reference, "partner_api"}} =
             current_manifest(%{assets: [asset]})
             |> Compatibility.validate_manifest()

    assert :ok =
             current_manifest(%{
               assets: [asset],
               execution_pools: %{partner_api: %{max_concurrency: 3}}
             })
             |> Compatibility.validate_manifest()
  end

  test "rejects malformed execution-pool references with a tagged error" do
    asset = %Asset{
      ref: {MyApp.MalformedPooledAsset, :asset},
      module: MyApp.MalformedPooledAsset,
      name: :asset,
      type: :elixir,
      execution_pool: {:not, :a_pool}
    }

    assert {:error, {:invalid_execution_pool_reference, {:not, :a_pool}}} =
             current_manifest(%{assets: [asset]})
             |> Compatibility.validate_manifest()
  end

  test "rejects unsupported runner contract version" do
    manifest = current_manifest(%{runner_contract_version: 15})

    assert {:error, {:unsupported_runner_contract_version, 15, 14}} =
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
    manifest = current_manifest(%{runner_contract_version: 13})

    assert {:error, {:unsupported_runner_contract_version, 13, 14}} =
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

  test "rejects missing runner release map" do
    manifest = Map.delete(current_manifest(), :runner_releases)

    assert {:error, {:missing_manifest_field, :runner_releases}} =
             Compatibility.validate_manifest(manifest)
  end

  test "rejects a non-canonical runner release identity in the map" do
    manifest = current_manifest(%{runner_releases: %{"default" => "rr_NOT_CANONICAL"}})

    assert {:error, {:invalid_runner_release_field, :runner_release_id, :invalid_id}} =
             Compatibility.validate_manifest(manifest)
  end

  test "rejects non-map compatibility input with tagged error" do
    assert {:error, {:invalid_manifest_input, :invalid}} =
             Compatibility.validate_manifest(:invalid)
  end

  test "rehydrates valid serialized manifest entries before compatibility validation" do
    manifest =
      current_manifest(%{
        assets: [],
        pipelines: [Map.from_struct(%Pipeline{module: MyApp.Pipeline, name: :daily})],
        schedules: [
          Map.from_struct(%Schedule{
            module: MyApp.Pipeline,
            name: :daily,
            ref: {MyApp.Pipeline, :daily},
            timezone: "Etc/UTC",
            timezone_source: :utc_fallback
          })
        ]
      })

    assert :ok = Compatibility.validate_manifest(manifest)
  end

  test "accepts a serialized SQL asset whose type is encoded as a string" do
    asset = %Asset{
      ref: {MyApp.SerializedSQLAsset, :asset},
      module: MyApp.SerializedSQLAsset,
      name: :asset,
      type: :sql,
      execution_package_hash: String.duplicate("a", 64)
    }

    assert {:ok, graph} = Graph.build([asset])

    manifest = %Favn.Manifest{
      assets: [asset],
      graph: graph,
      runner_releases: %{"default" => FavnTestSupport.runner_release_id(:primary)}
    }

    assert {:ok, encoded} = Serializer.encode_manifest(manifest)
    assert {:ok, decoded} = Jason.decode(encoded)
    assert get_in(decoded, ["assets", Access.at(0), "type"]) == "sql"
    assert :ok = Compatibility.validate_manifest(decoded)
  end

  test "rejects malformed entries instead of silently skipping them" do
    for {field, malformed} <- [assets: :not_an_asset, pipelines: "not a pipeline", schedules: 42] do
      manifest = Map.put(current_manifest(), field, [malformed])

      assert {:error, {:invalid_manifest_entry, ^field, 0}} =
               Compatibility.validate_manifest(manifest)
    end
  end

  test "rejects raw assets and pipelines with missing required identities" do
    for malformed <- [
          %{},
          %{
            "ref" => ["Elixir.MyApp.Asset", "asset"],
            "module" => "Elixir.MyApp.Asset",
            "name" => "asset"
          }
        ] do
      manifest = Map.put(current_manifest(), :assets, [malformed])

      assert {:error, {:invalid_manifest_entry, :assets, 0}} =
               Compatibility.validate_manifest(manifest)
    end

    for malformed <- [%{}, %{"module" => "Elixir.MyApp.Pipeline"}] do
      manifest = Map.put(current_manifest(), :pipelines, [malformed])

      assert {:error, {:invalid_manifest_entry, :pipelines, 0}} =
               Compatibility.validate_manifest(manifest)
    end

    manifest = Map.put(current_manifest(), :schedules, [%{}])

    assert {:error, {:invalid_manifest_entry, :schedules, 0}} =
             Compatibility.validate_manifest(manifest)
  end

  test "rejects canonical assets and pipelines with invalid identities" do
    assert {:error, {:invalid_manifest_asset, nil, :invalid_asset_identity}} =
             current_manifest(%{assets: [%Asset{}]})
             |> Compatibility.validate_manifest()

    assert {:error, {:invalid_manifest_pipeline, nil, :invalid_pipeline_identity}} =
             current_manifest(%{pipelines: [%Pipeline{}]})
             |> Compatibility.validate_manifest()

    assert {:error, {:invalid_manifest_schedule, nil, :invalid_schedule_identity}} =
             current_manifest(%{
               schedules: [
                 %Schedule{timezone: "Etc/UTC", timezone_source: :utc_fallback}
               ]
             })
             |> Compatibility.validate_manifest()
  end

  test "partition specs require SQL table or incremental assets" do
    spec = PartitionSpec.normalize!([:tenant_id])

    view = %Asset{
      ref: {MyApp.View, :asset},
      module: MyApp.View,
      name: :asset,
      type: :sql,
      materialization: :view,
      partition_spec: spec,
      execution_package_hash: String.duplicate("a", 64)
    }

    assert {:error,
            {:invalid_manifest_asset, {MyApp.View, :asset},
             :partition_spec_requires_table_materialization}} =
             current_manifest(%{assets: [view]})
             |> Compatibility.validate_manifest()

    elixir_asset = %Asset{
      ref: {MyApp.ElixirAsset, :asset},
      module: MyApp.ElixirAsset,
      name: :asset,
      type: :elixir,
      partition_spec: spec
    }

    assert {:error,
            {:invalid_manifest_asset, {MyApp.ElixirAsset, :asset},
             :partition_spec_requires_sql_asset}} =
             current_manifest(%{assets: [elixir_asset]})
             |> Compatibility.validate_manifest()
  end

  test "malformed partition structs produce typed compatibility errors" do
    asset = %Asset{
      ref: {MyApp.Table, :asset},
      module: MyApp.Table,
      name: :asset,
      type: :sql,
      materialization: :table,
      partition_spec: %PartitionSpec{keys: nil},
      execution_package_hash: String.duplicate("a", 64)
    }

    assert {:error,
            {:invalid_manifest_asset, {MyApp.Table, :asset}, {:invalid_partition_spec, message}}} =
             current_manifest(%{assets: [asset]})
             |> Compatibility.validate_manifest()

    assert message =~ "keys must be a non-empty list"
  end

  defp current_manifest(overrides \\ %{}) do
    %{assets: []}
    |> Map.merge(overrides)
    |> FavnTestSupport.with_manifest_contract()
    |> Map.merge(overrides)
  end
end
