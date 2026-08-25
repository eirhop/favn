defmodule Favn.Manifest.TargetDescriptorTest do
  use ExUnit.Case, async: true

  alias Favn.Coverage.Effective
  alias Favn.Coverage.Spec, as: CoverageSpec
  alias Favn.Manifest.TargetDescriptor
  alias Favn.RelationRef
  alias Favn.SQL.PartitionSpec
  alias Favn.Window.Spec, as: WindowSpec

  @package_hash String.duplicate("a", 64)
  @rc9_incremental_descriptor_hash "25a070c600d723c301784ceed41978ebc119bad62d8391d631f0de0efacc8d96"
  @rc9_incremental_descriptor_json ~S({"adapter":"Elixir.FavnTestSupport.TargetAdapter","connection_identity":{"definition_module":null,"name":"warehouse"},"contract_fingerprint":null,"coverage":null,"descriptor_hash":"25a070c600d723c301784ceed41978ebc119bad62d8391d631f0de0efacc8d96","execution_package_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","grain_fingerprint":null,"manifest_schema_version":17,"materialization":{"kind":"incremental","strategy":"delete_insert","unique_key":["id"],"window_column":"partition_day"},"relation":{"catalog":null,"connection":"warehouse","name":"asset","schema":"analytics"},"runner_contract_version":13,"schema_version":1,"target_id":"asset:Elixir.MyApp.Asset:asset","window_identity":null,"window_identity_fingerprint":null,"write_semantics":{"mode":"incremental","strategy":"delete_insert","unique_key":["id"],"window_column":"partition_day"}})

  test "builds stable compatibility fingerprints for a persisted SQL table" do
    asset = persisted_asset(WindowSpec.new!(:day, timezone: "Europe/Oslo"))

    descriptor =
      TargetDescriptor.from_asset(asset,
        connection_definitions: connection_definitions(),
        manifest_schema_version: 13,
        runner_contract_version: 12
      )

    assert descriptor.target_id == "asset:Elixir.MyApp.Target:asset"
    assert descriptor.relation.name == "target"

    assert descriptor.connection_identity == %{
             name: "warehouse",
             definition_module: "Elixir.MyApp.Warehouse"
           }

    assert descriptor.adapter == "Elixir.MyApp.Adapter"
    assert descriptor.window_identity == %{kind: "day", timezone: "Europe/Oslo"}
    assert byte_size(descriptor.descriptor_hash) == 64
    assert byte_size(descriptor.contract_fingerprint) == 64
    assert byte_size(descriptor.grain_fingerprint) == 64
  end

  test "provenance does not change window identity fingerprints" do
    local =
      WindowSpec.new!(:day, timezone: "Europe/Oslo")
      |> WindowSpec.with_declaration_source(:local)

    inherited =
      WindowSpec.new!(:day, timezone: "Europe/Oslo")
      |> WindowSpec.with_declaration_source(:namespace)

    left = TargetDescriptor.from_asset(persisted_asset(local), versions())
    right = TargetDescriptor.from_asset(persisted_asset(inherited), versions())

    assert left.window_identity_fingerprint == right.window_identity_fingerprint
    assert left.descriptor_hash == right.descriptor_hash
  end

  test "descriptions do not change physical fingerprints but unique keys do" do
    base = persisted_asset(WindowSpec.new!(:day, timezone: "Etc/UTC"))

    described =
      put_in(base, [:assurance, :contract], %{
        grain: %{by: [:id], description: "display text"},
        columns: [%{name: :id, type: :integer, nullable?: false, description: "display text"}],
        unique_keys: [%{columns: [:id]}]
      })

    renamed_description =
      described
      |> put_in([:assurance, :contract, :grain, :description], "different display text")
      |> put_in(
        [:assurance, :contract, :columns],
        [%{name: :id, type: :integer, nullable?: false, description: "different display text"}]
      )

    different_key =
      put_in(described, [:assurance, :contract, :unique_keys], [%{columns: [:id, :tenant_id]}])

    original = TargetDescriptor.from_asset(described, versions())
    metadata_only = TargetDescriptor.from_asset(renamed_description, versions())
    structural = TargetDescriptor.from_asset(different_key, versions())

    assert original.contract_fingerprint == metadata_only.contract_fingerprint
    assert original.grain_fingerprint == metadata_only.grain_fingerprint
    assert original.descriptor_hash == metadata_only.descriptor_hash
    refute original.grain_fingerprint == structural.grain_fingerprint
  end

  test "physical partition declarations do not become target compatibility identity" do
    asset = persisted_asset(WindowSpec.new!(:day, timezone: "Etc/UTC"))

    partitioned =
      Map.put(
        asset,
        :partition_spec,
        PartitionSpec.normalize!([:tenant_id, {:month, :occurred_at}])
      )

    assert TargetDescriptor.from_asset(asset, versions()).descriptor_hash ==
             TargetDescriptor.from_asset(partitioned, versions()).descriptor_hash
  end

  test "group replacement key is ordered structural identity" do
    asset = %{
      persisted_asset(nil)
      | materialization:
          {:incremental, strategy: :replace_groups, replacement_key: [:tenant_id, :customer_id]}
    }

    descriptor = TargetDescriptor.from_asset(asset, versions())

    assert descriptor.schema_version == 2

    assert descriptor.materialization == %{
             kind: "incremental",
             strategy: "replace_groups",
             unique_key: nil,
             window_column: nil,
             replacement_key: ["tenant_id", "customer_id"]
           }

    reversed =
      TargetDescriptor.from_asset(
        %{
          asset
          | materialization:
              {:incremental,
               strategy: :replace_groups, replacement_key: [:customer_id, :tenant_id]}
        },
        versions()
      )

    refute descriptor.descriptor_hash == reversed.descriptor_hash
  end

  test "rejects forbidden fields in a reconstructed group replacement descriptor" do
    asset = %{
      persisted_asset(nil)
      | materialization:
          {:incremental, strategy: :replace_groups, replacement_key: [:customer_id]}
    }

    descriptor = TargetDescriptor.from_asset(asset, versions())

    invalid = %{
      descriptor
      | materialization: %{descriptor.materialization | window_column: "event_day"}
    }

    assert {:error, :invalid_target_materialization} = TargetDescriptor.validate(invalid)
  end

  test "JSON roundtrip preserves the complete canonical descriptor" do
    window = WindowSpec.new!(:day, timezone: "Europe/Oslo")

    assert {:ok, coverage} =
             Effective.resolve(
               CoverageSpec.new!(
                 from: ~D[2026-01-01],
                 availability_delay: {:hours, 6}
               ),
               window,
               ~D[2026-03-01]
             )

    asset = %{
      persisted_asset(window)
      | materialization:
          {:incremental,
           strategy: :delete_insert, unique_key: [:id], window_column: :partition_day},
        coverage: coverage
    }

    descriptor = TargetDescriptor.from_asset(asset, versions())
    assert {:ok, encoded} = Favn.Manifest.Serializer.encode_manifest(descriptor)
    assert {:ok, decoded} = Favn.Manifest.Serializer.decode_manifest(encoded)
    assert {:ok, rehydrated} = TargetDescriptor.from_value(decoded)
    assert rehydrated == descriptor
  end

  test "persisted schema-1 descriptors retain their original canonical hash" do
    assert {:ok, decoded} =
             Favn.Manifest.Serializer.decode_manifest(@rc9_incremental_descriptor_json)

    assert {:error, {:unsupported_target_descriptor_schema, 1}} =
             TargetDescriptor.from_value(decoded)

    assert {:ok, rehydrated} = TargetDescriptor.from_persisted_value(decoded)
    assert rehydrated.schema_version == 1
    assert rehydrated.descriptor_hash == @rc9_incremental_descriptor_hash

    assert rehydrated.materialization == %{
             kind: "incremental",
             strategy: "delete_insert",
             unique_key: ["id"],
             window_column: "partition_day"
           }

    assert TargetDescriptor.compatibility_field(rehydrated, :materialization) == %{
             kind: "incremental",
             strategy: "delete_insert",
             unique_key: ["id"],
             window_column: "partition_day",
             replacement_key: nil
           }

    assert Favn.Manifest.Serializer.encode_manifest!(rehydrated) ==
             @rc9_incremental_descriptor_json

    refute Map.has_key?(rehydrated.materialization, :replacement_key)
    refute Map.has_key?(rehydrated.write_semantics, :replacement_key)

    assert {:error, {:unsupported_target_descriptor_schema, 1}} =
             TargetDescriptor.validate(rehydrated)
  end

  test "persisted descriptors reject unknown schema versions" do
    descriptor =
      persisted_asset(nil)
      |> TargetDescriptor.from_asset(versions())
      |> Map.from_struct()
      |> Map.put(:schema_version, 3)

    assert {:error, {:unsupported_target_descriptor_schema, 3}} =
             TargetDescriptor.from_persisted_value(descriptor)
  end

  test "descriptor validation matches contract and grain fingerprints to the asset" do
    asset = persisted_asset(WindowSpec.new!(:day, timezone: "Etc/UTC"))

    changed_contract =
      put_in(
        asset,
        [:assurance, :contract, :columns],
        [%{name: :id, type: :string, nullable?: false}]
      )

    changed_grain =
      put_in(asset, [:assurance, :contract, :unique_keys], [%{columns: [:id]}])

    contract_descriptor = TargetDescriptor.from_asset(changed_contract, versions())
    grain_descriptor = TargetDescriptor.from_asset(changed_grain, versions())

    assert {:error,
            {:target_descriptor_asset_mismatch, :contract_fingerprint, _actual, _expected}} =
             TargetDescriptor.validate_asset(contract_descriptor, asset, 12, 12)

    assert {:error, {:target_descriptor_asset_mismatch, :grain_fingerprint, _actual, _expected}} =
             TargetDescriptor.validate_asset(grain_descriptor, asset, 12, 12)
  end

  test "views are not persisted rebuild targets" do
    assert TargetDescriptor.from_asset(
             %{persisted_asset(nil) | materialization: :view},
             versions()
           ) ==
             nil
  end

  defp persisted_asset(window) do
    %{
      ref: {MyApp.Target, :asset},
      type: :sql,
      relation: RelationRef.new!(connection: :warehouse, schema: "mart", name: "target"),
      materialization: :table,
      execution_package_hash: @package_hash,
      assurance: %{
        contract: %{
          grain: %{by: [:id], description: nil},
          columns: [%{name: :id, type: :integer, nullable?: false}]
        }
      },
      window: window,
      coverage: nil
    }
  end

  defp versions do
    [
      connection_definitions: connection_definitions(),
      manifest_schema_version: 13,
      runner_contract_version: 12
    ]
  end

  defp connection_definitions do
    %{
      warehouse: %{
        adapter: MyApp.Adapter,
        module: MyApp.Warehouse
      }
    }
  end
end
