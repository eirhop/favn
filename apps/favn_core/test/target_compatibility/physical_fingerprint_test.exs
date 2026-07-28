defmodule Favn.TargetCompatibility.PhysicalFingerprintTest do
  use ExUnit.Case, async: true

  alias Favn.Contracts.RelationInspectionResult
  alias Favn.Manifest.TargetDescriptor
  alias Favn.SQL.Contract
  alias Favn.TargetCompatibility.PhysicalFingerprint

  test "canonicalizes the physical relation and ordered columns" do
    result =
      inspection([
        reliable_column("id", " bigint ", false),
        reliable_column("amount", "decimal(18, 2)", true)
      ])

    assert {:ok, fingerprint} = PhysicalFingerprint.from_inspection(result)
    assert fingerprint.adapter == "Elixir.MyApp.Adapter"

    assert fingerprint.relation == %{
             catalog: "warehouse",
             schema: "mart",
             name: "orders",
             kind: "table"
           }

    assert fingerprint.columns == [
             %{
               name: "id",
               native_type: "BIGINT",
               logical_type: "integer",
               nullable: false,
               nullability_reliable: true
             },
             %{
               name: "amount",
               native_type: "DECIMAL(18, 2)",
               logical_type: "decimal",
               nullable: true,
               nullability_reliable: true
             }
           ]

    assert byte_size(fingerprint.fingerprint) == 64
  end

  test "ignores non-structural inspection metadata" do
    first =
      inspection([
        %{
          name: "id",
          data_type: "BIGINT",
          nullable?: false,
          default: "nextval('secret')",
          comment: "first",
          metadata: %{contract_nullability: :reliable}
        }
      ])

    second =
      inspection([
        %{
          name: "id",
          data_type: "bigint",
          nullable?: false,
          default: nil,
          comment: "second",
          metadata: %{contract_nullability: :reliable}
        }
      ])

    assert {:ok, left} = PhysicalFingerprint.from_inspection(first)
    assert {:ok, right} = PhysicalFingerprint.from_inspection(second)
    assert left.fingerprint == right.fingerprint
  end

  test "ordered column, type, and nullability changes alter the fingerprint" do
    assert {:ok, original} =
             PhysicalFingerprint.from_inspection(
               inspection([
                 reliable_column("id", "BIGINT", false),
                 reliable_column("label", "VARCHAR", true)
               ])
             )

    variants = [
      inspection([
        reliable_column("label", "VARCHAR", true),
        reliable_column("id", "BIGINT", false)
      ]),
      inspection([
        reliable_column("id", "VARCHAR", false),
        reliable_column("label", "VARCHAR", true)
      ]),
      inspection([
        reliable_column("id", "BIGINT", true),
        reliable_column("label", "VARCHAR", true)
      ])
    ]

    for result <- variants do
      assert {:ok, changed} = PhysicalFingerprint.from_inspection(result)
      refute changed.fingerprint == original.fingerprint
    end
  end

  test "keeps reliability metadata outside the exact physical fingerprint" do
    assert {:ok, unreliable} =
             PhysicalFingerprint.from_inspection(
               inspection([%{name: "id", data_type: "BIGINT", nullable?: true}])
             )

    assert {:ok, reliable} =
             PhysicalFingerprint.from_inspection(
               inspection([reliable_column("id", "BIGINT", true)])
             )

    refute hd(unreliable.columns).nullability_reliable
    assert hd(reliable.columns).nullability_reliable
    assert unreliable.fingerprint == reliable.fingerprint
  end

  test "compares contract nullability only when the adapter marks it reliable" do
    contract = Contract.new!(%{columns: [%{name: :id, type: :integer, null: false}]})

    desired = %{
      descriptor(%{catalog: "warehouse", schema: "mart", name: "orders"})
      | contract_fingerprint: String.duplicate("c", 64)
    }

    assert {:ok, uncertain} =
             PhysicalFingerprint.new(
               adapter: MyApp.Adapter,
               relation: relation(),
               columns: [%{name: "id", data_type: "BIGINT", nullable?: true}]
             )

    assert PhysicalFingerprint.identity_diff(desired, uncertain, contract) == []

    assert {:ok, reliable} =
             PhysicalFingerprint.new(
               adapter: MyApp.Adapter,
               relation: relation(),
               columns: [reliable_column("id", "BIGINT", true)]
             )

    assert [
             %{
               field: :contract_fingerprint,
               observed: %{differences: [%{kind: :nullability, column: "id"}]}
             }
           ] = PhysicalFingerprint.identity_diff(desired, reliable, contract)
  end

  test "distinguishes an absent relation from incomplete inspection" do
    assert {:ok, :not_found} =
             PhysicalFingerprint.from_inspection(%RelationInspectionResult{
               relation: nil,
               warnings: [%{code: :columns_failed}]
             })

    assert {:error, :relation_inspection_failed} =
             PhysicalFingerprint.from_inspection(%RelationInspectionResult{
               relation: nil,
               warnings: [%{code: :relation_failed}]
             })

    assert {:error, :column_inspection_failed} =
             PhysicalFingerprint.from_inspection(%RelationInspectionResult{
               relation: relation(),
               warnings: [%{code: :columns_failed}]
             })
  end

  test "accepts concrete default namespaces for an unqualified logical relation" do
    desired = descriptor(%{catalog: nil, schema: nil, name: "orders"})

    assert {:ok, observed} =
             PhysicalFingerprint.new(
               adapter: MyApp.Adapter,
               relation: %{
                 catalog: "runtime_database",
                 schema: "main",
                 name: "orders",
                 type: :table
               },
               columns: []
             )

    assert PhysicalFingerprint.identity_diff(desired, observed, nil) == []
  end

  test "requires explicit logical namespaces to match exactly" do
    desired = descriptor(%{catalog: "warehouse", schema: "mart", name: "orders"})

    assert {:ok, observed} =
             PhysicalFingerprint.new(
               adapter: MyApp.Adapter,
               relation: %{
                 catalog: "other",
                 schema: "mart",
                 name: "orders",
                 type: :table
               },
               columns: []
             )

    assert [%{field: :relation}] = PhysicalFingerprint.identity_diff(desired, observed, nil)
  end

  defp inspection(columns) do
    %RelationInspectionResult{
      relation: relation(),
      adapter: MyApp.Adapter,
      columns: columns,
      table_metadata: %{owner: "ignored"},
      warnings: []
    }
  end

  defp relation do
    %{catalog: "warehouse", schema: "mart", name: "orders", type: :table}
  end

  defp reliable_column(name, data_type, nullable?) do
    %{
      name: name,
      data_type: data_type,
      nullable?: nullable?,
      metadata: %{contract_nullability: :reliable}
    }
  end

  defp descriptor(relation) do
    %TargetDescriptor{
      target_id: "asset:Elixir.MyApp.Assets.Orders:asset",
      relation: relation,
      adapter: "Elixir.MyApp.Adapter",
      connection_identity: %{name: "warehouse", definition_module: nil},
      materialization: %{kind: "table"},
      write_semantics: %{mode: "replace"},
      execution_package_hash: String.duplicate("a", 64),
      manifest_schema_version: 13,
      runner_contract_version: 12,
      descriptor_hash: String.duplicate("b", 64)
    }
  end
end
