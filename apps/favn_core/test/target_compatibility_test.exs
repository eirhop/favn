defmodule Favn.TargetCompatibilityTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest.TargetDescriptor
  alias Favn.RelationRef
  alias Favn.TargetCompatibility
  alias Favn.TargetCompatibility.PhysicalFingerprint
  alias Favn.TargetCompatibility.Result
  alias Favn.Window.Spec, as: WindowSpec

  @package_a String.duplicate("a", 64)
  @package_b String.duplicate("b", 64)

  test "classifies a compatible target as ready" do
    active = descriptor()
    physical = physical_fingerprint()

    assert %Result{status: :ready, reason_code: :compatible, diff: %{}} =
             TargetCompatibility.classify(
               descriptor(description: "metadata changed"),
               active,
               physical.fingerprint,
               physical
             )
  end

  test "classifies an execution-package change as rebuild available" do
    active = descriptor()
    desired = descriptor(execution_package_hash: @package_b)
    physical = physical_fingerprint()

    assert %Result{
             status: :rebuild_available,
             reason_code: :transformation_changed,
             diff: %{descriptor: [%{field: :execution_package_hash}]}
           } =
             TargetCompatibility.classify(
               desired,
               active,
               physical.fingerprint,
               physical
             )
  end

  test "classifies every structural descriptor category as rebuild required" do
    active = descriptor()
    physical = physical_fingerprint()

    cases = [
      {:contract_order,
       descriptor(
         columns: [
           %{name: :label, type: :string, nullable?: true},
           %{name: :id, type: :integer, nullable?: false}
         ]
       ), :contract_fingerprint},
      {:contract_type, descriptor(columns: [%{name: :id, type: :string, nullable?: false}]),
       :contract_fingerprint},
      {:contract_nullability,
       descriptor(columns: [%{name: :id, type: :integer, nullable?: true}]),
       :contract_fingerprint},
      {:grain, descriptor(grain: %{by: [:label]}), :grain_fingerprint},
      {:materialization,
       descriptor(
         materialization:
           {:incremental,
            strategy: :delete_insert, unique_key: [:id], window_column: :partition_day}
       ), :materialization},
      {:relation, descriptor(relation_name: "renamed_orders"), :relation},
      {:adapter, descriptor(adapter: MyApp.OtherAdapter), :adapter},
      {:connection, descriptor(connection_module: MyApp.OtherWarehouse), :connection_identity},
      {:window_kind, descriptor(window: WindowSpec.new!(:month, timezone: "Etc/UTC")),
       :window_identity},
      {:window_timezone, descriptor(window: WindowSpec.new!(:day, timezone: "Europe/Oslo")),
       :window_identity}
    ]

    for {category, desired, changed_field} <- cases do
      result =
        TargetCompatibility.classify(desired, active, physical.fingerprint, physical)

      message = Atom.to_string(category)

      assert result.status == :rebuild_required, message
      assert result.reason_code == :incompatible_descriptor
      assert Enum.any?(result.diff.descriptor, &(&1.field == changed_field)), message
      refute Result.writable?(result)
    end
  end

  test "physical mismatch and missing active relation are unexpected drift" do
    descriptor = descriptor()
    recorded = physical_fingerprint()

    observed =
      physical_fingerprint(columns: [%{name: "id", data_type: "VARCHAR", nullable?: false}])

    assert %Result{
             status: :unexpected_drift,
             reason_code: :physical_fingerprint_mismatch
           } =
             TargetCompatibility.classify(descriptor, descriptor, recorded.fingerprint, observed)

    assert %Result{
             status: :unexpected_drift,
             reason_code: :physical_relation_missing,
             diff: %{physical: %{recorded_fingerprint: recorded_fingerprint}}
           } =
             TargetCompatibility.classify(
               descriptor,
               descriptor,
               recorded.fingerprint,
               :not_found
             )

    assert recorded_fingerprint == recorded.fingerprint
  end

  test "matching recorded bytes do not hide a wrong physical target identity" do
    descriptor = descriptor()
    observed = physical_fingerprint(relation_name: "other_orders")

    assert %Result{
             status: :unexpected_drift,
             reason_code: :physical_identity_mismatch,
             diff: %{physical_identity: [%{field: :relation}]}
           } =
             TargetCompatibility.classify(
               descriptor,
               descriptor,
               observed.fingerprint,
               observed
             )
  end

  test "matching recorded bytes do not hide a physical contract mismatch" do
    descriptor = descriptor()

    observed =
      physical_fingerprint(
        columns: [
          %{name: "id", data_type: "VARCHAR", nullable?: false},
          %{name: "label", data_type: "VARCHAR", nullable?: true}
        ]
      )

    assert %Result{
             status: :unexpected_drift,
             reason_code: :physical_identity_mismatch,
             diff: %{physical_identity: [%{field: :contract_fingerprint}]}
           } =
             TargetCompatibility.classify(
               descriptor,
               descriptor,
               observed.fingerprint,
               observed
             )
  end

  test "a non-table physical relation cannot satisfy a persisted target" do
    descriptor = descriptor()
    observed = physical_fingerprint(relation_kind: :view)

    assert %Result{
             status: :unexpected_drift,
             reason_code: :physical_identity_mismatch,
             diff: %{physical_identity: [%{field: :relation_kind}]}
           } =
             TargetCompatibility.classify(
               descriptor,
               descriptor,
               observed.fingerprint,
               observed
             )
  end

  test "unbound targets require operator decision only when a physical relation exists" do
    desired = descriptor()
    physical = physical_fingerprint()

    assert %Result{status: :uninitialized, reason_code: :no_active_generation} =
             result =
             TargetCompatibility.classify(desired, nil, nil, :not_found)

    assert Result.writable?(result)

    assert %Result{
             status: :operator_decision,
             reason_code: :unmanaged_physical_relation,
             diff: %{physical: %{observed_fingerprint: fingerprint}}
           } = TargetCompatibility.classify(desired, nil, nil, physical)

    assert fingerprint == physical.fingerprint
  end

  test "an active generation without a recorded physical fingerprint needs operator decision" do
    descriptor = descriptor()
    physical = physical_fingerprint()

    assert %Result{
             status: :operator_decision,
             reason_code: :active_physical_fingerprint_missing
           } = TargetCompatibility.classify(descriptor, descriptor, nil, physical)
  end

  defp descriptor(opts \\ []) do
    window = Keyword.get(opts, :window, WindowSpec.new!(:day, timezone: "Etc/UTC"))
    relation_name = Keyword.get(opts, :relation_name, "orders")
    materialization = Keyword.get(opts, :materialization, :table)
    package_hash = Keyword.get(opts, :execution_package_hash, @package_a)
    description = Keyword.get(opts, :description)

    columns =
      Keyword.get(opts, :columns, [
        %{name: :id, type: :integer, nullable?: false},
        %{name: :label, type: :string, nullable?: true}
      ])

    grain = Keyword.get(opts, :grain, %{by: [:id], description: description})

    asset = %{
      ref: {MyApp.Orders, :asset},
      type: :sql,
      relation: RelationRef.new!(connection: :warehouse, schema: "mart", name: relation_name),
      materialization: materialization,
      execution_package_hash: package_hash,
      assurance: %{
        contract: %{
          grain: grain,
          columns: Enum.map(columns, &Map.put(&1, :description, description)),
          unique_keys: [%{columns: [:id]}]
        }
      },
      window: window,
      coverage: nil
    }

    TargetDescriptor.from_asset(asset,
      connection_definitions: %{
        warehouse: %{
          adapter: Keyword.get(opts, :adapter, MyApp.Adapter),
          module: Keyword.get(opts, :connection_module, MyApp.Warehouse)
        }
      },
      manifest_schema_version: 12,
      runner_contract_version: 12
    )
  end

  defp physical_fingerprint(opts \\ []) do
    columns =
      Keyword.get(opts, :columns, [
        %{name: "id", data_type: "BIGINT", nullable?: false},
        %{name: "label", data_type: "VARCHAR", nullable?: true}
      ])

    {:ok, fingerprint} =
      PhysicalFingerprint.new(
        adapter: MyApp.Adapter,
        relation: %{
          catalog: nil,
          schema: "mart",
          name: Keyword.get(opts, :relation_name, "orders"),
          type: Keyword.get(opts, :relation_kind, :table)
        },
        columns: columns
      )

    fingerprint
  end
end
