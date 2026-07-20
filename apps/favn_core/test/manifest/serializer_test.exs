defmodule Favn.Manifest.SerializerTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest.Build
  alias Favn.Manifest.Serializer
  alias Favn.RuntimeConfig.Ref
  alias Favn.SQL.Contract

  test "encodes canonical json with sorted keys" do
    manifest = %{schema_version: 9, runner_contract_version: 9, z: 1, a: 2}

    assert {:ok, encoded} = Serializer.encode_manifest(manifest)
    assert encoded == ~s|{"a":2,"runner_contract_version":9,"schema_version":9,"z":1}|
  end

  test "drops build-only keys from encoded payload" do
    manifest = %{
      schema_version: 9,
      runner_contract_version: 9,
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
      Build.new(%{schema_version: 9, runner_contract_version: 9, assets: []},
        diagnostics: ["ignored"]
      )

    assert {:ok, encoded} = Serializer.encode_manifest(build)
    assert {:ok, decoded} = Serializer.decode_manifest(encoded)
    assert decoded["schema_version"] == 9
    refute Map.has_key?(decoded, "diagnostics")
  end

  test "encodes tuples structurally without guessing reference semantics" do
    manifest = %{
      materialization: {:incremental, strategy: :delete_insert, window_column: :event_date}
    }

    assert {:ok, encoded} = Serializer.encode_manifest(manifest)
    assert {:ok, decoded} = Serializer.decode_manifest(encoded)

    assert decoded["materialization"] == [
             "incremental",
             [["strategy", "delete_insert"], ["window_column", "event_date"]]
           ]
  end

  test "encodes runtime config refs without resolved values" do
    manifest = %{
      schema_version: 9,
      runner_contract_version: 9,
      assets: [
        %{
          ref: {__MODULE__, :asset},
          runtime_config: %{
            source_system: %{
              segment_id: Ref.env!("SOURCE_SYSTEM_SEGMENT_ID"),
              token: Ref.secret_env!("SOURCE_SYSTEM_TOKEN")
            }
          }
        }
      ]
    }

    assert {:ok, encoded} = Serializer.encode_manifest(manifest)
    assert encoded =~ "SOURCE_SYSTEM_SEGMENT_ID"
    assert encoded =~ "SOURCE_SYSTEM_TOKEN"
    assert encoded =~ ~s|"secret?":true|
    refute encoded =~ "resolved-token-value"
  end

  test "serializes the canonical ordered row-count list" do
    single =
      Contract.new!(%{
        columns: [%{name: :id, type: :integer}],
        row_counts: [[min: 1]]
      })

    assert {:ok, single_encoded} = Serializer.encode_manifest(single)

    assert {:ok, %{"row_counts" => [%{"min" => 1}]} = single_decoded} =
             Serializer.decode_manifest(single_encoded)

    refute Map.has_key?(single_decoded, "row_count")

    multiple =
      Contract.new!(%{
        columns: single.columns,
        row_counts: [[equals: 0], [min: 1, on_violation: :warn]]
      })

    assert {:ok, multiple_encoded} = Serializer.encode_manifest(multiple)

    assert {:ok, %{"row_counts" => row_counts} = multiple_decoded} =
             Serializer.decode_manifest(multiple_encoded)

    assert Enum.map(row_counts, &{&1["equals"], &1["min"]}) == [{0, nil}, {nil, 1}]
    refute Map.has_key?(multiple_decoded, "row_count")
  end
end
