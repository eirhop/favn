defmodule FavnOrchestrator.ConnectionCircuitPolicyTest do
  use ExUnit.Case, async: true

  alias Favn.Connection.CircuitPolicySet
  alias Favn.Manifest
  alias FavnOrchestrator.ConnectionCircuitPolicy

  test "persists and validates one immutable manifest policy snapshot" do
    assert {:ok, policies} =
             CircuitPolicySet.new(%{
               warehouse: %{failure_threshold: 5, probe_after_ms: 10_000}
             })

    manifest = %Manifest{connection_circuits: policies}

    assert {:ok, configuration} =
             ConnectionCircuitPolicy.put(%{"schema_version" => 2}, manifest)

    assert configuration["schema_version"] == 3
    assert {:ok, ^policies} = ConnectionCircuitPolicy.effective(configuration)

    assert {:ok,
            [
              %{
                name: "warehouse",
                failure_threshold: 5,
                probe_after_ms: 10_000,
                source: "manifest"
              }
            ]} = ConnectionCircuitPolicy.diagnostics(configuration)
  end

  test "rejects tampered policy content and unknown block keys" do
    assert {:ok, policies} =
             CircuitPolicySet.new(%{
               warehouse: %{failure_threshold: 5, probe_after_ms: 10_000}
             })

    assert {:ok, configuration} =
             ConnectionCircuitPolicy.put(%{}, %Manifest{connection_circuits: policies})

    tampered =
      put_in(
        configuration,
        ["connection_circuit_policy", "effective", "warehouse", "failure_threshold"],
        6
      )

    assert {:error, :connection_circuit_policy_fingerprint_mismatch} =
             ConnectionCircuitPolicy.effective(tampered)

    unknown = put_in(configuration, ["connection_circuit_policy", "future"], true)

    assert {:error, {:unknown_connection_circuit_policy_key, "future"}} =
             ConnectionCircuitPolicy.effective(unknown)
  end

  test "legacy deployment schemas have an explicit empty policy" do
    assert {:ok, %{}} = ConnectionCircuitPolicy.effective(%{"schema_version" => 1})
    assert {:ok, %{}} = ConnectionCircuitPolicy.effective(%{"schema_version" => 2})
  end
end
