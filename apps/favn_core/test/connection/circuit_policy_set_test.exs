defmodule Favn.Connection.CircuitPolicySetTest do
  use ExUnit.Case, async: true

  alias Favn.Connection.CircuitPolicySet
  alias Favn.RuntimeConfig.Ref

  test "extracts only circuit policy and never retains runtime values or secrets" do
    config = %{
      warehouse: %{
        url: Ref.env!("WAREHOUSE_URL"),
        password: Ref.secret_env!("WAREHOUSE_PASSWORD"),
        circuit_breaker: %{failure_threshold: 5, probe_after_ms: 10_000}
      },
      analytics: %{url: "postgres://runner-local"}
    }

    assert {:ok, policies} = CircuitPolicySet.from_connection_config(config)

    assert CircuitPolicySet.to_map(policies) == %{
             "warehouse" => %{
               "failure_threshold" => 5,
               "probe_after_ms" => 10_000
             }
           }

    encoded = inspect(policies)
    refute encoded =~ "WAREHOUSE_URL"
    refute encoded =~ "WAREHOUSE_PASSWORD"
    refute encoded =~ "postgres://runner-local"
  end

  test "rejects ambiguous names and invalid policies" do
    assert {:error, {:duplicate_connection_name, "warehouse"}} =
             CircuitPolicySet.from_connection_config(%{
               :warehouse => %{},
               "warehouse" => %{}
             })

    assert {:error,
            {:invalid_connection_circuit_policy, "warehouse",
             :invalid_circuit_breaker_failure_threshold}} =
             CircuitPolicySet.from_connection_config(%{
               warehouse: %{circuit_breaker: %{failure_threshold: 0, probe_after_ms: 10}}
             })

    assert {:error,
            {:invalid_connection_circuit_policy, "warehouse",
             {:duplicate_connection_options, ["circuit_breaker"]}}} =
             CircuitPolicySet.from_connection_config(%{
               warehouse: %{
                 "circuit_breaker" => %{failure_threshold: 4, probe_after_ms: 20},
                 circuit_breaker: %{failure_threshold: 3, probe_after_ms: 10}
               }
             })
  end

  test "invalid runner-local configuration never appears in errors" do
    secret = "TOP-SECRET-CONNECTION-VALUE"
    malformed = [warehouse: [:not_a_pair, password: secret]]

    assert {:error,
            {:invalid_connection_circuit_policy, "warehouse", :invalid_circuit_breaker_policy}} =
             error = CircuitPolicySet.from_connection_config(malformed)

    refute inspect(error) =~ secret
  end

  test "persisted policies round-trip with a stable fingerprint" do
    input = %{"warehouse" => %{"failure_threshold" => 3, "probe_after_ms" => 5_000}}

    assert {:ok, policies} = CircuitPolicySet.new(input)
    assert CircuitPolicySet.to_map(policies) == input
    assert CircuitPolicySet.fingerprint(policies) == CircuitPolicySet.fingerprint(policies)
  end

  test "the policy bound does not limit connections without circuit policy" do
    connections =
      Map.new(1..(CircuitPolicySet.maximum_connections() + 1), fn index ->
        {"connection_#{index}", %{url: "runner-local"}}
      end)

    assert {:ok, %{}} = CircuitPolicySet.from_connection_config(connections)
  end

  test "rejects policy catalogues above the durable bound" do
    max = CircuitPolicySet.maximum_connections()
    count = max + 1

    persisted =
      Map.new(1..count, fn index ->
        {"connection_#{index}", %{failure_threshold: 3, probe_after_ms: 5_000}}
      end)

    authoring =
      Map.new(persisted, fn {name, policy} ->
        {name, %{circuit_breaker: policy}}
      end)

    assert {:error, {:too_many_connection_circuit_policies, ^count, ^max}} =
             CircuitPolicySet.new(persisted)

    assert {:error, {:too_many_connection_circuit_policies, ^count, ^max}} =
             CircuitPolicySet.from_connection_config(authoring)
  end
end
