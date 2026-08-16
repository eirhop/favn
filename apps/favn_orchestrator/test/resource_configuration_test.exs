defmodule FavnOrchestrator.ResourceConfigurationTest do
  use ExUnit.Case, async: true

  alias Favn.Resource.Ref
  alias FavnOrchestrator.ResourceConfiguration

  @policies %{
    execution_pools: %{
      partner_api: [
        max_concurrency: 2,
        circuit_breaker: [failure_threshold: 3, probe_after_ms: 5_000]
      ]
    },
    connection_circuits: %{
      "warehouse" => %{"failure_threshold" => 5, "probe_after_ms" => 10_000}
    }
  }

  test "reads pool and connection policy only from the explicit run snapshot" do
    assert {:ok, configuration} = ResourceConfiguration.new(@policies)

    assert {:ok, %{failure_threshold: 3, probe_after_ms: 5_000}} =
             ResourceConfiguration.circuit_breaker(
               Ref.new!(:execution_pool, :partner_api),
               configuration
             )

    assert {:ok, %{failure_threshold: 5, probe_after_ms: 10_000}} =
             ResourceConfiguration.circuit_breaker(
               Ref.new!(:connection, :warehouse),
               configuration
             )
  end

  test "returns nil for an unconfigured resource" do
    assert {:ok, configuration} = ResourceConfiguration.new(@policies)

    assert {:ok, nil} =
             ResourceConfiguration.circuit_breaker(
               Ref.new!(:connection, :analytics),
               configuration
             )
  end

  test "rejects malformed snapshotted policy" do
    assert {:error,
            {:invalid_connection_circuit_policy, "warehouse",
             :invalid_circuit_breaker_failure_threshold}} =
             ResourceConfiguration.new(
               put_in(@policies, [:connection_circuits, "warehouse", "failure_threshold"], 0)
             )
  end
end
