defmodule FavnOrchestrator.ResourceConfigurationTest do
  use ExUnit.Case, async: false

  alias Favn.Resource.Ref
  alias FavnOrchestrator.ResourceConfiguration

  setup do
    previous_pools = Application.get_env(:favn, :execution_pools)
    previous_connections = Application.get_env(:favn, :connections)

    on_exit(fn ->
      restore(:execution_pools, previous_pools)
      restore(:connections, previous_connections)
    end)

    :ok
  end

  test "reads colocated pool and connection circuit policies" do
    Application.put_env(:favn, :execution_pools,
      partner_api: [
        max_concurrency: 2,
        circuit_breaker: [failure_threshold: 3, probe_after_ms: 5_000]
      ]
    )

    Application.put_env(:favn, :connections, %{
      "warehouse" => %{
        "circuit_breaker" => %{"failure_threshold" => 5, "probe_after_ms" => 10_000}
      }
    })

    assert {:ok, %{failure_threshold: 3, probe_after_ms: 5_000}} =
             ResourceConfiguration.circuit_breaker(Ref.new!(:execution_pool, :partner_api))

    assert {:ok, %{failure_threshold: 5, probe_after_ms: 10_000}} =
             ResourceConfiguration.circuit_breaker(Ref.new!(:connection, :warehouse))
  end

  test "returns nil for an unconfigured resource" do
    Application.put_env(:favn, :connections, [])

    assert {:ok, nil} =
             ResourceConfiguration.circuit_breaker(Ref.new!(:connection, :warehouse))
  end

  defp restore(key, nil), do: Application.delete_env(:favn, key)
  defp restore(key, value), do: Application.put_env(:favn, key, value)
end
