defmodule FavnOrchestrator.ResourceConfigurationTest do
  use ExUnit.Case, async: false

  alias Favn.Resource.Ref
  alias FavnOrchestrator.ResourceConfiguration

  setup do
    previous_connections = Application.get_env(:favn, :connections)

    on_exit(fn ->
      restore(:connections, previous_connections)
    end)

    :ok
  end

  test "reads persisted pool policy and colocated connection policy" do
    Application.put_env(:favn, :connections, %{
      "warehouse" => %{
        "circuit_breaker" => %{"failure_threshold" => 5, "probe_after_ms" => 10_000}
      }
    })

    assert {:ok, %{failure_threshold: 3, probe_after_ms: 5_000}} =
             ResourceConfiguration.circuit_breaker(
               Ref.new!(:execution_pool, :partner_api),
               partner_api: [
                 max_concurrency: 2,
                 circuit_breaker: [failure_threshold: 3, probe_after_ms: 5_000]
               ]
             )

    assert {:ok, %{failure_threshold: 5, probe_after_ms: 10_000}} =
             ResourceConfiguration.circuit_breaker(Ref.new!(:connection, :warehouse))
  end

  test "returns nil for an unconfigured resource" do
    Application.put_env(:favn, :connections, [])

    assert {:ok, nil} =
             ResourceConfiguration.circuit_breaker(Ref.new!(:connection, :warehouse))
  end

  test "requires an explicit deployment policy for execution pools" do
    assert {:error, :execution_pool_policy_required} =
             ResourceConfiguration.circuit_breaker(Ref.new!(:execution_pool, :partner_api))
  end

  defp restore(key, nil), do: Application.delete_env(:favn, key)
  defp restore(key, value), do: Application.put_env(:favn, key, value)
end
