defmodule FavnOrchestrator.RunnerPoolsTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.RunnerPools
  alias FavnOrchestrator.RuntimeConfig

  test "normalizes arbitrary provider-neutral pool policies" do
    config = [
      duckdb: [mode: :elastic],
      pure_elixir: [mode: :resident],
      gpu_a10: [mode: :elastic, idle_grace_ms: 45_000]
    ]

    assert {:ok, pools} = RunnerPools.normalize(config)
    assert pools["duckdb"] == %{mode: :elastic, idle_grace_ms: 15_000}
    assert pools["pure_elixir"] == %{mode: :resident, idle_grace_ms: :infinity}
    assert pools["gpu_a10"] == %{mode: :elastic, idle_grace_ms: 45_000}
  end

  test "rejects infrastructure details and resident idle grace" do
    assert {:error, {:invalid_runner_pool_policy, :duckdb, _reason}} =
             RunnerPools.normalize(duckdb: [mode: :elastic, cpu: 4])

    assert {:error, {:invalid_runner_pool_policy, :resident, :resident_idle_grace_not_allowed}} =
             RunnerPools.normalize(resident: [mode: :resident, idle_grace_ms: 1])
  end

  test "runtime configuration freezes normalized pool policy" do
    assert {:ok, %RuntimeConfig{} = config} =
             RuntimeConfig.normalize(
               runner_pools: [private_network: [mode: :elastic, idle_grace_ms: 30_000]]
             )

    assert config.runner_pools == %{
             "private_network" => %{mode: :elastic, idle_grace_ms: 30_000}
           }
  end

  test "normalizes runtime string names without creating atoms" do
    pool_name = "customer_pool_" <> Integer.to_string(System.unique_integer([:positive]))
    refute existing_atom?(pool_name)

    assert {:ok, %{^pool_name => %{mode: :elastic, idle_grace_ms: 15_000}}} =
             RunnerPools.normalize(%{pool_name => %{"mode" => "elastic"}})

    refute existing_atom?(pool_name)
  end

  defp existing_atom?(value) do
    _atom = String.to_existing_atom(value)
    true
  rescue
    ArgumentError -> false
  end
end
