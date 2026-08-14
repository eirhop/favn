defmodule Favn.ExecutionPool.PolicySetTest do
  use ExUnit.Case, async: true

  alias Favn.ExecutionPool.Policy
  alias Favn.ExecutionPool.PolicySet

  test "normalizes a strict bounded policy catalogue" do
    assert {:ok, policies} =
             PolicySet.new(
               partner_api: [
                 max_concurrency: 3,
                 circuit_breaker: [failure_threshold: 4, probe_after_ms: 30_000]
               ]
             )

    assert %Policy{
             max_concurrency: 3,
             circuit_breaker: %{failure_threshold: 4, probe_after_ms: 30_000}
           } = policies["partner_api"]

    assert PolicySet.to_map(policies) == %{
             "partner_api" => %{
               "max_concurrency" => 3,
               "circuit_breaker" => %{
                 "failure_threshold" => 4,
                 "probe_after_ms" => 30_000
               }
             }
           }
  end

  test "rejects unknown keys, unsafe limits, duplicate names, and invalid names" do
    assert {:error, {:invalid_execution_pool_configuration, nil}} = PolicySet.new(nil)

    assert {:error,
            {:invalid_execution_pool_policy, "api",
             {:unknown_execution_pool_policy_keys, [:unexpected]}}} =
             PolicySet.new(api: [max_concurrency: 1, unexpected: true])

    assert {:error,
            {:invalid_execution_pool_policy, "api", {:invalid_execution_pool_max_concurrency, 0}}} =
             PolicySet.new(api: [max_concurrency: 0])

    assert {:error, {:duplicate_execution_pool_name, "api"}} =
             PolicySet.new(%{:api => [max_concurrency: 1], "api" => [max_concurrency: 2]})

    assert {:error, {:invalid_execution_pool_name, "bad pool"}} =
             PolicySet.new(%{"bad pool" => %{max_concurrency: 1}})

    assert {:error, {:duplicate_execution_pool_policy_keys, ["max_concurrency"]}} =
             Policy.new([{:max_concurrency, 1}, {:max_concurrency, 9}])

    assert {:error, {:duplicate_execution_pool_policy_keys, ["max_concurrency"]}} =
             Policy.new(%{"max_concurrency" => 9, max_concurrency: 1})
  end

  test "bounds the number of pools and fingerprints canonical content" do
    entries = Map.new(1..(PolicySet.maximum_pools() + 1), &{"pool-#{&1}", %{max_concurrency: 1}})

    assert {:error, {:too_many_execution_pools, 401, 400}} = PolicySet.new(entries)

    assert {:ok, first} = PolicySet.new(%{api: %{max_concurrency: 2}})
    assert {:ok, second} = PolicySet.new(%{"api" => %{"max_concurrency" => 2}})
    assert PolicySet.fingerprint(first) == PolicySet.fingerprint(second)
  end
end
