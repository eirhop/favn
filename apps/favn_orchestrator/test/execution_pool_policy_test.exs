defmodule FavnOrchestrator.ExecutionPoolPolicyTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest
  alias FavnOrchestrator.ExecutionPoolPolicy

  test "requires explicit approval before activating manifest defaults" do
    manifest = manifest(%{api: %{max_concurrency: 3}})

    assert {:error, :execution_pool_policy_approval_required} =
             ExecutionPoolPolicy.resolve(manifest, %{}, %{}, nil)

    assert {:ok, resolved} =
             ExecutionPoolPolicy.resolve(
               manifest,
               %{"schema_version" => 1},
               %{},
               %{approve_manifest_defaults: true}
             )

    assert resolved.effective["api"].max_concurrency == 3

    assert get_in(resolved.configuration, ["execution_pool_policy", "sources"]) == %{
             "api" => "manifest"
           }
  end

  test "preserves an operator override across later manifest activations" do
    first_manifest = manifest(%{api: %{max_concurrency: 3}})

    assert {:ok, first} =
             ExecutionPoolPolicy.resolve(first_manifest, %{}, %{}, %{
               approve_manifest_defaults: true,
               overrides: %{api: %{max_concurrency: 7}}
             })

    next_manifest = manifest(%{api: %{max_concurrency: 4}, warehouse: %{max_concurrency: 1}})

    assert {:ok, next} =
             ExecutionPoolPolicy.resolve(
               next_manifest,
               %{},
               first.configuration,
               %{approve_manifest_defaults: true}
             )

    assert next.effective["api"].max_concurrency == 7
    assert next.effective["warehouse"].max_concurrency == 1

    assert get_in(next.configuration, ["execution_pool_policy", "sources"]) == %{
             "api" => "operator_override",
             "warehouse" => "manifest"
           }
  end

  test "reset returns a pool to its manifest default" do
    manifest = manifest(%{api: %{max_concurrency: 3}})

    assert {:ok, overridden} =
             ExecutionPoolPolicy.resolve(manifest, %{}, %{}, %{
               approve_manifest_defaults: true,
               overrides: %{api: %{max_concurrency: 7}}
             })

    assert {:ok, reset} =
             ExecutionPoolPolicy.resolve(manifest, %{}, overridden.configuration, %{
               approve_manifest_defaults: true,
               reset: [:api]
             })

    assert reset.effective["api"].max_concurrency == 3

    assert get_in(reset.configuration, ["execution_pool_policy", "operator_overrides"]) ==
             %{}
  end

  test "removed overrides become inactive and never silently revive" do
    manifest = manifest(%{api: %{max_concurrency: 3}})

    assert {:ok, overridden} =
             ExecutionPoolPolicy.resolve(manifest, %{}, %{}, %{
               approve_manifest_defaults: true,
               overrides: %{api: %{max_concurrency: 7}}
             })

    assert {:ok, removed} =
             ExecutionPoolPolicy.resolve(
               manifest(%{}),
               %{},
               overridden.configuration,
               %{approve_manifest_defaults: true}
             )

    assert removed.effective == %{}

    assert get_in(removed.configuration, [
             "execution_pool_policy",
             "orphaned_overrides",
             "api",
             "max_concurrency"
           ]) ==
             7

    assert {:ok, returned} =
             ExecutionPoolPolicy.resolve(
               manifest(%{api: %{max_concurrency: 2}}),
               %{},
               removed.configuration,
               %{approve_manifest_defaults: true}
             )

    assert returned.effective["api"].max_concurrency == 2

    assert get_in(returned.configuration, ["execution_pool_policy", "orphaned_overrides"]) ==
             %{}
  end

  test "an operator can explicitly discard an inactive removed-pool override" do
    manifest = manifest(%{api: %{max_concurrency: 3}})

    assert {:ok, overridden} =
             ExecutionPoolPolicy.resolve(manifest, %{}, %{}, %{
               approve_manifest_defaults: true,
               overrides: %{api: %{max_concurrency: 7}}
             })

    assert {:ok, discarded} =
             ExecutionPoolPolicy.resolve(
               manifest(%{}),
               %{},
               overridden.configuration,
               %{approve_manifest_defaults: true, discard_orphaned: [:api]}
             )

    assert get_in(discarded.configuration, [
             "execution_pool_policy",
             "orphaned_overrides"
           ]) == %{}

    assert {:error, {:unknown_orphaned_execution_pool_overrides, ["missing"]}} =
             ExecutionPoolPolicy.resolve(
               manifest(%{}),
               %{},
               discarded.configuration,
               %{approve_manifest_defaults: true, discard_orphaned: [:missing]}
             )
  end

  test "rejects partial, unknown, or unapproved override requests" do
    manifest = manifest(%{api: %{max_concurrency: 3}})

    assert {:error, {:unknown_execution_pool_overrides, ["missing"]}} =
             ExecutionPoolPolicy.resolve(manifest, %{}, %{}, %{
               approve_manifest_defaults: true,
               overrides: %{missing: %{max_concurrency: 2}}
             })

    assert {:error,
            {:invalid_execution_pool_policy, "api",
             {:invalid_execution_pool_max_concurrency, nil}}} =
             ExecutionPoolPolicy.resolve(manifest, %{}, %{}, %{
               approve_manifest_defaults: true,
               overrides: %{api: %{circuit_breaker: nil}}
             })
  end

  defp manifest(execution_pools), do: %Manifest{execution_pools: execution_pools}
end
