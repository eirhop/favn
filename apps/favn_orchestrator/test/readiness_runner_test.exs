defmodule FavnOrchestrator.ReadinessRunnerTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Readiness

  test "readiness accepts an available registry with zero runners" do
    check =
      runner_check({:ok, %{available?: true, registered: 0, partitions: %{}}})

    assert %{status: :ok, details: details} = check
    assert details.registered == 0
  end

  test "readiness reports an unavailable registry" do
    assert %{status: :error, error: :runner_registry_not_running} =
             runner_check({:error, :runner_registry_not_running})
  end

  test "readiness accepts healthy capacity partitions with zero demand" do
    check = capacity_check({:ok, %{partition_count: 1, unhealthy_partition_count: 0}})

    assert %{status: :ok, details: %{partitions: 1, unhealthy_partitions: 0}} = check
  end

  test "readiness rejects a stale capacity partition" do
    check = capacity_check({:ok, %{partition_count: 300, unhealthy_partition_count: 1}})

    assert %{status: :error, error: %{partitions: 300, unhealthy_partitions: 1}} = check
  end

  test "readiness exposes the stable production check names" do
    assert Readiness.readiness(
             storage_snapshot: {:error, :not_used},
             capacity_snapshot: {:ok, %{partition_count: 0, unhealthy_partition_count: 0}},
             runner_snapshot: {:error, :runner_registry_not_running}
           ).checks
           |> Enum.map(& &1.name) == [
             :config,
             :api,
             :storage,
             :schema,
             :scheduler,
             :lifecycle,
             :runner_capacity,
             :runner_registry
           ]
  end

  defp runner_check(snapshot) do
    Readiness.readiness(
      runner_snapshot: snapshot,
      capacity_snapshot: {:ok, %{partition_count: 0, unhealthy_partition_count: 0}},
      storage_snapshot: {:error, :not_used}
    )
    |> Map.fetch!(:checks)
    |> Enum.find(&(&1.name == :runner_registry))
  end

  defp capacity_check(snapshot) do
    Readiness.readiness(
      runner_snapshot: {:ok, %{available?: true, registered: 0, partitions: %{}}},
      capacity_snapshot: snapshot,
      storage_snapshot: {:error, :not_used}
    )
    |> Map.fetch!(:checks)
    |> Enum.find(&(&1.name == :runner_capacity))
  end
end
