defmodule FavnOrchestrator.ResourceCircuitsTest do
  use ExUnit.Case, async: false

  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Index
  alias Favn.RelationRef
  alias FavnOrchestrator.Persistence.Commands.ResourceCircuitRequest
  alias FavnOrchestrator.Persistence.Runtime, as: PersistenceRuntime
  alias FavnOrchestrator.Persistence.Stores
  alias FavnOrchestrator.ResourceCircuits
  alias FavnOrchestrator.RunState

  defmodule Store do
    def acquire(command) do
      Process.put(:resource_circuits_acquire, command)

      {:ok, %FavnOrchestrator.Persistence.Results.ResourceCircuitAdmission{status: :allowed}}
    end
  end

  setup do
    stores = struct(Stores, resource_circuits: Store)
    runtime = %PersistenceRuntime{backend: __MODULE__, options: [], stores: stores}
    start_supervised!({PersistenceRuntime, runtime})
    :ok
  end

  test "acquires a connection circuit from the normalized run snapshot" do
    previous = Application.get_env(:favn, :connections)

    on_exit(fn ->
      if is_nil(previous),
        do: Application.delete_env(:favn, :connections),
        else: Application.put_env(:favn, :connections, previous)
    end)

    Application.put_env(:favn, :connections,
      warehouse: [circuit_breaker: [failure_threshold: 99, probe_after_ms: 99]]
    )

    connection_circuits =
      1..(Favn.Connection.CircuitPolicySet.maximum_connections() - 1)
      |> Map.new(fn index ->
        {"unused_#{index}", %{"failure_threshold" => 2, "probe_after_ms" => 1_000}}
      end)
      |> Map.put("warehouse", %{"failure_threshold" => 5, "probe_after_ms" => 10_000})

    asset_ref = {__MODULE__.Asset, :orders}

    asset = %Asset{
      ref: asset_ref,
      module: elem(asset_ref, 0),
      name: :orders,
      relation: RelationRef.new!(connection: :warehouse, name: "orders")
    }

    index = %Index{assets_by_ref: %{asset_ref => asset}}

    run = %RunState{
      id: "run-resource-circuit",
      workspace_id: "workspace-resource-circuit",
      timeout_ms: 30_000,
      metadata: %{
        execution_pool_policy: %{},
        connection_circuit_policy: connection_circuits
      }
    }

    work = %RunnerWork{
      run_id: run.id,
      asset_ref: asset_ref,
      asset_step_id: "step-resource-circuit"
    }

    assert {:ok, []} = ResourceCircuits.acquire(run, work, index)

    assert %{requests: [%ResourceCircuitRequest{} = request]} =
             Process.get(:resource_circuits_acquire)

    assert request.resource.kind == :connection
    assert request.resource.name == "warehouse"
    assert request.policy.failure_threshold == 5
    assert request.policy.probe_after_ms == 10_000
  end
end
