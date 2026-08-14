defmodule FavnOrchestrator.RebuildExecutionWorkerTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.Persistence.Results.RebuildLease
  alias FavnOrchestrator.Persistence.Results.RebuildOperation
  alias FavnOrchestrator.Persistence.Results.TargetOperationLock
  alias FavnOrchestrator.Persistence.Runtime
  alias FavnOrchestrator.Persistence.Stores
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.RebuildExecutionWorker

  defmodule RebuildStore do
    def renew_operation_lease(command) do
      Agent.get_and_update(agent(), fn state ->
        operation = state.operation

        if operation.dispatcher.owner == command.owner_id and
             operation.dispatcher.fencing_token == command.fencing_token do
          next = %{state | operation_renewals: state.operation_renewals + 1}
          {:ok, next}
        else
          {{:error, :fenced}, state}
        end
      end)
    end

    defp agent,
      do: Application.fetch_env!(:favn_orchestrator, :rebuild_execution_worker_test_agent)
  end

  defmodule LockStore do
    def renew_many(command) do
      Agent.get_and_update(agent(), fn state ->
        locks =
          Enum.map(state.locks, fn lock ->
            %{
              lock
              | lease_expires_at:
                  DateTime.add(DateTime.utc_now(), command.lease_duration_ms, :millisecond),
                version: lock.version + 1
            }
          end)

        {{:ok, locks}, %{state | locks: locks, lock_renewals: state.lock_renewals + 1}}
      end)
    end

    defp agent,
      do: Application.fetch_env!(:favn_orchestrator, :rebuild_execution_worker_test_agent)
  end

  setup do
    start_named_supervision()
    now = DateTime.utc_now()

    operation = %RebuildOperation{
      workspace_id: "workspace-execution-worker",
      operation_id: "rebuild-execution-worker",
      root_target_id: "target-execution-worker",
      manifest_version_id: "manifest-execution-worker",
      plan_hash: String.duplicate("a", 64),
      plan_version: 1,
      plan_payload: %{},
      actor_id: "operator",
      reason: "test execution worker",
      idempotency_key: "rebuild-execution-worker",
      evaluated_at: now,
      action_count: 1,
      window_count: 1,
      state: :activating,
      phase: :activating,
      version: 1,
      dispatcher: %RebuildLease{
        owner: "dispatcher-owner",
        fencing_token: 7,
        expires_at: DateTime.add(now, 300, :millisecond)
      }
    }

    locks = [
      %TargetOperationLock{
        workspace_id: operation.workspace_id,
        target_id: operation.root_target_id,
        operation_id: operation.operation_id,
        operation_type: :rebuild,
        fencing_token: 11,
        lease_owner: operation.operation_id,
        lease_expires_at: DateTime.add(now, 300, :millisecond),
        version: 1,
        inserted_at: now,
        updated_at: now
      }
    ]

    {:ok, agent} =
      Agent.start_link(fn ->
        %{operation: operation, locks: locks, operation_renewals: 0, lock_renewals: 0}
      end)

    Application.put_env(:favn_orchestrator, :rebuild_execution_worker_test_agent, agent)

    stores =
      struct(Stores,
        rebuilds: RebuildStore,
        target_operation_locks: LockStore
      )

    start_supervised!({Runtime, %Runtime{backend: __MODULE__, options: [], stores: stores}})

    on_exit(fn ->
      Application.delete_env(:favn_orchestrator, :rebuild_execution_worker_test_agent)
    end)

    {:ok, context} =
      WorkspaceContext.new("workspace-execution-worker", "operator", [:workspace_admin])

    %{agent: agent, context: context, locks: locks, operation: operation}
  end

  test "a cold runner wait longer than the lease keeps both durable fences live", fixture do
    parent = self()

    execute = fn _context, operation, _locks, _state ->
      send(parent, {:execution_started, operation.operation_id, self()})

      receive do
        :finish_execution -> :ok
      end
    end

    opts = [
      owner_id: "dispatcher-owner",
      lease_duration_ms: 300,
      execute: execute
    ]

    assert {:ok, worker} =
             RebuildExecutionWorker.ensure(
               fixture.context,
               fixture.operation,
               fixture.locks,
               opts
             )

    monitor = Process.monitor(worker)

    assert_receive {:execution_started, "rebuild-execution-worker", execution_task}

    assert {:ok, ^worker} =
             RebuildExecutionWorker.ensure(
               fixture.context,
               fixture.operation,
               fixture.locks,
               opts
             )

    Process.sleep(750)

    assert Process.alive?(worker)
    assert Process.alive?(execution_task)

    assert %{operation_renewals: operation_renewals, lock_renewals: lock_renewals} =
             Agent.get(fixture.agent, & &1)

    assert operation_renewals >= 2
    assert lock_renewals >= 2

    assert RebuildExecutionWorker.active_operation_ids(fixture.context.workspace_id) == [
             fixture.operation.operation_id
           ]

    send(execution_task, :finish_execution)
    assert_receive {:DOWN, ^monitor, :process, ^worker, :normal}
  end

  defp start_named_supervision do
    unless Process.whereis(FavnOrchestrator.RebuildExecutionRegistry) do
      start_supervised!(
        {Registry, keys: :unique, name: FavnOrchestrator.RebuildExecutionRegistry}
      )
    end

    unless Process.whereis(FavnOrchestrator.RebuildExecutionSupervisor) do
      start_supervised!(
        {DynamicSupervisor,
         strategy: :one_for_one, name: FavnOrchestrator.RebuildExecutionSupervisor}
      )
    end
  end
end
