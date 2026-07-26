defmodule FavnOrchestrator.RunsTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.Persistence.Runtime
  alias FavnOrchestrator.Persistence.Stores
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.Runs

  defmodule Store do
    def request_cancellation(command) do
      send(Process.get(:runs_test_pid), {:cancellation_command, command})
      {:ok, :captured}
    end
  end

  setup do
    Process.put(:runs_test_pid, self())

    stores =
      struct(Stores,
        registry: Store,
        runs: Store,
        run_ownership: Store,
        scheduler: Store,
        admission: Store,
        resource_circuits: Store,
        target_generations: Store,
        rebuilds: Store,
        target_operation_locks: Store,
        materialization: Store,
        backfills: Store,
        operator_reads: Store,
        logs: Store,
        identity: Store,
        maintenance: Store
      )

    start_supervised!({Runtime, %Runtime{backend: __MODULE__, options: [], stores: stores}})

    {:ok, context} = WorkspaceContext.new("workspace", "runs-test", [:workspace_admin])

    %{context: context}
  end

  test "cancellation derives a bounded deterministic command id from run and reason", %{
    context: context
  } do
    reason = %{actor_id: "actor", requested_by: :operator}

    assert {:ok, :captured} = Runs.request_cancellation(context, "run-1", reason)
    assert_receive {:cancellation_command, first}

    assert {:ok, :captured} = Runs.request_cancellation(context, "run-1", reason)
    assert_receive {:cancellation_command, second}

    assert first.command_id == second.command_id
    assert "cancel:" <> digest = first.command_id
    assert byte_size(digest) == 64
  end
end
