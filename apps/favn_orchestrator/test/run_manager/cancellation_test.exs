defmodule FavnOrchestrator.RunManager.CancellationTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.Persistence.Commands.RequestRunCancellation
  alias FavnOrchestrator.Persistence.Results.RunCommitted
  alias FavnOrchestrator.Persistence.Runtime
  alias FavnOrchestrator.Persistence.Stores
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.Events
  alias FavnOrchestrator.RunManager
  alias FavnOrchestrator.RunState

  defmodule Store do
    alias FavnOrchestrator.Persistence.Commands.RequestRunCancellation

    def request_cancellation(%RequestRunCancellation{} = command) do
      send(Process.get(:run_manager_cancellation_test_pid), {:request_cancellation, command})
      {:ok, Process.get(:run_manager_cancellation_committed)}
    end

    def get_run(_query), do: {:ok, Process.get(:run_manager_cancellation_current_run)}
  end

  setup do
    Process.put(:run_manager_cancellation_test_pid, self())

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
    start_supervised!({Phoenix.PubSub, name: Events.pubsub_name()})
    start_supervised!(RunManager)

    {:ok, context} =
      WorkspaceContext.new("workspace", "run-manager-cancellation-test", [:workspace_admin])

    :ok = Events.subscribe_run(context.workspace_id, "run-replayed-cancellation")

    on_exit(fn ->
      Process.delete(:run_manager_cancellation_test_pid)
      Process.delete(:run_manager_cancellation_committed)
      Process.delete(:run_manager_cancellation_current_run)
    end)

    %{context: context}
  end

  test "an exact nonterminal replay resumes runner notification without republishing", %{
    context: context
  } do
    stale = run(:pending)
    current = run(:running)
    put_replayed_commit(stale)
    Process.put(:run_manager_cancellation_current_run, current)

    test_pid = self()

    run_pid =
      spawn(fn ->
        receive do
          message -> send(test_pid, {:run_server_message, message})
        end
      end)

    :sys.replace_state(RunManager, fn state ->
      put_in(state, [:run_pids, {context.workspace_id, current.id}], run_pid)
    end)

    reason = %{actor_id: "operator"}
    assert :ok = RunManager.cancel_run(context, current.id, reason)
    assert_received {:request_cancellation, %RequestRunCancellation{}}
    assert_received {:run_server_message, {:favn_run_cancel_requested, ^reason}}
    refute_received {:favn_run_event, _event}
  end

  test "an exact terminal replay returns already terminal without runner notification", %{
    context: context
  } do
    stale = run(:pending)
    current = run(:ok)
    put_replayed_commit(stale)
    Process.put(:run_manager_cancellation_current_run, current)

    test_pid = self()

    run_pid =
      spawn(fn ->
        receive do
          message -> send(test_pid, {:run_server_message, message})
        end
      end)

    :sys.replace_state(RunManager, fn state ->
      put_in(state, [:run_pids, {context.workspace_id, current.id}], run_pid)
    end)

    assert {:error, :run_already_terminal} =
             RunManager.cancel_run(context, current.id, %{actor_id: "operator"})

    assert_received {:request_cancellation, %RequestRunCancellation{}}
    refute_received {:run_server_message, _message}
  end

  defp put_replayed_commit(run) do
    Process.put(
      :run_manager_cancellation_committed,
      %RunCommitted{
        run: run,
        event: %{
          run_id: run.id,
          sequence: run.event_seq,
          event_type: :run_cancel_requested,
          occurred_at: DateTime.utc_now()
        },
        event_id: 1,
        outbox_event_id: 1,
        replayed?: true
      }
    )
  end

  defp run(status) do
    %RunState{
      id: "run-replayed-cancellation",
      workspace_id: "workspace",
      manifest_version_id: "manifest",
      manifest_content_hash: String.duplicate("a", 64),
      status: status
    }
  end
end
