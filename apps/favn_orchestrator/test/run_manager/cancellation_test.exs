defmodule FavnOrchestrator.RunManager.CancellationTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.Persistence.Commands.RequestRunCancellation
  alias FavnOrchestrator.Persistence.Results.RunCommitted
  alias FavnOrchestrator.Persistence.Runtime
  alias FavnOrchestrator.Persistence.Stores
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.Events
  alias FavnOrchestrator.RunManager
  alias FavnOrchestrator.RunnerRegistry
  alias FavnOrchestrator.RunnerTasks
  alias FavnOrchestrator.RunState

  defmodule Store do
    alias FavnOrchestrator.Persistence.Commands.AcknowledgeRunnerTaskCancellation
    alias FavnOrchestrator.Persistence.Commands.CommitRunTransition
    alias FavnOrchestrator.Persistence.Commands.RequestRunCancellation
    alias FavnOrchestrator.Persistence.Commands.RequestRunnerTaskCancellation
    alias FavnOrchestrator.Persistence.Queries.GetRunnerTask
    alias FavnOrchestrator.Persistence.Results.RunCommitted

    def request_cancellation(%RequestRunCancellation{} = command) do
      send(Process.get(:run_manager_cancellation_test_pid), {:request_cancellation, command})
      {:ok, Process.get(:run_manager_cancellation_committed)}
    end

    def request_cancellation(%RequestRunnerTaskCancellation{} = command) do
      agent = Application.fetch_env!(:favn_orchestrator, :run_manager_runner_task_agent)

      task =
        Agent.get_and_update(agent, fn state ->
          status = if state.task.status == :queued, do: :cancelled, else: :cancelling
          task = %{state.task | status: status}
          {task, %{state | task: task}}
        end)

      test_pid = Application.fetch_env!(:favn_orchestrator, :run_manager_cancellation_test_pid)
      send(test_pid, {:runner_task_cancellation_requested, command, task})
      {:ok, task}
    end

    def acknowledge_cancellation(%AcknowledgeRunnerTaskCancellation{} = command) do
      agent = Application.fetch_env!(:favn_orchestrator, :run_manager_runner_task_agent)

      task =
        Agent.get_and_update(agent, fn state ->
          task = %{state.task | cancellation_acknowledged_at: command.occurred_at}
          {task, %{state | task: task}}
        end)

      {:ok, task}
    end

    def get(%GetRunnerTask{}) do
      agent = Application.fetch_env!(:favn_orchestrator, :run_manager_runner_task_agent)
      {:ok, Agent.get(agent, & &1.task)}
    end

    def get_run_size(_query), do: {:ok, 1_024}
    def get_run(_query),
      do: {:ok, Application.fetch_env!(:favn_orchestrator, :run_manager_cancellation_current_run)}

    def commit_transition(%CommitRunTransition{} = command) do
      committed = %RunCommitted{
        run: command.run,
        event: command.event,
        event_id: 1,
        outbox_event_id: 1,
        replayed?: false
      }

      test_pid = Application.fetch_env!(:favn_orchestrator, :run_manager_cancellation_test_pid)
      send(test_pid, {:run_transition_committed, command})
      {:ok, committed}
    end
  end

  setup do
    Process.put(:run_manager_cancellation_test_pid, self())

    stores =
      struct(Stores,
        registry: Store,
        runs: Store,
        runner_tasks: Store,
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
      Application.delete_env(:favn_orchestrator, :run_manager_cancellation_current_run)
      Application.delete_env(:favn_orchestrator, :run_manager_cancellation_test_pid)
      Application.delete_env(:favn_orchestrator, :run_manager_runner_task_agent)
    end)

    %{context: context}
  end

  test "an exact nonterminal replay resumes runner notification without republishing", %{
    context: context
  } do
    stale = run(:pending)
    current = run(:running)
    put_replayed_commit(stale)
    Application.put_env(:favn_orchestrator, :run_manager_cancellation_current_run, current)

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
    Application.put_env(:favn_orchestrator, :run_manager_cancellation_current_run, current)

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

  test "active cancellation durably cancels a queued task even if the hint is lost", %{
    context: context
  } do
    task = durable_task(:queued)
    {:ok, task_agent} = Agent.start_link(fn -> %{task: task} end)
    configure_runner_task_test(task_agent)

    current =
      run(:running)
      |> Map.put(:event_seq, 2)
      |> put_in([Access.key(:metadata), :active_runner_task_ids], [task.task_id])

    put_replayed_commit(run(:pending))
    Application.put_env(:favn_orchestrator, :run_manager_cancellation_current_run, current)

    blackhole = spawn(fn -> Process.sleep(:infinity) end)

    :sys.replace_state(RunManager, fn state ->
      put_in(state, [:run_pids, {context.workspace_id, current.id}], blackhole)
    end)

    assert :ok = RunManager.cancel_run(context, current.id, %{actor_id: "operator"})
    assert_receive {:runner_task_cancellation_requested, _command, %{status: :cancelled}}
  end

  test "inactive cancellation terminalizes a durably queued runner task", %{context: context} do
    task = durable_task(:queued)
    {:ok, task_agent} = Agent.start_link(fn -> %{task: task} end)
    configure_runner_task_test(task_agent)

    current =
      run(:running)
      |> Map.put(:event_seq, 2)
      |> put_in([Access.key(:metadata), :active_runner_task_ids], [task.task_id])

    put_replayed_commit(run(:pending))
    Application.put_env(:favn_orchestrator, :run_manager_cancellation_current_run, current)

    assert :ok = RunManager.cancel_run(context, current.id, %{actor_id: "operator"})
    assert_receive {:runner_task_cancellation_requested, _command, %{status: :cancelled}}
    assert_receive {:run_transition_committed, command}
    assert command.run.status == :cancelled
  end

  test "inactive cancellation waits for a running durable task acknowledgement", %{
    context: context
  } do
    start_runner_registry()
    {:ok, task_agent} = Agent.start_link(fn -> %{task: durable_task(:running)} end)
    configure_runner_task_test(task_agent)
    owner = self()

    runner =
      spawn_link(fn ->
        receive do
          {:favn_runner_task, %Favn.Contracts.RunnerTask.Cancellation{} = cancellation} ->
            ack = %Favn.Contracts.RunnerTask.CancellationAck{
              workspace_id: cancellation.workspace_id,
              task_id: cancellation.task_id,
              runner_instance_id: cancellation.runner_instance_id,
              runner_session_generation: cancellation.runner_session_generation,
              assignment_generation: cancellation.assignment_generation,
              command_id: cancellation.command_id,
              status: :observed,
              issued_at: cancellation.requested_at,
              acknowledged_at: DateTime.utc_now()
            }

            send(owner, {:runner_acknowledgement, RunnerTasks.acknowledge_cancellation(ack)})
        end
      end)

    registration = %Favn.Contracts.RunnerTask.Registration{
      runner_instance_id: "runner-inactive",
      boot_id: "boot-inactive",
      beam_node: Atom.to_string(node()),
      runner_pool: "default",
      required_runner_release_id: FavnTestSupport.runner_release_id(),
      lifecycle_mode: :elastic,
      supported_task_kinds: [:asset_attempt],
      capabilities: ["asset_execution"]
    }

    assert {:ok, registration_ack} = RunnerRegistry.register(registration, runner)

    Agent.update(task_agent, fn state ->
      task = %{
        state.task
        | assigned_runner_session_generation: registration_ack.runner_session_generation
      }

      %{state | task: task}
    end)

    task = Agent.get(task_agent, & &1.task)

    current =
      run(:running)
      |> Map.put(:event_seq, 2)
      |> put_in([Access.key(:metadata), :active_runner_task_ids], [task.task_id])

    put_replayed_commit(run(:pending))
    Application.put_env(:favn_orchestrator, :run_manager_cancellation_current_run, current)

    assert :ok = RunManager.cancel_run(context, current.id, %{actor_id: "operator"})
    assert_receive {:runner_acknowledgement, {:ok, _ack}}
    assert_receive {:run_transition_committed, command}
    assert command.run.status == :cancelled
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

  defp configure_runner_task_test(task_agent) do
    Application.put_env(
      :favn_orchestrator,
      :run_manager_cancellation_test_pid,
      self()
    )

    Application.put_env(
      :favn_orchestrator,
      :run_manager_runner_task_agent,
      task_agent
    )
  end

  defp durable_task(status) do
    %{
      workspace_id: "workspace",
      task_id: "rt_inactive",
      status: status,
      assigned_runner_instance_id: if(status == :running, do: "runner-inactive"),
      assigned_runner_session_generation: if(status == :running, do: 1, else: 0),
      assignment_generation: if(status == :running, do: 1, else: 0),
      runner_pool: "default",
      required_runner_release_id: FavnTestSupport.runner_release_id(),
      cancellation_acknowledged_at: nil
    }
  end

  defp start_runner_registry do
    case Process.whereis(RunnerRegistry) do
      nil -> start_supervised!({RunnerRegistry, []})
      pid -> pid
    end
  end
end
