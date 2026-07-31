defmodule FavnOrchestrator.RunServer.Execution.StepRunningPersistTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.Events
  alias FavnOrchestrator.Persistence.Commands.CommitRunTransition
  alias FavnOrchestrator.Persistence.Results.RunCommitted
  alias FavnOrchestrator.Persistence.Runtime
  alias FavnOrchestrator.Persistence.Stores
  alias FavnOrchestrator.RunServer.Execution
  alias FavnOrchestrator.RunServer.Execution.RunExecutionState
  alias FavnOrchestrator.RunState

  defmodule Store do
    alias FavnOrchestrator.Persistence.Commands.CommitRunTransition
    alias FavnOrchestrator.Persistence.Results.RunCommitted

    def commit_transition(%CommitRunTransition{} = command) do
      test_pid = Application.fetch_env!(:favn_orchestrator, :step_running_persist_test_pid)
      send(test_pid, {:run_transition_committed, command})

      {:ok,
       %RunCommitted{
         run: command.run,
         event: command.event,
         event_id: 1,
         outbox_event_id: 1,
         replayed?: false
       }}
    end
  end

  setup do
    Application.put_env(:favn_orchestrator, :step_running_persist_test_pid, self())

    stores = struct(Stores, runs: Store, logs: Store)
    start_supervised!({Runtime, %Runtime{backend: __MODULE__, options: [], stores: stores}})
    start_supervised!({Phoenix.PubSub, name: Events.pubsub_name()})

    on_exit(fn ->
      Application.delete_env(:favn_orchestrator, :step_running_persist_test_pid)
    end)

    :ok
  end

  test "a runner started signal persists one :step_running event at the next sequence" do
    task_id = "rt_step_running"
    run = run(event_seq: 3)

    await = %{
      pid: self(),
      monitor_ref: make_ref(),
      timeout_token: make_ref(),
      timeout_ref: make_ref(),
      entry: %{
        task_id: task_id,
        asset_ref: {__MODULE__, :asset},
        node_key: {{__MODULE__, :asset}, nil},
        asset_step_id: "step-running",
        window: nil,
        stage: 1,
        attempt: 1,
        execution_pool: "duckdb"
      },
      kind: :pipeline
    }

    state = %RunExecutionState{run: run, awaits: %{task_id => await}}

    assert {:cont, next} = Execution.handle_event(state, {:runner_task_started, task_id, %{}})

    assert_receive {:run_transition_committed, %CommitRunTransition{} = command}
    assert command.expected_sequence == run.event_seq
    assert command.run.event_seq == run.event_seq + 1
    assert field(command.event, :sequence) == run.event_seq + 1
    assert field(command.event, :event_type) in [:step_running, "step_running"]
    assert field(field(command.event, :data), :runner_task_id) == task_id

    assert next.run.event_seq == run.event_seq + 1
    assert %{started_persisted?: true} = next.awaits[task_id]

    assert {:cont, ^next} = Execution.handle_event(next, {:runner_task_started, task_id, %{}})
    refute_receive {:run_transition_committed, _command}, 20
  end

  defp run(event_seq: event_seq) do
    RunState.new(
      id: "run-step-running",
      workspace_id: "workspace",
      manifest_version_id: "manifest-step-running",
      manifest_content_hash: String.duplicate("a", 64),
      runner_releases: %{"default" => FavnTestSupport.runner_release_id()},
      asset_ref: {__MODULE__, :asset}
    )
    |> Map.put(:event_seq, event_seq)
    |> Map.put(:status, :running)
  end

  defp field(value, key) when is_map(value),
    do: Map.get(value, key, Map.get(value, Atom.to_string(key)))

  defp field(_value, _key), do: nil
end
