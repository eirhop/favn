defmodule FavnOrchestrator.RunnerSessionsTest do
  use ExUnit.Case, async: false

  alias Favn.Contracts.RunnerTask.Registration
  alias FavnOrchestrator.Persistence.Results.RunnerSession
  alias FavnOrchestrator.Persistence.Runtime
  alias FavnOrchestrator.Persistence.Stores
  alias FavnOrchestrator.RunnerRegistry
  alias FavnOrchestrator.RunnerSessions

  @release "rr_" <> String.duplicate("a", 64)

  @test_pid_key {__MODULE__, :test_pid}
  @store_mode_key {__MODULE__, :store_mode}

  defmodule Store do
    @test_pid_key {FavnOrchestrator.RunnerSessionsTest, :test_pid}
    @store_mode_key {FavnOrchestrator.RunnerSessionsTest, :store_mode}

    def open_session(command) do
      send(:persistent_term.get(@test_pid_key), {:open_session, command})

      case :persistent_term.get(@store_mode_key, :ok) do
        :ok -> {:ok, %RunnerSession{session_id: command.session_id}}
        :raise -> raise "session store unavailable"
      end
    end

    def close_session(command) do
      send(:persistent_term.get(@test_pid_key), {:close_session, command})

      case :persistent_term.get(@store_mode_key, :ok) do
        :ok -> {:ok, :closed}
        :raise -> raise "session store unavailable"
      end
    end

    def reconcile_sessions(command) do
      send(:persistent_term.get(@test_pid_key), {:reconcile_sessions, command})
      {:ok, 2}
    end

    def prune_sessions(command) do
      send(:persistent_term.get(@test_pid_key), {:prune_sessions, command})
      {:ok, 0}
    end
  end

  setup do
    :persistent_term.put(@test_pid_key, self())
    :persistent_term.put(@store_mode_key, :ok)

    previous = Application.get_env(:favn_orchestrator, :runner_pools)
    Application.put_env(:favn_orchestrator, :runner_pools, duckdb: [mode: :elastic])

    stores = struct(Stores, runner_tasks: Store)
    start_supervised!({Runtime, %Runtime{backend: __MODULE__, options: [], stores: stores}})
    start_supervised!({RunnerRegistry, []})

    start_supervised!({Task.Supervisor, name: FavnOrchestrator.RunnerSessionTaskSupervisor})

    on_exit(fn ->
      :persistent_term.erase(@test_pid_key)
      :persistent_term.erase(@store_mode_key)

      if is_nil(previous),
        do: Application.delete_env(:favn_orchestrator, :runner_pools),
        else: Application.put_env(:favn_orchestrator, :runner_pools, previous)
    end)

    %{}
  end

  test "an accepted registration opens exactly one durable session row" do
    agent = spawn_agent()
    registration = registration("runner-open", "boot-open")

    assert {:ok, ack} = RunnerRegistry.register(registration, agent)
    assert ack.status == :accepted

    assert_receive {:open_session, command}, 2_000
    assert command.session_id =~ ~r/^rs_[0-9a-f]{32}$/
    assert command.runner_instance_id == "runner-open"
    assert command.runner_boot_id == "boot-open"
    assert command.session_generation == ack.runner_session_generation
    assert command.control_plane_boot_id == RunnerSessions.control_plane_boot_id()
    assert command.runner_pool == "duckdb"
    assert command.required_runner_release_id == @release

    assert {:ok, ^ack} = RunnerRegistry.register(registration, agent)
    refute_receive {:open_session, _duplicate}, 300
  end

  test "an abnormal exit while busy closes as crashed with the interrupted task" do
    agent = spawn_agent()
    assert {:ok, ack} = RunnerRegistry.register(registration("runner-crash", "boot-crash"), agent)
    assert_receive {:open_session, opened}, 2_000

    :ok =
      RunnerRegistry.mark_busy(
        "runner-crash",
        ack.runner_session_generation,
        %{workspace_id: "workspace-crash", task_id: "rt_crash"}
      )

    Process.exit(agent, :kill)

    assert_receive {:close_session, command}, 2_000
    assert command.session_id == opened.session_id
    assert command.end_reason == :crashed
    assert command.busy_at_exit == true
    assert command.interrupted_task_workspace_id == "workspace-crash"
    assert command.interrupted_task_id == "rt_crash"
  end

  test "a normal exit while idle closes as shut down without interruption" do
    agent = spawn_agent()
    assert {:ok, _ack} = RunnerRegistry.register(registration("runner-quit", "boot-quit"), agent)
    assert_receive {:open_session, opened}, 2_000

    send(agent, :stop)

    assert_receive {:close_session, command}, 2_000
    assert command.session_id == opened.session_id
    assert command.end_reason == :shut_down
    assert command.busy_at_exit == false
    assert is_nil(command.interrupted_task_id)
  end

  test "a normal exit while busy shuts down but records the interrupted task" do
    agent = spawn_agent()
    assert {:ok, ack} = RunnerRegistry.register(registration("runner-drain", "boot-drain"), agent)
    assert_receive {:open_session, _opened}, 2_000

    :ok =
      RunnerRegistry.mark_busy(
        "runner-drain",
        ack.runner_session_generation,
        %{workspace_id: "workspace-drain", task_id: "rt_drain"}
      )

    send(agent, :stop)

    assert_receive {:close_session, command}, 2_000
    assert command.end_reason == :shut_down
    assert command.busy_at_exit == true
    assert command.interrupted_task_id == "rt_drain"
  end

  test "a raising session store never blocks registration or disconnect handling" do
    :persistent_term.put(@store_mode_key, :raise)
    agent = spawn_agent()

    assert {:ok, ack} = RunnerRegistry.register(registration("runner-safe", "boot-safe"), agent)
    assert ack.status == :accepted
    assert {:ok, _session} = RunnerRegistry.fetch("runner-safe")

    Process.exit(agent, :kill)

    assert_eventually(fn ->
      RunnerRegistry.fetch("runner-safe") == {:error, :runner_session_not_found}
    end)
  end

  test "boot reconciliation targets rows from other control-plane boots" do
    assert :ok = RunnerSessions.reconcile_boot()

    assert_receive {:reconcile_sessions, command}
    assert command.control_plane_boot_id == RunnerSessions.control_plane_boot_id()
    assert %DateTime{} = command.ended_at
  end

  defp registration(runner_id, boot_id) do
    %Registration{
      runner_instance_id: runner_id,
      boot_id: boot_id,
      beam_node: Atom.to_string(node()),
      runner_pool: "duckdb",
      required_runner_release_id: @release,
      lifecycle_mode: :elastic,
      supported_task_kinds: [:asset_attempt],
      capabilities: ["asset_execution"]
    }
  end

  defp spawn_agent do
    spawn(fn ->
      receive do
        :stop -> :ok
      end
    end)
  end

  defp assert_eventually(fun, attempts \\ 50)

  defp assert_eventually(fun, 0), do: assert(fun.())

  defp assert_eventually(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(20)
      assert_eventually(fun, attempts - 1)
    end
  end
end
