defmodule FavnLocal.DevelopmentRuntimeTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest.Publication
  alias FavnLocal.DevelopmentRuntime
  alias FavnOrchestrator.RunnerRegistry

  defmodule RenewalStore do
    def renew_local_manifest_deployment(command) do
      send(FavnLocal.RenewalObserverTest, {:renewed, command})
      :ok
    end
  end

  test "renewal handling progresses while a supervised activation observer is waiting" do
    alias FavnLocal.ActivationObserver
    alias FavnOrchestrator.Persistence.Error
    alias FavnOrchestrator.Persistence.Runtime
    alias FavnOrchestrator.Persistence.WorkspaceContext

    Process.register(self(), FavnLocal.RenewalObserverTest)
    start_supervised!({Task.Supervisor, name: FavnLocal.TaskSupervisor})
    stores = %{FavnStoragePostgres.Backend.stores() | registry: RenewalStore}

    start_supervised!(
      {Runtime, %Runtime{backend: FavnStoragePostgres.Backend, options: [], stores: stores}}
    )

    {:ok, workspace} = WorkspaceContext.new("local-dev", "favn-local", [:platform_operator])
    parent = self()

    observer =
      Task.Supervisor.async_nolink(FavnLocal.TaskSupervisor, fn ->
        ActivationObserver.await(
          workspace,
          "operation",
          System.monotonic_time(:millisecond) + 5_000,
          read_operation: fn _, _ ->
            {:error, Error.new(:unavailable, "temporary", retryable?: true)}
          end,
          wait: fn _ ->
            send(parent, :observer_waiting)

            receive do
              :finish -> exit(:normal)
            end
          end
        )
      end)

    assert_receive :observer_waiting

    owner = %{operation_id: "operation", session_id: "session"}
    state = %{status: :starting, deployment_owner: owner, config: %{workspace_id: "local-dev"}}

    assert {:noreply, ^state} =
             DevelopmentRuntime.handle_info({:renew_deployment_owner, owner}, state)

    assert_receive {:renewed, command}
    assert command.operation_id == "operation"
    assert command.session_id == "session"
    assert command.workspace_context.workspace_id == "local-dev"
    assert Process.alive?(observer.pid)
    Task.shutdown(observer, :brutal_kill)
  end

  defmodule SlowReload do
    use GenServer
    def init(parent), do: {:ok, parent}

    def handle_call({:reload, _, _}, _from, parent) do
      send(parent, :reload_still_running)
      {:noreply, parent}
    end

    def handle_call(:status, _from, parent),
      do: {:reply, %{status: :reloading, deployment_operation_id: "local-observed"}, parent}
  end

  test "reload observer timeout returns the operation identity without stopping its owner" do
    {:ok, pid} = GenServer.start_link(SlowReload, self(), name: DevelopmentRuntime)
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)
    publication = %Publication{version: nil, execution_packages: []}

    assert {:error, {:reload_pending, %{deployment_operation_id: "local-observed"}}} =
             DevelopmentRuntime.reload(publication, "release", 1)

    assert_receive :reload_still_running
    assert Process.alive?(pid)
    assert DevelopmentRuntime.status().status == :reloading
  end

  setup do
    previous = System.get_env("FAVN_DEV_RUNNER_START_TIMEOUT_MS")
    System.delete_env("FAVN_DEV_RUNNER_START_TIMEOUT_MS")

    on_exit(fn ->
      if previous,
        do: System.put_env("FAVN_DEV_RUNNER_START_TIMEOUT_MS", previous),
        else: System.delete_env("FAVN_DEV_RUNNER_START_TIMEOUT_MS")
    end)
  end

  test "the default caller wait covers deployment and an increased registration budget" do
    assert DevelopmentRuntime.startup_timeout_ms() == 360_000
    System.put_env("FAVN_DEV_RUNNER_START_TIMEOUT_MS", "120000")
    assert DevelopmentRuntime.startup_timeout_ms() == 450_000
  end

  test "registered startup gives deployment a fresh budget and ignores the old phase timer" do
    state = registered_startup()
    started_at = System.monotonic_time(:millisecond)

    assert {:noreply, deploying} = DevelopmentRuntime.handle_info(:probe_runner, state)
    assert deploying.deadline >= started_at + 330_000
    assert deploying.deadline < started_at + 331_000
    assert {:noreply, ^deploying} = DevelopmentRuntime.handle_info(:startup_deadline, deploying)
    assert {:error, _invalid_workspace} = Task.await(deploying.task)
  end

  test "deployment preserves an explicit caller deadline across registration" do
    state = registered_startup()
    deadline = System.monotonic_time(:millisecond) + 10_000
    from = {self(), make_ref()}

    assert {:noreply, waiting} =
             DevelopmentRuntime.handle_call({:await_ready, deadline}, from, state)

    assert {:noreply, deploying} = DevelopmentRuntime.handle_info(:probe_runner, waiting)
    assert deploying.deadline == deadline
    assert deploying.startup_wait_deadline == deadline
    assert {:error, _invalid_workspace} = Task.await(deploying.task)
  end

  test "startup deadline stops owned deployment work and runners and replies with the phase" do
    task =
      Task.async(fn ->
        receive do
          :finish -> :ok
        end
      end)

    runner = child()
    candidate = child()
    unrelated = child()
    on_exit(fn -> if Port.info(unrelated.port), do: Port.close(unrelated.port) end)
    reference = make_ref()

    state = %{
      status: :starting,
      retiring: nil,
      deadline: System.monotonic_time(:millisecond) - 1,
      task: task,
      runner: runner,
      candidate: candidate,
      ready_waiters: [{self(), reference}],
      request: nil,
      failure: nil
    }

    assert {:noreply, failed} = DevelopmentRuntime.handle_info(:startup_deadline, state)
    assert failed.status == :failed
    assert failed.failure == {:startup_timeout, :deployment}
    assert_receive {^reference, {:error, {:startup_timeout, :deployment}}}
    refute Process.alive?(task.pid)
    assert Port.info(runner.port) == nil
    assert Port.info(candidate.port) == nil
    assert Port.info(unrelated.port) != nil
    Port.close(unrelated.port)
  end

  test "a shorter caller deadline applies to the existing startup budget" do
    now = System.monotonic_time(:millisecond)

    state = %{
      status: :starting,
      deadline: now + 2_000,
      startup_wait_deadline: nil,
      ready_waiters: []
    }

    from = {self(), make_ref()}

    assert {:noreply, waiting} =
             DevelopmentRuntime.handle_call({:await_ready, now + 1_000}, from, state)

    assert waiting.deadline == now + 1_000

    assert {:noreply, same} =
             DevelopmentRuntime.handle_call({:await_ready, now + 3_000}, from, waiting)

    assert same.deadline == waiting.deadline
    assert same.startup_wait_deadline == now + 1_000

    expired =
      Map.merge(same, %{
        deadline: now - 1,
        retiring: nil,
        task: nil,
        runner: nil,
        candidate: nil,
        request: nil,
        failure: nil
      })

    assert {:noreply, %{failure: {:startup_timeout, :registration}}} =
             DevelopmentRuntime.handle_info(:startup_deadline, expired)
  end

  test "startup cancellation start failure returns an explicit unknown with its operation" do
    state = %{
      status: :starting,
      deployment_owner: %{operation_id: "local-crash", session_id: "session"},
      deadline: System.monotonic_time(:millisecond) - 1,
      task: nil,
      runner: nil,
      candidate: nil,
      retiring: nil,
      ready_waiters: [],
      request: nil,
      failure: nil
    }

    assert {:noreply, waiting} = DevelopmentRuntime.handle_info(:startup_deadline, state)
    ref = waiting.cancellation_task.ref
    assert_receive {:cancellation_start_failed, ^ref} = message
    assert {:noreply, failed} = DevelopmentRuntime.handle_info(message, waiting)

    assert {:startup_interrupted, _, %{activation: :unknown, operation_id: "local-crash"}} =
             failed.failure

    refute Map.has_key?(failed, :cancellation_task)
  end

  test "cancellation worker DOWN and deadline finish once and ignore late replies" do
    for message_kind <- [:down, :timeout] do
      ref = make_ref()
      timer = Process.send_after(self(), :unused, 60_000)

      state = %{
        status: :failed,
        deployment_owner: %{operation_id: "local-crash", session_id: "session"},
        task: nil,
        runner: nil,
        candidate: nil,
        ready_waiters: [],
        request: nil,
        failure: nil,
        cancellation_task: %{
          ref: ref,
          pid: nil,
          timer: timer,
          continuation: {:startup, :deadline}
        }
      }

      message =
        if message_kind == :down,
          do: {:DOWN, ref, :process, self(), :killed},
          else: {:cancellation_timeout, ref}

      assert {:noreply, failed} = DevelopmentRuntime.handle_info(message, state)

      assert {:startup_interrupted, :deadline,
              %{activation: :unknown, operation_id: "local-crash"}} = failed.failure

      assert {:noreply, ^failed} =
               DevelopmentRuntime.handle_info({ref, {:ok, :cancelled_before_acceptance}}, failed)
    end
  end

  defp child do
    port = Port.open({:spawn_executable, System.find_executable("cat")}, [:binary, :exit_status])
    %{port: port, node: nil, release_id: "local-deadline-fixture", runner_instance_id: "fixture"}
  end

  defp registered_startup do
    start_supervised!(RunnerRegistry)
    start_supervised!({Task.Supervisor, name: FavnLocal.TaskSupervisor})

    session = %{
      required_runner_release_id: "local-deadline-fixture",
      runner_pool: "default",
      lifecycle_mode: :resident,
      agent_pid: self()
    }

    :sys.replace_state(RunnerRegistry, &%{&1 | sessions: %{"fixture" => session}})

    %{
      status: :starting,
      startup_action: :deploy,
      retiring: nil,
      runner: %{release_id: "local-deadline-fixture", runner_instance_id: "fixture", node: nil},
      deadline: System.monotonic_time(:millisecond) - 1,
      startup_wait_deadline: nil,
      ready_waiters: [],
      request: nil,
      task: nil,
      publication: %Publication{version: nil, execution_packages: []},
      # Validation rejects the empty workspace before any database access.
      config: %{workspace_id: ""}
    }
  end

  test "the runner restart budget fails the stack after repeated fast exits" do
    now = 1_000_000
    recent = Enum.map(1..5, &(now - &1 * 1_000))

    assert {:fail, ^recent} = DevelopmentRuntime.runner_crash_budget_state(recent, now)
  end

  test "exits outside the crash window do not count against the budget" do
    now = 1_000_000
    old = Enum.map(1..5, &(now - 60_001 - &1))

    assert {:continue, [^now]} = DevelopmentRuntime.runner_crash_budget_state(old, now)
  end

  test "a first exit restarts and starts the window" do
    assert {:continue, [50]} = DevelopmentRuntime.runner_crash_budget_state([], 50)
  end

  test "all reload classifications are refused while a previous runner retires" do
    state = %{status: :ready, retiring: %{release_id: "old"}}

    for release <- ["current", "new"] do
      assert {:reply, {:error, :runner_still_draining}, ^state} =
               DevelopmentRuntime.handle_call({:reload, :publication, release}, :from, state)
    end
  end

  test "candidate exit before deployment preserves the successful publication" do
    {state, port, reply_ref} = pending_reload(nil)
    assert {:noreply, ready} = DevelopmentRuntime.handle_info({port, {:exit_status, 1}}, state)
    assert_receive {^reply_ref, {:error, :candidate_runner_exited}}
    assert ready.status == :ready
    assert ready.publication == state.publication
    assert ready.deployment == state.deployment
  end

  test "candidate exit during deployment blocks admission and ignores late completion" do
    task_ref = make_ref()
    {state, port, reply_ref} = pending_reload(%{ref: task_ref})
    assert {:noreply, failed} = DevelopmentRuntime.handle_info({port, {:exit_status, 1}}, state)
    assert_receive {^reply_ref, {:error, {:reload_outcome_unknown, :candidate_runner_exited}}}
    assert failed.status == :failed
    assert failed.task == nil
    assert failed.publication == state.publication
    assert failed.deployment == state.deployment

    assert {:reply, {:error, {:lifecycle_not_ready, :failed}}, ^failed} =
             DevelopmentRuntime.handle_call({:reload, :publication, "next"}, :from, failed)

    assert {:noreply, ^failed} =
             DevelopmentRuntime.handle_info({task_ref, {:ok, %{deployment_id: "late"}}}, failed)

    assert {:noreply, ^failed} =
             DevelopmentRuntime.handle_info({:DOWN, task_ref, :process, self(), :normal}, failed)
  end

  test "a crashed manifest deployment task reports an unknown outcome" do
    task_ref = make_ref()
    {state, _port, reply_ref} = pending_reload(%{ref: task_ref})
    state = %{state | candidate: nil}

    assert {:noreply, failed} =
             DevelopmentRuntime.handle_info({:DOWN, task_ref, :process, self(), :killed}, state)

    assert_receive {^reply_ref,
                    {:error, {:reload_outcome_unknown, {:deployment_task_failed, :killed}}}}

    assert failed.status == :failed
    assert failed.task == nil
    assert failed.publication == state.publication
  end

  test "a returned unknown activation error blocks reload until stop/start" do
    task_ref = make_ref()
    {state, _port, reply_ref} = pending_reload(%{ref: task_ref})
    state = %{state | candidate: nil}
    reason = {:reload_outcome_unknown, :activation_unavailable}

    assert {:noreply, failed} =
             DevelopmentRuntime.handle_info({task_ref, {:error, reason}}, state)

    assert_receive {^reply_ref, {:error, ^reason}}
    assert failed.status == :failed
    assert failed.failure == reason
    assert failed.publication == state.publication
  end

  test "a returned durable read failure preserves readiness and does not advance the baseline" do
    task_ref = make_ref()
    {state, _port, reply_ref} = pending_reload(%{ref: task_ref})
    state = %{state | candidate: nil}

    assert {:noreply, ready} =
             DevelopmentRuntime.handle_info({task_ref, {:error, :read_unavailable}}, state)

    assert_receive {^reply_ref, {:error, :read_unavailable}}
    assert ready.status == :ready
    assert ready.publication == state.publication
    assert ready.deployment == state.deployment
  end

  test "late deployment messages cannot take ownership from the shutdown task" do
    old_ref = make_ref()

    stopping = %{
      status: :stopping,
      task: %{ref: make_ref()},
      request: {:stop, {self(), make_ref()}}
    }

    assert {:noreply, ^stopping} =
             DevelopmentRuntime.handle_info({old_ref, {:ok, %{deployment_id: "late"}}}, stopping)

    assert {:noreply, ^stopping} =
             DevelopmentRuntime.handle_info({:DOWN, old_ref, :process, self(), :normal}, stopping)
  end

  defp pending_reload(task) do
    port = Port.open({:spawn_executable, System.find_executable("cat")}, [:binary])
    on_exit(fn -> if Port.info(port), do: Port.close(port) end)
    ref = make_ref()

    state = %{
      status: :reloading,
      candidate: %{port: port},
      retiring: nil,
      publication: :last_successful_publication,
      deployment: :last_successful_deployment,
      request: {{self(), ref}, %Publication{version: nil, execution_packages: []}, "next"},
      task: task,
      ready_waiters: [],
      ignored_ports: MapSet.new(),
      deadline: 1,
      startup_wait_deadline: nil,
      failure: nil
    }

    {state, port, ref}
  end
end
