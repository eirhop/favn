defmodule FavnLocal.DevelopmentRuntimeTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest.Publication
  alias FavnLocal.DevelopmentRuntime

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
    stopping = %{status: :stopping, task: %{ref: make_ref()}, request: {:stop, {self(), make_ref()}}}

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
      failure: nil
    }

    {state, port, ref}
  end
end
