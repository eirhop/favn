defmodule FavnOrchestrator.RunnerTaskResultRouterTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.Persistence.Queries.GetRunnerTask
  alias FavnOrchestrator.Persistence.Runtime
  alias FavnOrchestrator.Persistence.Stores
  alias FavnOrchestrator.RunnerTaskResultRouter

  defmodule Store do
    def get(%GetRunnerTask{}) do
      agent = Application.fetch_env!(:favn_orchestrator, :runner_task_router_test_agent)

      response =
        Agent.get_and_update(agent, fn state ->
          {state.response, %{state | reads: state.reads + 1}}
        end)

      case response do
        {:block, owner, result} ->
          send(owner, {:runner_task_read_started, self()})

          receive do
            :release_runner_task_read -> result
          end

        result ->
          result
      end
    end
  end

  setup do
    {:ok, store_state} = Agent.start_link(fn -> %{reads: 0, response: {:error, :temporary}} end)

    Application.put_env(
      :favn_orchestrator,
      :runner_task_router_test_agent,
      store_state
    )

    stores = struct(Stores, runner_tasks: Store)
    start_supervised!({Runtime, %Runtime{backend: __MODULE__, options: [], stores: stores}})
    start_supervised!({Task.Supervisor, name: FavnOrchestrator.RunnerClaimSupervisor})

    on_exit(fn ->
      Application.delete_env(:favn_orchestrator, :runner_task_router_test_agent)
    end)

    %{store_state: store_state}
  end

  test "durable await retries task-read failures and re-subscribes after router restart", %{
    store_state: store_state
  } do
    {:ok, router} = RunnerTaskResultRouter.start_link([])
    Process.unlink(router)
    parent = self()
    waiter = spawn_link(fn -> RunnerTaskResultRouter.await("workspace", "rt_router", parent) end)

    assert_eventually(fn -> Agent.get(store_state, & &1.reads) >= 1 end)
    assert Process.alive?(waiter)

    active = %{workspace_id: "workspace", task_id: "rt_router", status: :running}
    Agent.update(store_state, &%{&1 | response: {:ok, active}})
    assert_eventually(fn -> Agent.get(store_state, & &1.reads) >= 2 end)

    GenServer.stop(router)
    assert_eventually(fn -> Process.whereis(RunnerTaskResultRouter) == nil end)

    {:ok, replacement} = RunnerTaskResultRouter.start_link([])
    Process.unlink(replacement)

    terminal = active |> Map.put(:status, :succeeded) |> Map.put(:result, %{status: :ok})
    Agent.update(store_state, &%{&1 | response: {:ok, terminal}})
    assert_eventually(fn -> Agent.get(store_state, & &1.reads) >= 3 end)
    RunnerTaskResultRouter.notify(terminal)

    assert_receive {:runner_task_result, "workspace", "rt_router", ^terminal}
    assert_eventually(fn -> not Process.alive?(waiter) end)

    GenServer.stop(replacement)
  end

  test "bounds durable reads without losing a notification during the check", %{
    store_state: store_state
  } do
    owner = self()
    active = %{workspace_id: "workspace", task_id: "rt_bounded", status: :running}
    Agent.update(store_state, &%{&1 | response: {:block, owner, {:ok, active}}})

    {:ok, router} = RunnerTaskResultRouter.start_link(max_concurrency: 1)
    Process.unlink(router)

    subscription =
      Task.async(fn ->
        RunnerTaskResultRouter.subscribe("workspace", "rt_bounded", owner)
      end)

    assert_receive {:runner_task_read_started, read_worker}

    assert {:error, :runner_task_result_router_overloaded} =
             RunnerTaskResultRouter.subscribe("workspace", "rt_overloaded", owner)

    terminal = Map.merge(active, %{status: :succeeded, result: %{status: :ok}})
    RunnerTaskResultRouter.notify(terminal)

    assert Task.await(subscription) == :ready
    assert_receive {:runner_task_result, "workspace", "rt_bounded", ^terminal}
    assert_eventually(fn -> not Process.alive?(read_worker) end)

    Agent.update(store_state, &%{&1 | response: {:ok, terminal}})

    assert_eventually(fn ->
      RunnerTaskResultRouter.subscribe("workspace", "rt_bounded", owner) == :ready
    end)

    assert_receive {:runner_task_result, "workspace", "rt_bounded", ^terminal}
    assert Process.alive?(router)
    GenServer.stop(router)
  end

  test "subscriber loss cancels its durable read and releases capacity", %{
    store_state: store_state
  } do
    owner = self()
    active = %{workspace_id: "workspace", task_id: "rt_abandoned", status: :running}
    Agent.update(store_state, &%{&1 | response: {:block, owner, {:ok, active}}})

    {:ok, router} = RunnerTaskResultRouter.start_link(max_concurrency: 1)
    Process.unlink(router)

    subscriber =
      spawn(fn ->
        result = RunnerTaskResultRouter.subscribe("workspace", "rt_abandoned")
        send(owner, {:abandoned_subscription_returned, result})
      end)

    subscriber_ref = Process.monitor(subscriber)
    assert_receive {:runner_task_read_started, read_worker}

    Process.exit(subscriber, :kill)
    assert_receive {:DOWN, ^subscriber_ref, :process, ^subscriber, :killed}
    assert_eventually(fn -> not Process.alive?(read_worker) end)

    terminal = Map.merge(active, %{status: :succeeded, result: %{status: :ok}})
    Agent.update(store_state, &%{&1 | response: {:ok, terminal}})

    assert RunnerTaskResultRouter.subscribe("workspace", "rt_abandoned", owner) == :ready
    assert_receive {:runner_task_result, "workspace", "rt_abandoned", ^terminal}
    refute_receive {:abandoned_subscription_returned, _result}
    assert Process.alive?(router)
    GenServer.stop(router)
  end

  defp assert_eventually(fun, attempts \\ 100)
  defp assert_eventually(_fun, 0), do: flunk("condition did not become true")

  defp assert_eventually(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      assert_eventually(fun, attempts - 1)
    end
  end
end
