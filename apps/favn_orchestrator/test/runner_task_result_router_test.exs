defmodule FavnOrchestrator.RunnerTaskResultRouterTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.Persistence.Queries.GetRunnerTask
  alias FavnOrchestrator.Persistence.Runtime
  alias FavnOrchestrator.Persistence.Stores
  alias FavnOrchestrator.RunnerTaskResultRouter

  defmodule Store do
    def get(%GetRunnerTask{} = query) do
      agent = Application.fetch_env!(:favn_orchestrator, :runner_task_router_test_agent)

      response =
        Agent.get_and_update(agent, fn state ->
          {state.response, %{state | reads: state.reads + 1}}
        end)

      case response do
        fun when is_function(fun, 1) ->
          fun.(query)

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

  test "scalar notification reads durable details and a lost notification is rediscovered", %{
    store_state: store_state
  } do
    router = start_supervised!(RunnerTaskResultRouter)
    active = %{workspace_id: "workspace", task_id: "rt_scalar", status: :running}
    Agent.update(store_state, &%{&1 | response: {:ok, active}})
    assert :waiting = RunnerTaskResultRouter.subscribe("workspace", "rt_scalar")

    terminal = %{
      workspace_id: "workspace",
      task_id: "rt_scalar",
      status: :succeeded,
      result: %{value: 42},
      data_state: :available
    }

    Agent.update(store_state, &%{&1 | response: {:ok, terminal}})
    RunnerTaskResultRouter.notify(%{terminal | result: nil, data_state: :not_loaded})
    assert_receive {:runner_task_result, "workspace", "rt_scalar", ^terminal}
    Agent.update(store_state, &%{&1 | response: {:ok, active}})
    assert :waiting = RunnerTaskResultRouter.subscribe("workspace", "rt_scalar")
    Agent.update(store_state, &%{&1 | response: {:ok, terminal}})
    send(router, :poll)
    assert_receive {:runner_task_result, "workspace", "rt_scalar", ^terminal}
    assert :sys.get_state(router).waiters == %{}
  end

  test "polling reaches later terminal tasks while a full batch remains running", %{
    store_state: store_state
  } do
    router = start_supervised!({RunnerTaskResultRouter, max_concurrency: 32})

    active = fn query ->
      {:ok, %{workspace_id: "workspace", task_id: query.task_id, status: :running}}
    end

    Agent.update(store_state, &%{&1 | response: active})
    ids = for n <- 1..40, do: "rt_" <> String.pad_leading(to_string(n), 2, "0")
    for id <- ids, do: assert(:waiting == RunnerTaskResultRouter.subscribe("workspace", id))

    Agent.update(
      store_state,
      &%{
        &1
        | response: fn query ->
            {:ok,
             %{
               workspace_id: "workspace",
               task_id: query.task_id,
               status: if(query.task_id == "rt_40", do: :succeeded, else: :running)
             }}
          end
      }
    )

    for _ <- 1..2 do
      send(router, :check_waiters)
      assert_eventually(fn -> map_size(:sys.get_state(router).checks) == 0 end)
    end

    assert_receive {:runner_task_result, "workspace", "rt_40", %{status: :succeeded}}, 1_000
    assert map_size(:sys.get_state(router).waiters) == 39
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

  test "started notifications reach opted-in waiters without consuming them", %{
    store_state: store_state
  } do
    {:ok, router} = RunnerTaskResultRouter.start_link([])
    Process.unlink(router)
    parent = self()

    queued = %{workspace_id: "workspace", task_id: "rt_started", status: :queued}
    Agent.update(store_state, &%{&1 | response: {:ok, queued}})

    waiter =
      spawn_link(fn ->
        RunnerTaskResultRouter.await("workspace", "rt_started", parent, notify_started?: true)
      end)

    assert_eventually(fn -> Agent.get(store_state, & &1.reads) >= 1 end)

    running = %{queued | status: :running}
    RunnerTaskResultRouter.notify_started(running)
    assert_receive {:runner_task_started, "workspace", "rt_started", ^running}
    assert Process.alive?(waiter)

    terminal = Map.merge(running, %{status: :succeeded, result: %{status: :ok}})
    RunnerTaskResultRouter.notify(terminal)
    assert_receive {:runner_task_result, "workspace", "rt_started", ^terminal}

    GenServer.stop(router)
  end

  test "started notifications skip waiters that did not opt in", %{store_state: store_state} do
    {:ok, router} = RunnerTaskResultRouter.start_link([])
    Process.unlink(router)

    queued = %{workspace_id: "workspace", task_id: "rt_plain", status: :queued}
    Agent.update(store_state, &%{&1 | response: {:ok, queued}})

    assert RunnerTaskResultRouter.subscribe("workspace", "rt_plain", self()) == :waiting

    RunnerTaskResultRouter.notify_started(%{queued | status: :running})
    refute_receive {:runner_task_started, _workspace, _task, _task_state}, 50

    GenServer.stop(router)
  end

  test "a task already running at subscription time delivers started from the check", %{
    store_state: store_state
  } do
    {:ok, router} = RunnerTaskResultRouter.start_link([])
    Process.unlink(router)

    running = %{workspace_id: "workspace", task_id: "rt_live", status: :running}
    Agent.update(store_state, &%{&1 | response: {:ok, running}})

    assert RunnerTaskResultRouter.subscribe("workspace", "rt_live", self(), notify_started?: true) ==
             :waiting

    assert_receive {:runner_task_started, "workspace", "rt_live", ^running}

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
