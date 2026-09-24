defmodule FavnOrchestrator.RunnerTaskClaimTest do
  use ExUnit.Case, async: false

  alias Favn.Contracts.RunnerTask.Assignment
  alias Favn.Contracts.RunnerTask.ClaimRequest
  alias Favn.Contracts.RunnerTask.NoWork
  alias Favn.Contracts.RunnerTask.Registration
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Results.RunnerTask
  alias FavnOrchestrator.Persistence.Runtime
  alias FavnOrchestrator.Persistence.Stores
  alias FavnOrchestrator.RunnerQueueCoordinator
  alias FavnOrchestrator.RunnerQueueSupervisor
  alias FavnOrchestrator.RunnerRegistry
  alias FavnOrchestrator.RunnerTasks

  @release "rr_" <> String.duplicate("a", 64)
  @pool "duckdb"

  defmodule ChurningStore do
    @moduledoc false
    @behaviour FavnOrchestrator.Persistence.RunnerTaskStore

    # Every empty claim bumps the queue generation before returning, so each
    # racing wait observes a stale generation and the retry budget drains.
    def claim(command) do
      Agent.get_and_update(agent(), fn state ->
        {reply, remaining} =
          case state.replies do
            [reply | rest] -> {reply, rest}
            [] -> {{:ok, nil}, []}
          end

        if reply == {:ok, nil} and state.churn? do
          FavnOrchestrator.RunnerQueueCoordinator.notify(
            command.runner_pool,
            command.required_runner_release_id,
            0
          )
        end

        next = %{state | replies: remaining, claims: [command | state.claims]}
        maybe_restart_registry(reply, state.restart_registry?)
        {reply, next}
      end)
    end

    defp maybe_restart_registry(_reply, true) do
      previous = Process.whereis(FavnOrchestrator.RunnerRegistry)
      GenServer.stop(previous, :normal)
      await_registry_restart(previous, 100)
    end

    defp maybe_restart_registry(_reply, _restart?), do: :ok

    defp await_registry_restart(_previous, 0), do: raise("runner registry did not restart")

    defp await_registry_restart(previous, attempts) do
      case Process.whereis(FavnOrchestrator.RunnerRegistry) do
        pid when is_pid(pid) and pid != previous ->
          :ok

        _other ->
          Process.sleep(5)
          await_registry_restart(previous, attempts - 1)
      end
    end

    def admit(_command), do: unavailable()
    def enqueue(_command), do: unavailable()
    def get(_query), do: unavailable()
    def transition(_command), do: unavailable()
    def persist_runtime_inputs(_command), do: unavailable()
    def append_log_batch(_command), do: unavailable()
    def complete(_command), do: unavailable()
    def request_cancellation(_command), do: unavailable()
    def acknowledge_cancellation(_command), do: unavailable()
    def release(_command), do: unavailable()
    def retry(_command), do: unavailable()
    def resolve_write(_command), do: unavailable()
    def get_write_resolution(_command), do: unavailable()
    def recover_expired(_command), do: unavailable()
    def reconcile_demand(_command), do: unavailable()
    def ensure_demand(_command), do: unavailable()
    def page_run(_query), do: unavailable()
    def demand(_query), do: unavailable()
    def list_demands(_query), do: unavailable()
    def release_drain(_query), do: unavailable()
    def capacity_health(_query), do: unavailable()
    def list_release_drains(_query), do: unavailable()
    def open_session(_command), do: unavailable()
    def close_session(_command), do: unavailable()
    def reconcile_sessions(_command), do: unavailable()

    def page_sessions(_query), do: unavailable()
    def session_window_totals(_query), do: unavailable()
    def page_session_tasks(_query), do: unavailable()
    def workspace_task_stats(_query), do: unavailable()

    defp agent, do: Application.fetch_env!(:favn_orchestrator, :runner_task_claim_test_agent)

    defp unavailable, do: {:error, Error.new(:unavailable, "runner task claim test store")}
  end

  defmodule ClaimGateway do
    use GenServer

    def start_link(owner), do: GenServer.start_link(__MODULE__, owner)
    def init(owner), do: {:ok, owner}
    def handle_call(:gateway, _from, owner), do: {:reply, {:ok, self()}, owner}

    def handle_call({:register, registration, agent}, _from, owner),
      do: {:reply, RunnerRegistry.register(registration, agent), owner}

    def handle_call({:request, %ClaimRequest{} = request}, _from, owner) do
      reply = RunnerTasks.claim(request)
      send(owner, {:claim_reply, request, reply})
      {:reply, reply, owner}
    end
  end

  setup do
    {:ok, agent} =
      Agent.start_link(fn ->
        %{replies: [], claims: [], churn?: true, restart_registry?: false}
      end)

    Application.put_env(:favn_orchestrator, :runner_task_claim_test_agent, agent)

    previous = Application.get_env(:favn_orchestrator, :runner_pools)
    Application.put_env(:favn_orchestrator, :runner_pools, duckdb: [mode: :resident])

    stores = struct(Stores, runner_tasks: ChurningStore)
    start_supervised!({Runtime, %Runtime{backend: __MODULE__, options: [], stores: stores}})
    start_supervised!({RunnerRegistry, []})
    start_supervised!({RunnerQueueSupervisor, []})

    on_exit(fn ->
      Application.delete_env(:favn_orchestrator, :runner_task_claim_test_agent)

      if is_nil(previous),
        do: Application.delete_env(:favn_orchestrator, :runner_pools),
        else: Application.put_env(:favn_orchestrator, :runner_pools, previous)
    end)

    {:ok, agent: agent}
  end

  test "an exhausted wake-race retry budget still leaves the runner wakeable", %{agent: agent} do
    session_generation = register("runner-exhausted")
    request = claim_request("runner-exhausted", session_generation, "claim-exhausted")

    assert {:ok, %NoWork{action: :wait, wait_ms: 30_000}} = RunnerTasks.claim(request)

    claims = Agent.get(agent, & &1.claims)

    assert Enum.map(claims, & &1.command_id) |> Enum.reverse() ==
             Enum.map(0..3, &"claim-exhausted:#{&1}")

    RunnerQueueCoordinator.notify(@pool, @release, 1)

    assert_receive {:agent_message, {:favn_runner_task, wake}}
    assert wake.runner_instance_id == "runner-exhausted"
  end

  test "the enrolled final claim takes a task and releases the waiter slot", %{agent: agent} do
    session_generation = register("runner-final-claim")
    request = claim_request("runner-final-claim", session_generation, "claim-final")

    task = queued_task("rt_final_claim")
    Agent.update(agent, &%{&1 | replies: [{:ok, nil}, {:ok, nil}, {:ok, nil}, {:ok, task}]})

    assert {:ok, %Assignment{task_id: "rt_final_claim"}} = RunnerTasks.claim(request)

    RunnerQueueCoordinator.notify(@pool, @release, 1)
    refute_receive {:agent_message, {:favn_runner_task, _wake}}, 50
  end

  test "a registry restart after the durable claim does not lose or duplicate the assignment", %{
    agent: agent
  } do
    session_generation = register("runner-registry-restart")
    request = claim_request("runner-registry-restart", session_generation, "claim-restart")
    task = queued_task("rt_registry_restart")

    Agent.update(agent, fn state ->
      %{state | replies: [{:ok, task}], churn?: false, restart_registry?: true}
    end)

    assert {:ok, %Assignment{task_id: "rt_registry_restart"} = assignment} =
             RunnerTasks.claim(request)

    assert [_claim] = Agent.get(agent, & &1.claims)
    assert RunnerRegistry.list() == []

    registration =
      registration("runner-registry-restart")
      |> Map.put(:runner_session_generation, session_generation)
      |> Map.put(:active_assignment, %{
        workspace_id: assignment.workspace_id,
        task_id: assignment.task_id,
        assignment_generation: assignment.assignment_generation
      })

    resumed_agent = spawn(fn -> Process.sleep(:infinity) end)
    on_exit(fn -> Process.exit(resumed_agent, :kill) end)

    assert {:ok, %{status: :accepted}} =
             RunnerRegistry.register_verified(registration, resumed_agent)

    assert {:ok, %{status: :busy, active_assignment: %{task_id: "rt_registry_restart"}}} =
             RunnerRegistry.fetch("runner-registry-restart")
  end

  test "a transient claim failure retries its identity and preserves elastic idle grace", %{
    agent: store
  } do
    {:ok, _} = Application.ensure_all_started(:favn_runner)

    Application.put_env(:favn_orchestrator, :runner_pools,
      duckdb: [mode: :elastic, idle_grace_ms: 60_000]
    )

    error = Error.new(:unavailable, "test checkout failure", retryable?: true)
    Agent.update(store, &%{&1 | replies: [{:error, error}, {:ok, nil}], churn?: false})
    gateway = start_supervised!({ClaimGateway, self()})
    owner = self()

    runner =
      start_supervised!(
        {FavnRunner.RunnerAgent,
         name: nil,
         connection: gateway,
         runner_pool: @pool,
         lifecycle_mode: :elastic,
         exit_fun: fn code -> send(owner, {:unexpected_exit, code}) end}
      )

    assert_receive {:claim_reply, first, {:error, ^error}}, 2_000
    assert_receive {:claim_reply, ^first, {:ok, %NoWork{wait_ms: 60_000}}}, 2_000
    refute_receive {:unexpected_exit, _}, 50
    assert Process.alive?(runner)
    assert :sys.get_state(runner).phase == :waiting
    assert Process.read_timer(:sys.get_state(runner).idle_timer) > 59_000
    assert [second, first_store] = Agent.get(store, & &1.claims)
    assert second.command_id == first_store.command_id
  end

  test "registry loss while reporting a claim failure preserves the original error", %{
    agent: agent
  } do
    generation = register("runner-error-restart")
    request = claim_request("runner-error-restart", generation, "claim-error-restart")
    error = Error.new(:unavailable, "test checkout failure", retryable?: true)
    Agent.update(agent, &%{&1 | replies: [{:error, error}], restart_registry?: true})
    assert {:error, ^error} = RunnerTasks.claim(request)
    assert RunnerRegistry.list() == []
  end

  test "the enrolled final subclaim can fail and retry the same logical command", %{agent: agent} do
    generation = register("runner-final-error")
    request = claim_request("runner-final-error", generation, "claim-final-error")
    error = Error.new(:unavailable, "test lost assignment reply", retryable?: true)
    task = queued_task("rt_replayed_final_claim")

    Agent.update(
      agent,
      &%{&1 | replies: [{:ok, nil}, {:ok, nil}, {:ok, nil}, {:error, error}, {:ok, task}]}
    )

    assert {:error, ^error} = RunnerTasks.claim(request)
    assert {:ok, %Assignment{task_id: "rt_replayed_final_claim"}} = RunnerTasks.claim(request)

    assert Agent.get(
             agent,
             &Enum.map(Enum.reverse(&1.claims), fn command -> command.command_id end)
           ) ==
             [
               "claim-final-error:0",
               "claim-final-error:1",
               "claim-final-error:2",
               "claim-final-error:3",
               "claim-final-error:0"
             ]

    RunnerQueueCoordinator.notify(@pool, @release, 1)
    refute_receive {:agent_message, {:favn_runner_task, _wake}}, 50
  end

  defp register(runner_id) do
    parent = self()
    agent_pid = spawn(fn -> agent_loop(parent) end)

    registration = registration(runner_id)

    assert {:ok, %{status: :accepted} = ack} = RunnerRegistry.register(registration, agent_pid)
    ack.runner_session_generation
  end

  defp registration(runner_id) do
    %Registration{
      runner_instance_id: runner_id,
      boot_id: "boot-#{runner_id}",
      beam_node: Atom.to_string(node()),
      runner_pool: @pool,
      required_runner_release_id: @release,
      lifecycle_mode: :resident,
      supported_task_kinds: [:asset_attempt],
      capabilities: ["asset_execution"]
    }
  end

  defp claim_request(runner_id, session_generation, command_id) do
    %ClaimRequest{
      command_id: command_id,
      issued_at: DateTime.utc_now(),
      runner_instance_id: runner_id,
      runner_session_generation: session_generation,
      runner_pool: @pool,
      required_runner_release_id: @release,
      supported_task_kinds: [:asset_attempt],
      capabilities: ["asset_execution"]
    }
  end

  defp queued_task(task_id) do
    %RunnerTask{
      workspace_id: "workspace-claim",
      task_id: task_id,
      task_kind: :asset_attempt,
      runner_pool: @pool,
      required_runner_release_id: @release,
      retry_class: :unknown_do_not_retry,
      status: :assigned,
      payload: :test_payload,
      assignment_generation: 1,
      assignment_expires_at: DateTime.add(DateTime.utc_now(), 30, :second)
    }
  end

  defp agent_loop(parent) do
    receive do
      message ->
        send(parent, {:agent_message, message})
        agent_loop(parent)
    end
  end
end
