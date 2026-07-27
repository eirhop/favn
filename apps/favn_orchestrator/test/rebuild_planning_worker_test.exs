defmodule FavnOrchestrator.RebuildPlanningWorkerTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Results.RebuildLease
  alias FavnOrchestrator.Persistence.Results.RebuildOperation
  alias FavnOrchestrator.Persistence.Runtime
  alias FavnOrchestrator.Persistence.Stores
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.RebuildPlanningWorker

  defmodule Store do
    def claim_operation(command) do
      Agent.get_and_update(agent(), fn state ->
        operation = state.operation
        claims = state.claims + 1

        if operation.state == :planning do
          token =
            if operation.dispatcher.owner in [nil, command.owner_id],
              do: max(operation.dispatcher.fencing_token, 1),
              else: operation.dispatcher.fencing_token + 1

          claimed = %{
            operation
            | dispatcher: %RebuildLease{
                owner: command.owner_id,
                fencing_token: token,
                expires_at:
                  DateTime.add(DateTime.utc_now(), command.lease_duration_ms, :millisecond)
              },
              version: operation.version + 1
          }

          {{:ok, claimed}, %{state | operation: claimed, claims: claims}}
        else
          {{:ok, nil}, %{state | claims: claims}}
        end
      end)
    end

    def transition_operation(command) do
      Agent.get_and_update(agent(), fn state ->
        operation = state.operation

        cond do
          operation.state != :planning ->
            {{:error, Error.new(:conflict, "planning already changed")}, state}

          operation.dispatcher.owner != command.owner_id or
              operation.dispatcher.fencing_token != command.fencing_token ->
            {{:error, Error.new(:fenced, "planning fence changed")}, state}

          true ->
            failed = %{
              operation
              | state: command.state,
                phase: command.phase,
                terminal_error: command.terminal_error,
                dispatcher: %RebuildLease{},
                version: operation.version + 1
            }

            {{:ok, failed}, %{state | operation: failed}}
        end
      end)
    end

    def get(_query), do: {:ok, Agent.get(agent(), & &1.operation)}

    defp agent,
      do: Application.fetch_env!(:favn_orchestrator, :rebuild_planning_worker_test_agent)
  end

  setup do
    start_named_supervision()

    operation = %RebuildOperation{
      workspace_id: "workspace-planning-worker",
      operation_id: "rebuild-planning-worker",
      root_target_id: "target-planning-worker",
      manifest_version_id: "manifest-planning-worker",
      plan_hash: String.duplicate("a", 64),
      plan_version: 1,
      plan_payload: %{},
      actor_id: "operator",
      reason: "test planning worker",
      idempotency_key: "rebuild-planning-worker",
      evaluated_at: DateTime.utc_now(),
      action_count: 0,
      window_count: 0,
      state: :planning,
      phase: :planning,
      version: 1,
      dispatcher: %RebuildLease{}
    }

    {:ok, agent} = Agent.start_link(fn -> %{operation: operation, claims: 0} end)
    Application.put_env(:favn_orchestrator, :rebuild_planning_worker_test_agent, agent)

    stores = struct(Stores, rebuilds: Store)
    start_supervised!({Runtime, %Runtime{backend: __MODULE__, options: [], stores: stores}})

    on_exit(fn ->
      Application.delete_env(:favn_orchestrator, :rebuild_planning_worker_test_agent)
    end)

    {:ok, context} =
      WorkspaceContext.new("workspace-planning-worker", "operator", [:workspace_admin])

    %{agent: agent, context: context, operation: operation}
  end

  test "repeated dispatcher ensures share one worker and renew one durable fence", fixture do
    parent = self()

    resume = fn _context, operation ->
      send(parent, {:planning_started, operation.operation_id, self()})

      receive do
        :finish_planning -> {:ok, :planned}
      end
    end

    opts = [owner_id: "dispatcher-owner", lease_duration_ms: 300, resume: resume]

    assert {:ok, first} = RebuildPlanningWorker.ensure(fixture.context, fixture.operation, opts)
    assert_receive {:planning_started, "rebuild-planning-worker", planning_task}

    for _ <- 1..5 do
      assert {:ok, ^first} =
               RebuildPlanningWorker.ensure(fixture.context, fixture.operation, opts)
    end

    assert RebuildPlanningWorker.active_operation_ids(fixture.context.workspace_id) == [
             fixture.operation.operation_id
           ]

    assert_eventually(fn -> Agent.get(fixture.agent, & &1.claims) >= 2 end)
    refute_receive {:planning_started, "rebuild-planning-worker", _duplicate}, 100

    send(planning_task, :finish_planning)
    monitor = Process.monitor(first)
    assert_receive {:DOWN, ^monitor, :process, ^first, :normal}
  end

  test "cancellation stops renewal and the cancellation-aware continuation exits", fixture do
    parent = self()

    resume = fn _context, operation ->
      send(parent, {:planning_started, operation.operation_id, self()})
      wait_until_cancelled(fixture.agent)
      send(parent, {:planning_observed_cancel, operation.operation_id})
      {:error, :rebuild_operation_cancelled}
    end

    assert {:ok, worker} =
             RebuildPlanningWorker.ensure(fixture.context, fixture.operation,
               owner_id: "dispatcher-owner",
               lease_duration_ms: 300,
               resume: resume
             )

    assert_receive {:planning_started, "rebuild-planning-worker", _planning_task}

    Agent.update(fixture.agent, fn state ->
      %{state | operation: %{state.operation | state: :cancelled, phase: :terminal}}
    end)

    monitor = Process.monitor(worker)
    assert_receive {:planning_observed_cancel, "rebuild-planning-worker"}
    assert_receive {:DOWN, ^monitor, :process, ^worker, :normal}

    claims_after_stop = Agent.get(fixture.agent, & &1.claims)
    Process.sleep(150)
    assert Agent.get(fixture.agent, & &1.claims) == claims_after_stop
  end

  test "a stopped worker can be recovered under a new fence owner", fixture do
    parent = self()

    resume = fn _context, operation ->
      send(parent, {:planning_started, operation.operation_id, self()})

      receive do
        :finish_planning -> {:ok, :planned}
      end
    end

    assert {:ok, first} =
             RebuildPlanningWorker.ensure(fixture.context, fixture.operation,
               owner_id: "first-owner",
               lease_duration_ms: 300,
               resume: resume
             )

    assert_receive {:planning_started, "rebuild-planning-worker", _first_task}
    first_monitor = Process.monitor(first)
    Process.exit(first, :kill)
    assert_receive {:DOWN, ^first_monitor, :process, ^first, :killed}

    assert_eventually(fn ->
      RebuildPlanningWorker.active_operation_ids(fixture.context.workspace_id) == []
    end)

    current = Agent.get(fixture.agent, & &1.operation)

    assert {:ok, second} =
             RebuildPlanningWorker.ensure(fixture.context, current,
               owner_id: "second-owner",
               lease_duration_ms: 300,
               resume: resume
             )

    refute second == first
    assert_receive {:planning_started, "rebuild-planning-worker", second_task}
    send(second_task, :finish_planning)
    assert_eventually(fn -> not Process.alive?(second) end)
  end

  defp start_named_supervision do
    unless Process.whereis(FavnOrchestrator.RebuildPlanningRegistry) do
      start_supervised!({Registry, keys: :unique, name: FavnOrchestrator.RebuildPlanningRegistry})
    end

    unless Process.whereis(FavnOrchestrator.RebuildPlanningSupervisor) do
      start_supervised!(
        {DynamicSupervisor,
         strategy: :one_for_one, name: FavnOrchestrator.RebuildPlanningSupervisor}
      )
    end
  end

  defp wait_until_cancelled(agent) do
    if Agent.get(agent, &(&1.operation.state == :cancelled)) do
      :ok
    else
      Process.sleep(10)
      wait_until_cancelled(agent)
    end
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
