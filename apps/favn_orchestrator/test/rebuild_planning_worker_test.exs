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
    def renew_operation_lease(command) do
      Agent.get_and_update(agent(), fn state ->
        v = state.operation.validation

        result =
          if v.status == "active" and v.owner_id == command.owner_id and
               v.fencing_token == command.fencing_token,
             do: :ok,
             else: {:error, Error.new(:fenced, "validation expired")}

        {result, %{state | claims: state.claims + 1}}
      end)
    end

    def close_validation(command) do
      Agent.get_and_update(agent(), fn state ->
        cond do
          Map.get(state, :close_unavailable, false) ->
            {{:error, Error.new(:unavailable, "offline")}, state}

          state.operation.validation.status == "accepted" ->
            {{:ok, state.operation}, state}

          true ->
            reason =
              if is_struct(command.reason, Error),
                do: command.reason,
                else:
                  FavnOrchestrator.Rebuild.Validation.failure(state.operation.operation_id, :plan)

            operation = %{
              state.operation
              | state: :failed,
                validation: %{
                  state.operation.validation
                  | status: "failed",
                    failure: FavnOrchestrator.Rebuild.Validation.encode_error(reason)
                }
            }

            {{:ok, operation}, %{state | operation: operation}}
        end
      end)
    end

    def get_validation(query) do
      operation = Agent.get(agent(), & &1.operation)

      if operation.validation.attempt_id == query.validation.attempt_id,
        do: {:ok, operation},
        else:
          {:error,
           FavnOrchestrator.Rebuild.Validation.failure(query.validation.operation_id, :plan)}
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
      validation: %{
        FavnOrchestrator.Rebuild.Validation.new(
          "rebuild-planning-worker",
          :plan,
          "request",
          DateTime.utc_now()
        )
        | fencing_token: 1
      },
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

  test "unsupported release retains fixed upgrade guidance", fixture do
    resume = fn _, _ ->
      {:error,
       Error.new(:invalid, "untrusted detail",
         details: %{reason_code: "rebuild_input_resolution_unsupported"}
       )}
    end

    assert {:error, error} =
             RebuildPlanningWorker.ensure_and_await(fixture.context, fixture.operation,
               resume: resume
             )

    assert error.details.reason_code == "rebuild_input_resolution_unsupported"
    assert error.message =~ "Activate an upgraded release and create a new plan"
    refute error.message =~ "untrusted"
  end

  test "repeated live callers share one worker and renew the same fence", fixture do
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

    send(first, :renew)
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

  test "a replay after worker loss does not create a successor", fixture do
    parent = self()

    resume = fn _, _ ->
      send(parent, :started)
      receive do: (:finish -> {:ok, :planned})
    end

    assert {:ok, worker} =
             RebuildPlanningWorker.ensure(fixture.context, fixture.operation, resume: resume)

    assert_receive :started
    ref = Process.monitor(worker)
    Process.exit(worker, :kill)
    assert_receive {:DOWN, ^ref, :process, ^worker, :killed}

    assert_eventually(fn ->
      RebuildPlanningWorker.active_operation_ids(fixture.context.workspace_id) == []
    end)

    Agent.update(fixture.agent, fn state ->
      %{
        state
        | operation: %{
            state.operation
            | validation: %{
                state.operation.validation
                | status: "failed",
                  failure:
                    FavnOrchestrator.Rebuild.Validation.encode_error(
                      FavnOrchestrator.Rebuild.Validation.failure(
                        state.operation.operation_id,
                        :plan
                      )
                    )
              }
          }
      }
    end)

    assert {:error, %{details: %{reason_code: "rebuild_planning_failed"}}} =
             RebuildPlanningWorker.await_existing(fixture.context, fixture.operation)

    refute_receive :started
  end

  test "replay of an older attempt cannot join a newer worker", fixture do
    parent = self()

    fresh = %{
      fixture.operation
      | validation: %{fixture.operation.validation | attempt_id: "fresh"}
    }

    Agent.update(fixture.agent, &%{&1 | operation: fresh})

    assert {:ok, worker} =
             RebuildPlanningWorker.ensure(fixture.context, fresh,
               resume: fn _, _ ->
                 send(parent, :started)
                 receive do: (:finish -> {:ok, :planned})
               end
             )

    assert_receive :started
    assert {:error, _} = RebuildPlanningWorker.await_existing(fixture.context, fixture.operation)
    assert Process.alive?(worker)
    GenServer.stop(worker)
  end

  test "a lost acceptance reply returns committed success", fixture do
    resume = fn _, _ ->
      Agent.update(fixture.agent, fn state ->
        %{
          state
          | operation: %{
              state.operation
              | state: :planned,
                validation: %{state.operation.validation | status: "accepted"}
            }
        }
      end)

      {:error, Error.new(:unavailable, "reply lost")}
    end

    assert {:ok, %{state: :planned}} =
             RebuildPlanningWorker.ensure_and_await(fixture.context, fixture.operation,
               resume: resume
             )
  end

  test "unavailable closure returns uncertainty instead of terminal rejection", fixture do
    Agent.update(fixture.agent, &Map.put(&1, :close_unavailable, true))

    assert {:error, %{kind: :unavailable, retryable?: true}} =
             RebuildPlanningWorker.ensure_and_await(fixture.context, fixture.operation,
               resume: fn _, _ -> {:error, :disconnected} end
             )
  end

  test "a replay observes a live attempt owned elsewhere without starting work", fixture do
    waiter =
      Task.async(fn ->
        RebuildPlanningWorker.await_existing(fixture.context, fixture.operation)
      end)

    refute Task.yield(waiter, 100)

    Agent.update(fixture.agent, fn state ->
      %{
        state
        | operation: %{
            state.operation
            | state: :planned,
              validation: %{state.operation.validation | status: "accepted"}
          }
      }
    end)

    assert {:ok, %{state: :planned}} = Task.await(waiter)
    assert Agent.get(fixture.agent, & &1.claims) == 0
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
