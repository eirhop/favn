defmodule FavnRunner.RunnerAgentTest do
  use ExUnit.Case, async: false

  alias Favn.Contracts.RunnerTask
  alias Favn.Contracts.RunnerResult
  alias FavnRunner.RunnerAgent
  alias FavnRunner.TaskExecutor

  defmodule FakeControlPlane do
    use GenServer

    def start_link(owner), do: GenServer.start_link(__MODULE__, owner)

    @impl true
    def init(owner), do: {:ok, owner}

    @impl true
    def handle_call(:gateway, _from, owner), do: {:reply, {:ok, self()}, owner}

    def handle_call({:register, registration, agent}, _from, owner) do
      send(owner, {:registered, registration, agent})

      {:reply,
       {:ok,
        %RunnerTask.RegistrationAck{
          runner_instance_id: registration.runner_instance_id,
          runner_session_generation: max(registration.runner_session_generation, 1),
          status: :accepted
        }}, owner}
    end

    def handle_call({:request, %RunnerTask.ClaimRequest{} = request}, _from, owner) do
      send(owner, {:claimed, request})

      {:reply,
       {:ok,
        %RunnerTask.NoWork{
          command_id: request.command_id,
          runner_instance_id: request.runner_instance_id,
          runner_session_generation: request.runner_session_generation,
          action: :wait,
          wait_ms: 5
        }}, owner}
    end

    def handle_call({:request, %RunnerTask.CancellationAck{} = acknowledgement}, _from, owner) do
      send(owner, {:cancellation_ack, acknowledgement})
      {:reply, {:ok, acknowledgement}, owner}
    end

    def handle_call({:request, %RunnerTask.LeaseRenewal{} = renewal}, _from, owner) do
      send(owner, {:lease_renewal, renewal})
      {:reply, {:error, :control_plane_unavailable}, owner}
    end

    def handle_call({:request, %RunnerTask.Result{} = result}, _from, owner) do
      send(owner, {:result_delivery, result})
      {:reply, {:error, %{kind: :fenced}}, owner}
    end
  end

  defmodule RejectedControlPlane do
    use GenServer

    def start_link(owner), do: GenServer.start_link(__MODULE__, owner)

    @impl true
    def init(owner), do: {:ok, owner}

    @impl true
    def handle_call(:gateway, _from, owner), do: {:reply, {:ok, self()}, owner}

    def handle_call({:register, registration, agent}, _from, owner) do
      send(owner, {:rejected_registration, registration, agent})

      {:reply,
       {:ok,
        %RunnerTask.RegistrationAck{
          runner_instance_id: registration.runner_instance_id,
          runner_session_generation: registration.runner_session_generation,
          status: :rejected,
          reason: :runner_lifecycle_mode_mismatch
        }}, owner}
    end
  end

  defmodule FakeExecutor do
    use GenServer

    def start_link(owner), do: GenServer.start_link(__MODULE__, owner)

    @impl true
    def init(owner), do: {:ok, owner}

    @impl true
    def handle_call({:cancel, reason}, _from, owner) do
      send(owner, {:executor_cancelled, reason})
      {:reply, :ok, owner}
    end
  end

  defmodule FinishedExecutor do
    use GenServer

    def start_link(_owner), do: GenServer.start_link(__MODULE__, nil)

    @impl true
    def init(state), do: {:ok, state}

    @impl true
    def handle_call({:cancel, _reason}, _from, state),
      do: {:reply, {:error, :already_finished}, state}
  end

  defmodule RuntimeInputReplayControlPlane do
    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, Map.new(opts))

    @impl true
    def init(state),
      do: {:ok, Map.merge(state, %{agent: nil, assignment: nil, runtime_inputs_seen: 0})}

    @impl true
    def handle_call(:gateway, _from, state), do: {:reply, {:ok, self()}, state}

    def handle_call({:register, registration, agent}, _from, state) do
      assignment =
        state.assignment ||
          %RunnerTask.Assignment{
            command_id: "runtime-input-claim",
            workspace_id: "workspace-runtime-input",
            task_id: "rt_runtime_input_replay",
            task_kind: :asset_attempt,
            runner_instance_id: registration.runner_instance_id,
            runner_session_generation: 1,
            assignment_generation: 1,
            runner_pool: "duckdb",
            required_runner_release_id: FavnTestSupport.runner_release_id(),
            lease_expires_at: DateTime.add(DateTime.utc_now(), 30, :second),
            retry_class: :unknown_do_not_retry,
            payload: state.work
          }

      send(state.owner, {:runtime_input_registration, registration, agent})

      ack = %RunnerTask.RegistrationAck{
        runner_instance_id: registration.runner_instance_id,
        runner_session_generation: 1,
        status: :accepted
      }

      {:reply, {:ok, ack}, %{state | assignment: assignment, agent: agent}}
    end

    def handle_call({:request, %RunnerTask.ClaimRequest{}}, _from, state),
      do: {:reply, {:ok, state.assignment}, state}

    def handle_call({:fetch_manifest, _assignment}, _from, state),
      do: {:reply, {:ok, state.version}, state}

    def handle_call(
          {:request, %RunnerTask.RuntimeInputsResolved{} = message},
          _from,
          %{runtime_inputs_seen: 0} = state
        ) do
      send(state.owner, {:runtime_inputs_committed_before_ack, message})
      {:reply, {:error, :control_plane_unavailable}, %{state | runtime_inputs_seen: 1}}
    end

    def handle_call(
          {:request, %RunnerTask.RuntimeInputsResolved{} = message},
          _from,
          state
        ) do
      send(state.owner, {:runtime_inputs_replayed, message})

      ack = %RunnerTask.RuntimeInputsAck{
        workspace_id: message.workspace_id,
        task_id: message.task_id,
        runner_instance_id: message.runner_instance_id,
        runner_session_generation: message.runner_session_generation,
        assignment_generation: message.assignment_generation,
        resolution_id: message.resolution_id,
        payload_fingerprint: message.runtime_inputs.payload_fingerprint,
        status: :persisted
      }

      {:reply, {:ok, ack}, %{state | runtime_inputs_seen: state.runtime_inputs_seen + 1}}
    end

    def handle_call({:request, %RunnerTask.Started{}}, _from, state),
      do: {:reply, {:error, :stop_after_runtime_input_replay}, state}

    def handle_call({:request, %RunnerTask.Result{} = result}, _from, state) do
      ack = %RunnerTask.ResultAck{
        workspace_id: result.workspace_id,
        task_id: result.task_id,
        runner_instance_id: result.runner_instance_id,
        runner_session_generation: result.runner_session_generation,
        assignment_generation: result.assignment_generation,
        result_version: result.result_version,
        status: :persisted
      }

      {:reply, {:ok, ack}, state}
    end

    @impl true
    def handle_cast(:connect, state), do: {:noreply, state}
  end

  defmodule RestartingControlPlane do
    use GenServer

    def start_link(owner), do: GenServer.start_link(__MODULE__, owner)
    def fail_next(server, kind), do: GenServer.call(server, {:fail_next, kind})

    @impl true
    def init(owner),
      do: {:ok, %{owner: owner, registration_required?: false, fail_next: nil}}

    @impl true
    def handle_call(:gateway, _from, state), do: {:reply, {:ok, self()}, state}

    def handle_call({:fail_next, kind}, _from, state),
      do: {:reply, :ok, %{state | fail_next: kind}}

    def handle_call({:register, registration, agent}, _from, state) do
      send(state.owner, {:restart_registration, registration, agent})

      reply =
        {:ok,
         %RunnerTask.RegistrationAck{
           runner_instance_id: registration.runner_instance_id,
           runner_session_generation: max(registration.runner_session_generation, 1),
           status: :accepted
         }}

      {:reply, reply, %{state | registration_required?: false}}
    end

    def handle_call({:request, message}, _from, %{registration_required?: true} = state) do
      send(state.owner, {:request_before_resume, message})
      {:reply, {:error, :runner_session_not_found}, state}
    end

    def handle_call({:request, %RunnerTask.ClaimRequest{} = request}, _from, state) do
      no_work = %RunnerTask.NoWork{
        command_id: request.command_id,
        runner_instance_id: request.runner_instance_id,
        runner_session_generation: request.runner_session_generation,
        action: :wait,
        wait_ms: 60_000
      }

      {:reply, {:ok, no_work}, state}
    end

    def handle_call(
          {:request, %RunnerTask.Result{} = result},
          _from,
          %{fail_next: :result} = state
        ) do
      send(state.owner, {:result_interrupted, result})

      {:reply, {:error, :control_plane_unavailable},
       %{state | fail_next: nil, registration_required?: true}}
    end

    def handle_call({:request, %RunnerTask.Result{} = result}, _from, state) do
      send(state.owner, {:result_resumed, result})

      ack = %RunnerTask.ResultAck{
        workspace_id: result.workspace_id,
        task_id: result.task_id,
        runner_instance_id: result.runner_instance_id,
        runner_session_generation: result.runner_session_generation,
        assignment_generation: result.assignment_generation,
        result_version: result.result_version,
        status: :persisted
      }

      {:reply, {:ok, ack}, state}
    end

    def handle_call(
          {:request, %RunnerTask.LeaseRenewal{} = renewal},
          _from,
          %{fail_next: :renewal} = state
        ) do
      send(state.owner, {:renewal_interrupted, renewal})

      {:reply, {:error, :control_plane_unavailable},
       %{state | fail_next: nil, registration_required?: true}}
    end

    def handle_call({:request, %RunnerTask.LeaseRenewal{} = renewal}, _from, state) do
      send(state.owner, {:renewal_resumed, renewal})
      {:reply, {:ok, %{lease_expires_at: renewal.lease_expires_at}}, state}
    end

    @impl true
    def handle_cast(:connect, state), do: {:noreply, state}
  end

  defmodule ExecutingControlPlane do
    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, Map.new(opts))
    def block_logs(server), do: GenServer.call(server, :block_logs)

    @impl true
    def init(state), do: {:ok, Map.put(state, :assignment, nil)}

    @impl true
    def handle_call(:gateway, _from, state), do: {:reply, {:ok, self()}, state}

    def handle_call(:block_logs, _from, state),
      do: {:reply, :ok, Map.put(state, :block_logs?, true)}

    def handle_call({:register, registration, agent}, _from, state) do
      assignment = %RunnerTask.Assignment{
        command_id: "execute-claim",
        workspace_id: "workspace-execute",
        task_id: "rt_" <> state.work.run_id,
        task_kind: :asset_attempt,
        runner_instance_id: registration.runner_instance_id,
        runner_session_generation: 1,
        assignment_generation: 1,
        runner_pool: "duckdb",
        required_runner_release_id: FavnTestSupport.runner_release_id(),
        lease_expires_at: DateTime.add(DateTime.utc_now(), 30, :second),
        retry_class: :unknown_do_not_retry,
        payload: state.work
      }

      ack = %RunnerTask.RegistrationAck{
        runner_instance_id: registration.runner_instance_id,
        runner_session_generation: 1,
        status: :accepted
      }

      {:reply, {:ok, ack}, Map.merge(state, %{assignment: assignment, agent: agent})}
    end

    def handle_call({:request, %RunnerTask.ClaimRequest{}}, _from, state),
      do: {:reply, {:ok, state.assignment}, state}

    def handle_call({:fetch_manifest, _assignment}, _from, state),
      do: {:reply, {:ok, state.version}, state}

    def handle_call({:request, %RunnerTask.Started{} = started}, _from, state) do
      send(state.owner, {:task_started, started, state.agent})
      {:reply, {:ok, %{status: :running}}, state}
    end

    def handle_call({:request, %RunnerTask.CancellationAck{} = ack}, _from, state),
      do: {:reply, {:ok, ack}, state}

    def handle_call({:request, %RunnerTask.LogBatch{} = batch}, _from, state) do
      if Map.get(state, :block_logs?, false) do
        send(state.owner, :log_delivery_blocked)

        receive do
          :release_logs -> :ok
        end
      end

      ack = %RunnerTask.LogAck{
        workspace_id: batch.workspace_id,
        task_id: batch.task_id,
        runner_instance_id: batch.runner_instance_id,
        runner_session_generation: batch.runner_session_generation,
        assignment_generation: batch.assignment_generation,
        batch_id: batch.batch_id,
        sequence: batch.sequence
      }

      {:reply, {:ok, ack}, Map.put(state, :block_logs?, false)}
    end

    def handle_call({:request, %RunnerTask.Result{} = result}, _from, state) do
      send(state.owner, {:cancelled_result_persisted, result})

      ack = %RunnerTask.ResultAck{
        workspace_id: result.workspace_id,
        task_id: result.task_id,
        runner_instance_id: result.runner_instance_id,
        runner_session_generation: result.runner_session_generation,
        assignment_generation: result.assignment_generation,
        result_version: result.result_version,
        status: :persisted
      }

      {:reply, {:ok, ack}, %{state | assignment: nil}}
    end
  end

  test "an elastic runner honors the control-plane wait and exits after a final empty claim" do
    owner = self()
    {:ok, control_plane} = start_supervised({FakeControlPlane, self()})

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         exit_fun: fn status -> send(owner, {:runner_exit, status}) end}
      )

    assert_receive {:registered, %RunnerTask.Registration{lifecycle_mode: :elastic}, ^agent}
    assert_receive {:claimed, %RunnerTask.ClaimRequest{}}
    assert_receive {:claimed, %RunnerTask.ClaimRequest{}}, 500
    assert_receive {:runner_exit, 0}, 500
    assert_eventually(fn -> not Process.alive?(agent) end)
  end

  test "a rejected boot registration exits non-zero" do
    owner = self()
    {:ok, control_plane} = start_supervised({RejectedControlPlane, owner})

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         exit_fun: fn status -> send(owner, {:runner_exit, status}) end}
      )

    assert_receive {:rejected_registration, %RunnerTask.Registration{}, ^agent}
    assert_receive {:runner_exit, 1}, 500
    assert_eventually(fn -> not Process.alive?(agent) end)
  end

  test "an elastic runner exits at bounded maximum uptime while idle" do
    owner = self()
    {:ok, control_plane} = start_supervised({FakeControlPlane, self()})

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         max_uptime_ms: 10,
         exit_fun: fn status -> send(owner, {:runner_exit, status}) end}
      )

    assert_receive {:registered, %RunnerTask.Registration{}, ^agent}
    assert_receive {:runner_exit, 0}, 500
    assert_eventually(fn -> not Process.alive?(agent) end)
  end

  test "a resident runner parks until the control plane wakes it" do
    {:ok, control_plane} = start_supervised({FakeControlPlane, self()})

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         lifecycle_mode: :resident,
         exit_fun: fn _status -> :ok end}
      )

    assert_receive {:registered, registration, ^agent}
    assert_receive {:claimed, %RunnerTask.ClaimRequest{}}
    refute_receive {:claimed, %RunnerTask.ClaimRequest{}}, 50

    send(
      agent,
      {:favn_runner_task,
       %RunnerTask.Wake{
         runner_instance_id: registration.runner_instance_id,
         runner_session_generation: 1,
         runner_pool: registration.runner_pool,
         required_runner_release_id: registration.required_runner_release_id
       }}
    )

    assert_receive {:claimed, %RunnerTask.ClaimRequest{}}, 500
  end

  test "cancellation is acknowledged and applied only to the exact assignment fence" do
    {:ok, control_plane} = start_supervised({FakeControlPlane, self()})
    {:ok, executor} = start_supervised({FakeExecutor, self()})

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         lifecycle_mode: :resident,
         exit_fun: fn _status -> :ok end}
      )

    assert_receive {:registered, registration, ^agent}
    assert_receive {:claimed, %RunnerTask.ClaimRequest{}}

    assignment = assignment(registration.runner_instance_id)

    :sys.replace_state(agent, fn state ->
      %{
        state
        | assignment: assignment,
          executor: executor,
          session_generation: assignment.runner_session_generation,
          phase: :running
      }
    end)

    send(
      agent,
      {:favn_runner_task,
       %RunnerTask.Cancellation{
         workspace_id: assignment.workspace_id,
         task_id: assignment.task_id,
         runner_instance_id: assignment.runner_instance_id,
         runner_session_generation: assignment.runner_session_generation,
         assignment_generation: assignment.assignment_generation,
         command_id: "cancel-task",
         reason: :operator_request,
         requested_at: DateTime.utc_now()
       }}
    )

    assert_receive {:cancellation_ack, %RunnerTask.CancellationAck{status: :observed}}
    assert_receive {:executor_cancelled, :operator_request}
  end

  test "cancellation is rejected after execution has already reached reporting" do
    {:ok, control_plane} = start_supervised({FakeControlPlane, self()})

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         lifecycle_mode: :resident,
         exit_fun: fn _status -> :ok end}
      )

    assert_receive {:registered, registration, ^agent}
    assert_receive {:claimed, %RunnerTask.ClaimRequest{}}

    assignment = assignment(registration.runner_instance_id)

    :sys.replace_state(agent, fn state ->
      %{
        state
        | assignment: assignment,
          executor: nil,
          session_generation: assignment.runner_session_generation,
          phase: :reporting
      }
    end)

    send(
      agent,
      {:favn_runner_task,
       %RunnerTask.Cancellation{
         workspace_id: assignment.workspace_id,
         task_id: assignment.task_id,
         runner_instance_id: assignment.runner_instance_id,
         runner_session_generation: assignment.runner_session_generation,
         assignment_generation: assignment.assignment_generation,
         command_id: "cancel-reported-task",
         reason: :operator_request,
         requested_at: DateTime.utc_now()
       }}
    )

    assert_receive {:cancellation_ack, %RunnerTask.CancellationAck{status: :rejected}}
    refute_receive {:executor_cancelled, _reason}
  end

  test "runtime-input-only asset tasks finish without starting customer execution" do
    assignment = assignment("runner-runtime-input-resolution")

    work = %{
      assignment.payload
      | run_id: "run_runtime_input_resolution",
        asset_step_id: "step_runtime_input_resolution",
        attempt: 1,
        metadata: %{runner_task_mode: :runtime_input_resolution}
    }

    assignment = %{assignment | payload: work}

    assert {:ok, executor} =
             TaskExecutor.start_link(assignment: assignment, payload: work, owner: self())

    assert_receive {:runner_task_finished, ^executor,
                    %RunnerResult{
                      run_id: "run_runtime_input_resolution",
                      status: :ok,
                      asset_results: []
                    }}
  end

  test "cancellation racing a terminal executor is rejected without crashing the runner" do
    {:ok, control_plane} = start_supervised({FakeControlPlane, self()})
    {:ok, executor} = start_supervised({FinishedExecutor, self()})

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         lifecycle_mode: :resident,
         exit_fun: fn _status -> :ok end}
      )

    assert_receive {:registered, registration, ^agent}
    assert_receive {:claimed, %RunnerTask.ClaimRequest{}}
    assignment = assignment(registration.runner_instance_id)

    :sys.replace_state(agent, fn state ->
      %{
        state
        | assignment: assignment,
          executor: executor,
          session_generation: assignment.runner_session_generation,
          phase: :running
      }
    end)

    send(agent, {:favn_runner_task, cancellation(assignment, "cancel-terminal-race")})

    assert_receive {:cancellation_ack, %RunnerTask.CancellationAck{status: :rejected}}
    assert Process.alive?(agent)

    Process.exit(executor, :kill)
    send(agent, {:favn_runner_task, cancellation(assignment, "cancel-dead-executor-race")})

    assert_receive {:cancellation_ack, %RunnerTask.CancellationAck{status: :rejected}}
    assert Process.alive?(agent)
  end

  test "an executor process loss produces one conservative durable result" do
    owner = self()
    {:ok, control_plane} = start_supervised({FakeControlPlane, owner})
    {:ok, executor} = start_supervised({FakeExecutor, owner})
    :ok = FavnRunner.TaskResultBuffer.reset()

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         lifecycle_mode: :resident,
         exit_fun: fn status -> send(owner, {:runner_exit, status}) end}
      )

    assert_receive {:registered, registration, ^agent}
    assert_receive {:claimed, %RunnerTask.ClaimRequest{}}
    assignment = assignment(registration.runner_instance_id)

    :sys.replace_state(agent, fn state ->
      %{
        state
        | assignment: assignment,
          executor: executor,
          executor_monitor: Process.monitor(executor),
          session_generation: assignment.runner_session_generation,
          phase: :running
      }
    end)

    Process.exit(executor, :kill)

    assert_receive {:result_delivery,
                    %RunnerTask.Result{
                      task_id: "rt_task",
                      outcome: :unknown,
                      retry_class: :unknown_do_not_retry
                    }}

    assert_receive {:runner_exit, 0}
    refute_receive {:result_delivery, %RunnerTask.Result{task_id: "rt_task"}}, 100
  end

  test "an expired unrenewed lease stops execution and enters conservative drain" do
    {:ok, control_plane} = start_supervised({FakeControlPlane, self()})
    {:ok, executor} = start_supervised({FakeExecutor, self()})

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         lifecycle_mode: :resident,
         exit_fun: fn _status -> :ok end}
      )

    assert_receive {:registered, registration, ^agent}
    assert_receive {:claimed, %RunnerTask.ClaimRequest{}}

    assignment = %{
      assignment(registration.runner_instance_id)
      | lease_expires_at: DateTime.add(DateTime.utc_now(), -1, :second)
    }

    :sys.replace_state(agent, fn state ->
      %{
        state
        | assignment: assignment,
          executor: executor,
          session_generation: assignment.runner_session_generation,
          phase: :running
      }
    end)

    send(agent, :renew_lease)

    assert_receive {:lease_renewal, %RunnerTask.LeaseRenewal{}}
    assert_receive {:executor_cancelled, :runner_task_lease_lost}

    state = :sys.get_state(agent)
    assert state.phase == :lease_lost
    assert state.draining?
  end

  test "a stale result fence is abandoned instead of retrying forever" do
    owner = self()
    {:ok, control_plane} = start_supervised({FakeControlPlane, self()})
    :ok = FavnRunner.TaskResultBuffer.reset()

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         lifecycle_mode: :resident,
         exit_fun: fn status -> send(owner, {:runner_exit, status}) end}
      )

    assert_receive {:registered, registration, ^agent}
    assert_receive {:claimed, %RunnerTask.ClaimRequest{}}

    assignment = assignment(registration.runner_instance_id)

    result = %RunnerTask.Result{
      workspace_id: assignment.workspace_id,
      task_id: assignment.task_id,
      task_kind: assignment.task_kind,
      runner_instance_id: assignment.runner_instance_id,
      runner_session_generation: assignment.runner_session_generation,
      assignment_generation: assignment.assignment_generation,
      outcome: :unknown,
      retry_class: :unknown_do_not_retry,
      error: Favn.Contracts.RunnerError.new(outcome: :unknown, retryable?: false),
      finished_at: DateTime.utc_now()
    }

    :ok = FavnRunner.TaskResultBuffer.reset()
    :ok = FavnRunner.TaskResultBuffer.put_result(result)

    :sys.replace_state(agent, fn state ->
      %{
        state
        | assignment: assignment,
          executor: nil,
          session_generation: assignment.runner_session_generation,
          phase: :reporting
      }
    end)

    send(agent, :deliver_result)

    assert_receive {:result_delivery, ^result}
    assert_receive {:runner_exit, 0}
    assert_eventually(fn -> not Process.alive?(agent) end)
    assert nil == FavnRunner.TaskResultBuffer.pending_result()
  end

  test "an active assignment re-registers before result delivery resumes" do
    {:ok, control_plane} = start_supervised({RestartingControlPlane, self()})
    :ok = FavnRunner.TaskResultBuffer.reset()

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         lifecycle_mode: :resident,
         exit_fun: fn _status -> :ok end}
      )

    assert_receive {:restart_registration, registration, ^agent}
    assignment = assignment(registration.runner_instance_id)

    result = %RunnerTask.Result{
      workspace_id: assignment.workspace_id,
      task_id: assignment.task_id,
      task_kind: assignment.task_kind,
      runner_instance_id: assignment.runner_instance_id,
      runner_session_generation: assignment.runner_session_generation,
      assignment_generation: assignment.assignment_generation,
      outcome: :unknown,
      retry_class: :unknown_do_not_retry,
      result: nil,
      error: Favn.Contracts.RunnerError.new(outcome: :unknown),
      finished_at: DateTime.utc_now()
    }

    :ok = FavnRunner.TaskResultBuffer.put_result(result)
    :ok = RestartingControlPlane.fail_next(control_plane, :result)

    :sys.replace_state(agent, fn state ->
      %{
        state
        | assignment: assignment,
          session_generation: assignment.runner_session_generation,
          phase: :reporting
      }
    end)

    send(agent, :deliver_result)

    assert_receive {:result_interrupted, ^result}

    assert_receive {:restart_registration,
                    %RunnerTask.Registration{active_assignment: active_assignment}, ^agent},
                   2_000

    assert active_assignment.task_id == assignment.task_id
    refute_receive {:request_before_resume, _message}
    assert_receive {:result_resumed, ^result}
    assert_eventually(fn -> FavnRunner.TaskResultBuffer.pending_result() == nil end)
  end

  test "an executing assignment re-registers before lease renewal resumes" do
    {:ok, control_plane} = start_supervised({RestartingControlPlane, self()})
    {:ok, executor} = start_supervised({FakeExecutor, self()})
    :ok = FavnRunner.TaskResultBuffer.reset()

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         lifecycle_mode: :resident,
         exit_fun: fn _status -> :ok end}
      )

    assert_receive {:restart_registration, registration, ^agent}
    assignment = assignment(registration.runner_instance_id)
    :ok = RestartingControlPlane.fail_next(control_plane, :renewal)

    :sys.replace_state(agent, fn state ->
      %{
        state
        | assignment: assignment,
          executor: executor,
          session_generation: assignment.runner_session_generation,
          phase: :running
      }
    end)

    send(agent, :renew_lease)

    assert_receive {:renewal_interrupted, %RunnerTask.LeaseRenewal{}}

    assert_receive {:restart_registration,
                    %RunnerTask.Registration{active_assignment: active_assignment}, ^agent},
                   2_000

    assert active_assignment.task_id == assignment.task_id
    refute_receive {:request_before_resume, _message}
    assert_receive {:renewal_resumed, %RunnerTask.LeaseRenewal{}}, 2_000
  end

  test "runtime input resolution replays the exact committed payload after a lost acknowledgement" do
    :ok = FavnRunner.TaskResultBuffer.reset()
    {version, work} = runtime_input_version_and_work()
    {:ok, resolver_calls} = Agent.start_link(fn -> 0 end)

    resolver = fn _work ->
      call = Agent.get_and_update(resolver_calls, fn count -> {count + 1, count + 1} end)

      Favn.RuntimeInput.Resolution.new(%{
        resolver: __MODULE__,
        params: %{region: if(call == 1, do: "eu", else: "us")},
        input_identity: "settings-#{call}",
        metadata: %{},
        sensitive_params: []
      })
    end

    {:ok, control_plane} =
      start_supervised(
        {RuntimeInputReplayControlPlane, owner: self(), version: version, work: work}
      )

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         lifecycle_mode: :resident,
         runtime_input_resolver: resolver,
         exit_fun: fn _status -> :ok end}
      )

    assert_receive {:runtime_input_registration, %RunnerTask.Registration{}, ^agent}

    assert_receive {:runtime_inputs_committed_before_ack,
                    %RunnerTask.RuntimeInputsResolved{} = first},
                   2_000

    assert_receive {:runtime_input_registration,
                    %RunnerTask.Registration{active_assignment: active}, ^agent},
                   2_000

    assert active.task_id == first.task_id
    assert_receive {:runtime_inputs_replayed, %RunnerTask.RuntimeInputsResolved{} = replay}, 2_000
    assert replay == first
    assert Agent.get(resolver_calls, & &1) == 1
  end

  test "real task executor cancellation reaches a valid persisted acknowledgement" do
    :ok = FavnRunner.TaskResultBuffer.reset()

    asset = %Favn.Manifest.Asset{
      ref: {__MODULE__.SlowAsset, :asset},
      module: __MODULE__.SlowAsset,
      name: :asset,
      type: :elixir,
      execution: %{entrypoint: :asset, arity: 1}
    }

    manifest =
      %Favn.Manifest{
        assets: [asset],
        pipelines: [],
        schedules: [],
        graph: %Favn.Manifest.Graph{nodes: [asset.ref], edges: [], topo_order: [asset.ref]}
      }
      |> FavnTestSupport.with_manifest_contract()

    {:ok, version} =
      Favn.Manifest.Version.new(manifest,
        manifest_version_id: "mv_task_executor_cancel_#{System.unique_integer([:positive])}"
      )

    work = %Favn.Contracts.RunnerWork{
      run_id: "run_task_executor_cancel",
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      required_runner_release_id: FavnTestSupport.runner_release_id(),
      runner_pool: :duckdb,
      asset_ref: asset.ref,
      asset_step_id: "step_task_executor_cancel",
      attempt: 1,
      metadata: %{}
    }

    {:ok, control_plane} =
      start_supervised({ExecutingControlPlane, owner: self(), version: version, work: work})

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         lifecycle_mode: :resident,
         exit_fun: fn _status -> :ok end}
      )

    assert_receive {:task_started, %RunnerTask.Started{} = started, ^agent}, 2_000
    assert_eventually(fn -> is_pid(:sys.get_state(agent).executor) end)
    executor = :sys.get_state(agent).executor

    :ok = ExecutingControlPlane.block_logs(control_plane)

    Enum.each(1..50, fn index ->
      assert :ok =
               GenServer.call(
                 executor,
                 {:runner_log_entry, started.task_id, %{message: "initial-#{index}"}}
               )
    end)

    assert_receive :log_delivery_blocked

    Enum.each(1..500, fn index ->
      assert :ok =
               GenServer.call(
                 executor,
                 {:runner_log_entry, started.task_id, %{message: "blocked-#{index}"}}
               )
    end)

    assert %{count: count, dropped: dropped, max_entries: max_entries} =
             FavnRunner.TaskResultBuffer.stats()

    assert count == max_entries
    assert dropped > 0
    assert {:message_queue_len, queue_len} = Process.info(agent, :message_queue_len)
    assert queue_len <= 2

    send(control_plane, :release_logs)

    send(
      agent,
      {:favn_runner_task,
       %RunnerTask.Cancellation{
         workspace_id: started.workspace_id,
         task_id: started.task_id,
         runner_instance_id: started.runner_instance_id,
         runner_session_generation: started.runner_session_generation,
         assignment_generation: started.assignment_generation,
         command_id: "cancel-real-task",
         reason: :operator_request,
         requested_at: DateTime.utc_now()
       }}
    )

    assert_receive {:cancelled_result_persisted, %RunnerTask.Result{} = result}, 2_000
    assert result.outcome == :cancelled, inspect(result)
    assert result.retry_class == :terminal
    assert result.error.outcome == :cancelled
    assert :ok = RunnerTask.Result.validate(result)
  end

  test "killing an asset executor also terminates its owned customer-code worker" do
    :ok = FavnRunner.TaskResultBuffer.reset()

    asset = %Favn.Manifest.Asset{
      ref: {__MODULE__.SlowAsset, :asset},
      module: __MODULE__.SlowAsset,
      name: :asset,
      type: :elixir,
      execution: %{entrypoint: :asset, arity: 1}
    }

    manifest =
      %Favn.Manifest{
        assets: [asset],
        pipelines: [],
        schedules: [],
        graph: %Favn.Manifest.Graph{nodes: [asset.ref], edges: [], topo_order: [asset.ref]}
      }
      |> FavnTestSupport.with_manifest_contract()

    {:ok, version} =
      Favn.Manifest.Version.new(manifest,
        manifest_version_id: "mv_asset_executor_ownership_#{System.unique_integer([:positive])}"
      )

    work = %Favn.Contracts.RunnerWork{
      run_id: "run_asset_executor_ownership",
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      required_runner_release_id: FavnTestSupport.runner_release_id(),
      runner_pool: :duckdb,
      asset_ref: asset.ref,
      asset_step_id: "step_asset_executor_ownership",
      attempt: 1,
      metadata: %{}
    }

    {:ok, control_plane} =
      start_supervised({ExecutingControlPlane, owner: self(), version: version, work: work})

    agent =
      start_supervised!(
        {RunnerAgent,
         name: nil,
         connection: control_plane,
         runner_pool: :duckdb,
         lifecycle_mode: :resident,
         exit_fun: fn _status -> :ok end}
      )

    assert_receive {:task_started, %RunnerTask.Started{}, ^agent}, 2_000
    assert_eventually(fn -> is_pid(:sys.get_state(agent).executor) end)
    executor = :sys.get_state(agent).executor
    assert_eventually(fn -> is_pid(:sys.get_state(executor).worker) end)
    worker = :sys.get_state(executor).worker

    executor_monitor = Process.monitor(executor)
    worker_monitor = Process.monitor(worker)
    Process.exit(executor, :kill)

    assert_receive {:DOWN, ^executor_monitor, :process, ^executor, :killed}
    assert_receive {:DOWN, ^worker_monitor, :process, ^worker, reason}
    assert reason in [:killed, :kill]
    refute Process.alive?(worker)
    assert_receive {:cancelled_result_persisted, %RunnerTask.Result{}}, 2_000
    assert_eventually(fn -> is_nil(:sys.get_state(agent).assignment) end)
  end

  defp assignment(runner_instance_id) do
    %RunnerTask.Assignment{
      command_id: "claim-task",
      workspace_id: "workspace-task",
      task_id: "rt_task",
      task_kind: :asset_attempt,
      runner_instance_id: runner_instance_id,
      runner_session_generation: 1,
      assignment_generation: 2,
      runner_pool: "duckdb",
      required_runner_release_id: FavnTestSupport.runner_release_id(),
      lease_expires_at: DateTime.add(DateTime.utc_now(), 30, :second),
      retry_class: :unknown_do_not_retry,
      payload: %Favn.Contracts.RunnerWork{
        manifest_version_id: "mv_task",
        manifest_content_hash: String.duplicate("a", 64),
        required_runner_release_id: FavnTestSupport.runner_release_id()
      }
    }
  end

  defp cancellation(assignment, command_id) do
    %RunnerTask.Cancellation{
      workspace_id: assignment.workspace_id,
      task_id: assignment.task_id,
      runner_instance_id: assignment.runner_instance_id,
      runner_session_generation: assignment.runner_session_generation,
      assignment_generation: assignment.assignment_generation,
      command_id: command_id,
      reason: :operator_request,
      requested_at: DateTime.utc_now()
    }
  end

  defp runtime_input_version_and_work do
    asset = %Favn.Manifest.Asset{
      ref: {__MODULE__.SlowAsset, :asset},
      module: __MODULE__.SlowAsset,
      name: :asset,
      type: :elixir,
      execution: %{entrypoint: :asset, arity: 1}
    }

    manifest =
      %Favn.Manifest{
        assets: [asset],
        pipelines: [],
        schedules: [],
        graph: %Favn.Manifest.Graph{nodes: [asset.ref], edges: [], topo_order: [asset.ref]}
      }
      |> FavnTestSupport.with_manifest_contract()

    {:ok, version} =
      Favn.Manifest.Version.new(manifest,
        manifest_version_id: "mv_runtime_input_replay_#{System.unique_integer([:positive])}"
      )

    work = %Favn.Contracts.RunnerWork{
      run_id: "run_runtime_input_replay",
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      required_runner_release_id: FavnTestSupport.runner_release_id(),
      runner_pool: :duckdb,
      asset_ref: asset.ref,
      asset_step_id: "step_runtime_input_replay",
      attempt: 1,
      metadata: %{}
    }

    {version, work}
  end

  defp assert_eventually(fun, attempts \\ 50)
  defp assert_eventually(_fun, 0), do: flunk("condition did not become true")

  defp assert_eventually(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      assert_eventually(fun, attempts - 1)
    end
  end

  defmodule SlowAsset do
    def asset(_context), do: Process.sleep(:infinity)
  end
end
