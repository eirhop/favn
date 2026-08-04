defmodule FavnRunner.RunnerAgent do
  @moduledoc """
  One-slot pull/execute/report loop for durable protocol-13 runner tasks.

  Failure results are always classified from their error envelope through
  `Favn.Contracts.RunnerTask.classify_failure/2`, so every result the agent
  delivers is one the control plane accepts. A permanent rejection is never
  retried through reconnects: the agent delivers one coherent unknown-outcome
  fallback and otherwise abandons the fenced assignment.

  Task outcomes never terminate a `:resident` agent. Lost leases, stale
  assignment resumes, and abandoned assignments are reported (or dropped when
  fenced) and the agent claims the next task. An `:elastic` agent exits in
  those situations and lets its pool replace it.
  """
  use GenServer

  require Logger

  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerTask
  alias Favn.Contracts.RunnerWork
  alias Favn.RuntimeInput.Pin
  alias FavnRunner.ControlPlaneConnection
  alias FavnRunner.ManifestStore
  alias FavnRunner.ReleaseVerifier
  alias FavnRunner.TaskExecutor
  alias FavnRunner.TaskExecutor.Result, as: ExecutorResult
  alias FavnRunner.TaskResultBuffer

  @default_max_uptime_ms 3_600_000
  @default_lease_ms 30_000
  @log_flush_ms 1_000
  @mailbox_pressure_threshold 1_000
  @supported_task_kinds Favn.Contracts.RunnerTask.task_kinds()
  @capabilities [
    "asset_execution",
    "relation_inspection",
    "generation_capabilities",
    "generation_marker_read",
    "generation_marker_initialize",
    "generation_activate",
    "generation_reconcile",
    "generation_discard"
  ]

  def start_link(opts) do
    case Keyword.get(opts, :name, __MODULE__) do
      nil -> GenServer.start_link(__MODULE__, opts)
      name -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  @doc "Stops claiming new work and exits after the current assignment is durably reported."
  @spec drain(GenServer.server()) :: :ok
  def drain(server \\ __MODULE__), do: GenServer.cast(server, :drain)

  @doc "Cancels the active executor, reports its outcome, and then exits."
  @spec cancel_and_drain(GenServer.server()) :: :ok
  def cancel_and_drain(server \\ __MODULE__),
    do: GenServer.call(server, :cancel_and_drain, 15_000)

  @doc false
  @spec drained?(GenServer.server()) :: boolean()
  def drained?(server \\ __MODULE__), do: GenServer.call(server, :drained?, 1_000)

  @impl true
  def init(opts) do
    {:ok, release} = ReleaseVerifier.verified_release()
    runner_id = Keyword.get(opts, :runner_instance_id) || random_id("runner")

    state = %{
      connection: Keyword.get(opts, :connection, ControlPlaneConnection),
      gateway: nil,
      runner_instance_id: runner_id,
      boot_id: Keyword.get(opts, :boot_id, random_id("boot")),
      runner_pool: Keyword.fetch!(opts, :runner_pool),
      required_runner_release_id: release.runner_release_id,
      lifecycle_mode: Keyword.get(opts, :lifecycle_mode, :elastic),
      exit_fun: Keyword.get(opts, :exit_fun, &System.stop/1),
      runtime_input_resolver:
        Keyword.get(opts, :runtime_input_resolver, &FavnRunner.resolve_runtime_inputs/1),
      session_generation: nil,
      assignment: nil,
      manifest_lease_id: nil,
      executor: nil,
      executor_monitor: nil,
      pending_runtime_inputs: nil,
      idle_timer: nil,
      final_claim?: false,
      lease_timer: nil,
      lease_deadline_timer: nil,
      log_timer: nil,
      log_sequence: 0,
      pending_log_batch: nil,
      control_operations: %{},
      reconnect_attempt: 0,
      resume_phase: nil,
      draining?: false,
      phase: :connecting
    }

    if state.lifecycle_mode == :elastic do
      Process.send_after(
        self(),
        :max_uptime,
        Keyword.get(opts, :max_uptime_ms, @default_max_uptime_ms)
      )
    end

    send(self(), :connect)
    {:ok, state}
  end

  @impl true
  def handle_cast(:drain, %{assignment: nil} = state) do
    state = stop_control_operation(state, :registration)
    {:stop, :normal, %{state | draining?: true, phase: :draining}}
  end

  def handle_cast(:drain, state), do: {:noreply, %{state | draining?: true}}

  @impl true
  def handle_call(:cancel_and_drain, _from, %{assignment: nil} = state) do
    state = stop_control_operation(state, :registration)
    {:stop, :normal, :ok, %{state | draining?: true, phase: :draining}}
  end

  def handle_call(:cancel_and_drain, _from, %{executor: executor} = state)
      when is_pid(executor) do
    _ = safe_cancel_executor(executor, :runner_shutdown_deadline)
    {:reply, :ok, %{state | draining?: true}}
  end

  def handle_call(:cancel_and_drain, _from, state),
    do: {:reply, :ok, %{state | draining?: true}}

  def handle_call(:drained?, _from, state) do
    drained? =
      is_nil(state.assignment) and is_nil(state.executor) and
        map_size(state.control_operations) == 0 and
        is_nil(TaskResultBuffer.pending_result())

    {:reply, drained?, state}
  end

  @impl true
  def handle_info(:connect, state) do
    agent_pid = self()

    {:noreply,
     start_control_operation(state, :registration, fn ->
       register_with_control_plane(state, agent_pid)
     end)}
  end

  def handle_info(:claim, %{assignment: nil, phase: phase} = state)
      when phase in [:idle, :waiting] do
    state = cancel_idle_timer(state)
    command_id = random_id("claim")

    request = %RunnerTask.ClaimRequest{
      command_id: command_id,
      issued_at: DateTime.utc_now(),
      runner_instance_id: state.runner_instance_id,
      runner_session_generation: state.session_generation,
      runner_pool: state.runner_pool,
      required_runner_release_id: state.required_runner_release_id,
      supported_task_kinds: @supported_task_kinds,
      capabilities: @capabilities
    }

    case request(state, request) do
      {:ok, %RunnerTask.Assignment{} = assignment} ->
        begin_assignment(%{
          state
          | assignment: assignment,
            final_claim?: false,
            phase: :preparing
        })

      {:ok, %RunnerTask.NoWork{} = no_work} ->
        wait_for_work(state, no_work)

      {:error, _reason} ->
        reconnect(%{state | phase: :connecting})
    end
  end

  def handle_info({:favn_runner_task, %RunnerTask.Wake{} = wake}, state) do
    if wake.runner_instance_id == state.runner_instance_id and
         wake.runner_session_generation == state.session_generation and
         is_nil(state.assignment) do
      send(self(), :claim)
      {:noreply, %{cancel_idle_timer(state) | final_claim?: false, phase: :idle}}
    else
      {:noreply, state}
    end
  end

  def handle_info(
        {:favn_runner_task, %RunnerTask.Cancellation{} = cancellation},
        %{assignment: %RunnerTask.Assignment{} = assignment} = state
      ) do
    if cancellation_matches?(cancellation, assignment, state) do
      cond do
        is_pid(state.executor) ->
          operation =
            fn ->
              status =
                case safe_cancel_executor(state.executor, cancellation.reason) do
                  :ok -> :observed
                  {:error, _reason} -> :rejected
                end

              ack = cancellation_ack(state, assignment, cancellation, status)
              request(state, ack)
            end

          {:noreply,
           start_control_operation(
             state,
             {:cancellation, cancellation.command_id},
             operation
           )}

        control_operation?(state, :preparation) ->
          state = stop_control_operation(state, :preparation)
          :ok = buffer_result(prestart_cancelled_result(assignment, cancellation))
          send(self(), :deliver_result)

          ack = cancellation_ack(state, assignment, cancellation, :observed)

          {:noreply,
           start_control_operation(
             %{state | phase: :reporting},
             {:cancellation, cancellation.command_id},
             fn -> request(state, ack) end
           )}

        true ->
          ack = cancellation_ack(state, assignment, cancellation, :rejected)

          {:noreply,
           start_control_operation(state, {:cancellation, cancellation.command_id}, fn ->
             request(state, ack)
           end)}
      end
    else
      {:noreply, state}
    end
  end

  def handle_info(
        {:favn_runner_task, %RunnerTask.Shutdown{} = shutdown},
        state
      ) do
    if shutdown.runner_instance_id == state.runner_instance_id and
         shutdown.runner_session_generation == state.session_generation and
         shutdown.action == :stop do
      if is_nil(state.assignment) do
        state.exit_fun.(0)
        {:stop, :normal, state}
      else
        {:noreply, %{state | draining?: true}}
      end
    else
      {:noreply, state}
    end
  end

  def handle_info(:idle_expired, %{assignment: nil, lifecycle_mode: :elastic} = state) do
    send(self(), :claim)
    {:noreply, %{state | idle_timer: nil, final_claim?: true, phase: :idle}}
  end

  # Residents poll on idle expiry instead of exiting, so a lost wake delays
  # queued work by at most one wait interval.
  def handle_info(:idle_expired, %{assignment: nil, lifecycle_mode: :resident} = state) do
    send(self(), :claim)
    {:noreply, %{state | idle_timer: nil, phase: :idle}}
  end

  def handle_info(:idle_expired, state), do: {:noreply, state}

  def handle_info(:max_uptime, %{lifecycle_mode: :elastic, assignment: nil} = state) do
    state.exit_fun.(0)
    {:stop, :normal, %{state | draining?: true, phase: :draining}}
  end

  def handle_info(:max_uptime, %{lifecycle_mode: :elastic} = state),
    do: {:noreply, %{state | draining?: true}}

  def handle_info(:max_uptime, state), do: {:noreply, state}

  def handle_info({:runner_task_logs_ready, executor}, %{executor: executor} = state) do
    emit_mailbox_pressure(state)
    send(self(), :flush_logs)
    {:noreply, state}
  end

  def handle_info(:flush_logs, %{assignment: %RunnerTask.Assignment{}} = state) do
    {:noreply, start_log_delivery(state)}
  end

  def handle_info(:flush_logs, state), do: {:noreply, state}

  def handle_info(
        {:runner_task_finished, executor, %RunnerResult{} = result},
        %{executor: executor, phase: :lease_lost} = state
      ) do
    state = clear_executor_monitor(state)
    protocol_result = lease_lost_result(state.assignment, result)
    :ok = buffer_result(protocol_result)
    send(self(), :deliver_result)
    {:noreply, %{state | executor: nil, phase: :reporting}}
  end

  def handle_info(
        {:runner_task_finished, executor, %ExecutorResult{}},
        %{executor: executor, phase: :lease_lost} = state
      ) do
    state = clear_executor_monitor(state)
    :ok = buffer_result(lease_lost_protocol_result(state.assignment))
    send(self(), :deliver_result)
    {:noreply, %{state | executor: nil, phase: :reporting}}
  end

  def handle_info(
        {:runner_task_finished, executor, %ExecutorResult{} = result},
        %{executor: executor} = state
      ) do
    state = clear_executor_monitor(state)
    :ok = buffer_result(protocol_result(state.assignment, result))
    send(self(), :deliver_result)
    {:noreply, %{state | executor: nil, phase: :reporting}}
  end

  def handle_info(
        {:runner_task_finished, executor, %RunnerResult{} = result},
        %{executor: executor} = state
      ) do
    state = clear_executor_monitor(state)
    protocol_result = protocol_result(state.assignment, result)
    :ok = buffer_result(protocol_result)
    send(self(), :deliver_result)
    {:noreply, %{state | executor: nil, phase: :reporting}}
  end

  def handle_info(
        {:DOWN, monitor, :process, executor, reason},
        %{executor: executor, executor_monitor: monitor, assignment: %RunnerTask.Assignment{}} =
          state
      ) do
    result =
      if state.phase == :lease_lost do
        lease_lost_protocol_result(state.assignment)
      else
        executor_stopped_protocol_result(state.assignment, reason)
      end

    :ok = buffer_result(result)
    send(self(), :deliver_result)
    {:noreply, %{state | executor: nil, executor_monitor: nil, phase: :reporting}}
  end

  def handle_info(:deliver_result, %{assignment: assignment} = state)
      when not is_nil(assignment) do
    cond do
      control_operation?(state, :logs) or not is_nil(state.pending_log_batch) or
          TaskResultBuffer.stats().count > 0 ->
        send(self(), :flush_logs)
        {:noreply, state}

      control_operation?(state, :result) ->
        {:noreply, state}

      match?(%RunnerTask.Result{}, TaskResultBuffer.pending_result()) ->
        result = TaskResultBuffer.pending_result()

        {:noreply,
         start_control_operation(state, :result, fn ->
           request(state, result)
         end)}

      true ->
        reconnect(state)
    end
  end

  def handle_info(:renew_lease, %{assignment: %RunnerTask.Assignment{} = assignment} = state) do
    if control_operation?(state, :renewal) do
      {:noreply, state}
    else
      occurred_at = DateTime.utc_now()
      expires_at = DateTime.add(occurred_at, lease_ms(), :millisecond)

      renewal = %RunnerTask.LeaseRenewal{
        workspace_id: assignment.workspace_id,
        task_id: assignment.task_id,
        runner_instance_id: state.runner_instance_id,
        runner_session_generation: state.session_generation,
        assignment_generation: assignment.assignment_generation,
        lease_expires_at: expires_at
      }

      {:noreply,
       start_control_operation(state, :renewal, fn ->
         {expires_at, request(state, renewal)}
       end)}
    end
  end

  def handle_info(
        {:lease_deadline, task_id, assignment_generation, expires_at},
        %{assignment: %RunnerTask.Assignment{} = assignment} = state
      ) do
    if assignment.task_id == task_id and
         assignment.assignment_generation == assignment_generation and
         assignment.lease_expires_at == expires_at and
         DateTime.compare(DateTime.utc_now(), expires_at) != :lt do
      state =
        state
        |> stop_control_operation(:preparation)
        |> Map.merge(%{
          phase: :lease_lost,
          lease_timer: nil,
          lease_deadline_timer: nil,
          draining?: drain_after_lease_loss?(state)
        })

      if is_pid(state.executor) do
        operation = fn -> safe_cancel_executor(state.executor, :runner_task_lease_lost) end
        {:noreply, start_control_operation(state, :lease_cancellation, operation)}
      else
        if is_nil(TaskResultBuffer.pending_result()) do
          :ok = buffer_result(lease_lost_protocol_result(assignment))
        end

        send(self(), :deliver_result)
        {:noreply, %{state | phase: :reporting}}
      end
    else
      {:noreply, state}
    end
  end

  def handle_info({:lease_deadline, _task_id, _assignment_generation, _expires_at}, state),
    do: {:noreply, state}

  def handle_info({:runner_agent_operation, pid, kind, result}, state) do
    case pop_control_operation(state, kind, pid) do
      {:ok, state} -> handle_control_operation_result(kind, result, state)
      :error -> {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case pop_control_operation_by_ref(state, ref) do
      {:ok, kind, state} ->
        handle_control_operation_result(kind, {:error, {:control_operation_down, reason}}, state)

      :error ->
        {:noreply, state}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp begin_assignment(state) do
    TaskResultBuffer.reset()

    state =
      state
      |> schedule_lease_renewal()
      |> start_control_operation(:preparation, fn -> prepare_assignment(state) end)

    {:noreply, state}
  end

  defp prepare_assignment(state) do
    assignment = state.assignment
    lease_id = "task:#{assignment.task_id}:#{assignment.assignment_generation}"

    with {:ok, version} <-
           safe_gateway_call(fn ->
             ControlPlaneConnection.fetch_manifest(state.gateway, assignment)
           end),
         :ok <-
           ManifestStore.register_for_release(
             version,
             assignment.required_runner_release_id
           ),
         # This closure holds a snapshot of the assignment, and a slow manifest
         # fetch can outlive the snapshot's lease horizon even while renewals
         # keep the real assignment alive on the agent. Acquire the manifest
         # lease for a fresh horizon; every later renewal keeps it in step.
         :ok <-
           ManifestStore.acquire_for_release(
             version,
             assignment.required_runner_release_id,
             lease_id,
             DateTime.add(DateTime.utc_now(), lease_ms(), :millisecond)
           ),
         {:ok, state, payload} <- prepare_payload(state, assignment, lease_id),
         {:ok, _task} <- request(state, started_message(state, assignment)) do
      {:ok, payload, lease_id, state.pending_runtime_inputs}
    else
      {:reconnect, reconnect_state} ->
        {:reconnect, reconnect_state.pending_runtime_inputs}

      {:error, reason, failed_state} ->
        {:error, reason, failed_state.pending_runtime_inputs, lease_id}

      {:error, {:control_plane_call_failed, _reason}} ->
        {:reconnect, state.pending_runtime_inputs}

      {:error, :control_plane_unavailable} ->
        {:reconnect, state.pending_runtime_inputs}

      {:error, reason} ->
        {:error, reason, state.pending_runtime_inputs, lease_id}
    end
  end

  defp start_prepared_executor(state, payload, lease_id) do
    assignment = state.assignment

    if DateTime.compare(assignment.lease_expires_at, DateTime.utc_now()) == :gt do
      case DynamicSupervisor.start_child(
             FavnRunner.TaskExecutorSupervisor,
             {TaskExecutor, assignment: assignment, payload: payload, owner: self()}
           ) do
        {:ok, executor} ->
          _ = ManifestStore.renew(lease_id, assignment.lease_expires_at)

          {:noreply,
           %{
             state
             | executor: executor,
               executor_monitor: Process.monitor(executor),
               manifest_lease_id: lease_id,
               phase: :running
           }
           |> schedule_log_flush()}

        {:error, reason} ->
          report_preparation_failure(state, assignment, lease_id, reason)
      end
    else
      :ok = buffer_result(lease_lost_protocol_result(assignment))
      send(self(), :deliver_result)

      {:noreply,
       %{
         state
         | manifest_lease_id: lease_id,
           phase: :reporting,
           draining?: drain_after_lease_loss?(state)
       }}
    end
  end

  defp prepare_payload(
         state,
         %RunnerTask.Assignment{task_kind: :asset_attempt} = assignment,
         lease_id
       ) do
    with %RunnerWork{} = work <- %{assignment.payload | manifest_lease_id: lease_id} do
      resolve_runtime_inputs(state, assignment, work)
    else
      _invalid -> {:error, :invalid_asset_attempt_payload}
    end
  end

  defp prepare_payload(state, %RunnerTask.Assignment{} = assignment, _lease_id),
    do: {:ok, state, assignment.payload}

  defp resolve_runtime_inputs(
         %{pending_runtime_inputs: %RunnerTask.RuntimeInputsResolved{} = message} = state,
         _assignment,
         work
       ) do
    persist_runtime_inputs(state, work, message)
  end

  defp resolve_runtime_inputs(state, assignment, work) do
    case state.runtime_input_resolver.(work) do
      {:ok, nil} ->
        {:ok, state, work}

      {:ok, resolution} ->
        message = %RunnerTask.RuntimeInputsResolved{
          workspace_id: assignment.workspace_id,
          task_id: assignment.task_id,
          runner_instance_id: state.runner_instance_id,
          runner_session_generation: state.session_generation,
          assignment_generation: assignment.assignment_generation,
          resolution_id: "ri:#{assignment.task_id}:#{assignment.assignment_generation}",
          issued_at: DateTime.utc_now(),
          status: :resolved,
          runtime_inputs: resolution
        }

        persist_runtime_inputs(state, work, message)

      {:error, reason} ->
        # Resolution failures that arrive as plain reasons (lease or release
        # verification, for example) happened before any SQL ran, so they are
        # safe failures; an already-normalized error keeps its own envelope.
        error =
          RunnerError.normalize(reason,
            phase: :runtime_inputs,
            retryable?: true,
            outcome: :safe_failure
          )

        message = %RunnerTask.RuntimeInputsResolved{
          workspace_id: assignment.workspace_id,
          task_id: assignment.task_id,
          runner_instance_id: state.runner_instance_id,
          runner_session_generation: state.session_generation,
          assignment_generation: assignment.assignment_generation,
          resolution_id: "ri:#{assignment.task_id}:#{assignment.assignment_generation}",
          issued_at: DateTime.utc_now(),
          status: :failed,
          error: error
        }

        persist_runtime_inputs(state, work, message)
    end
  end

  defp persist_runtime_inputs(state, work, message) do
    pending_state = %{state | pending_runtime_inputs: message}

    case request(state, message) do
      {:ok, %RunnerTask.RuntimeInputsAck{status: :persisted}} ->
        acknowledged_state = %{pending_state | pending_runtime_inputs: nil}

        case message do
          %RunnerTask.RuntimeInputsResolved{status: :resolved, runtime_inputs: resolution} ->
            pin = Pin.new(work.run_id, RunnerWork.node_key(work), resolution)
            {:ok, acknowledged_state, %{work | runtime_input_pin: pin}}

          %RunnerTask.RuntimeInputsResolved{status: :failed, error: error} ->
            {:error, error, acknowledged_state}
        end

      {:ok, %RunnerTask.RuntimeInputsAck{status: status}} ->
        {:error, {:runtime_input_resolution_rejected, status}, pending_state}

      {:error, reason} ->
        # Stale fences and permanent rejections recur on every retry, so they
        # must fail the preparation; only transport-shaped failures warrant a
        # reconnect-and-resume.
        if stale_assignment_error?(reason) or permanent_control_rejection?(reason) do
          {:error, reason, pending_state}
        else
          {:reconnect, pending_state}
        end

      _other ->
        {:reconnect, pending_state}
    end
  end

  defp report_preparation_failure(state, assignment, lease_id, reason) do
    # normalize/2 keeps an already-normalized error's own classification, so
    # the opts below only classify plain reasons; the result's outcome and
    # retry class are then derived from whatever the error actually says.
    error =
      RunnerError.normalize(reason,
        phase: :runner_task_preparation,
        retryable?: true,
        outcome: :safe_failure
      )

    {outcome, retry_class} = RunnerTask.classify_failure(assignment.task_kind, error)

    protocol_result = %RunnerTask.Result{
      workspace_id: assignment.workspace_id,
      task_id: assignment.task_id,
      task_kind: assignment.task_kind,
      runner_instance_id: state.runner_instance_id,
      runner_session_generation: state.session_generation,
      assignment_generation: assignment.assignment_generation,
      outcome: outcome,
      retry_class: retry_class,
      result: nil,
      error: error,
      finished_at: DateTime.utc_now()
    }

    TaskResultBuffer.reset()
    buffer_result(protocol_result)
    send(self(), :deliver_result)
    {:noreply, %{state | manifest_lease_id: lease_id, phase: :reporting}}
  end

  defp start_log_delivery(state) do
    cond do
      control_operation?(state, :logs) ->
        state

      batch = state.pending_log_batch ->
        start_control_operation(state, :logs, fn -> request(state, batch) end)

      true ->
        case next_log_batch(state) do
          {state, nil} ->
            if state.phase == :reporting do
              send(self(), :deliver_result)
              state
            else
              schedule_log_flush(state)
            end

          {state, batch} ->
            start_control_operation(state, :logs, fn -> request(state, batch) end)
        end
    end
  end

  defp next_log_batch(state) do
    case TaskResultBuffer.drain_logs() do
      [] ->
        {state, nil}

      entries ->
        {selected, remaining} = take_bounded_log_entries(state, entries)
        :ok = TaskResultBuffer.restore_logs(remaining)
        batch = log_batch(state, selected)
        {%{state | pending_log_batch: batch}, batch}
    end
  end

  defp take_bounded_log_entries(state, entries) do
    case largest_valid_prefix(state, entries, 1, length(entries), 0) do
      0 ->
        [_oversized | remaining] = entries
        {[%{type: :truncated, dropped_count: 1}], remaining}

      count ->
        Enum.split(entries, count)
    end
  end

  defp largest_valid_prefix(_state, _entries, lower, upper, best) when lower > upper, do: best

  defp largest_valid_prefix(state, entries, lower, upper, best) do
    count = div(lower + upper, 2)

    if RunnerTask.LogBatch.validate(log_batch(state, Enum.take(entries, count))) == :ok do
      largest_valid_prefix(state, entries, count + 1, upper, count)
    else
      largest_valid_prefix(state, entries, lower, count - 1, best)
    end
  end

  defp log_batch(state, entries) do
    assignment = state.assignment
    sequence = state.log_sequence

    %RunnerTask.LogBatch{
      workspace_id: assignment.workspace_id,
      task_id: assignment.task_id,
      runner_instance_id: state.runner_instance_id,
      runner_session_generation: state.session_generation,
      assignment_generation: assignment.assignment_generation,
      batch_id: "logs:#{assignment.task_id}:#{assignment.assignment_generation}:#{sequence}",
      issued_at: DateTime.utc_now(),
      sequence: sequence,
      entries: entries,
      truncated?: Enum.any?(entries, &match?(%{type: :truncated}, &1))
    }
  end

  defp handle_control_operation_result(
         :registration,
         {:ok, gateway, %RunnerTask.RegistrationAck{status: :accepted} = acknowledgement},
         state
       ) do
    state = %{
      state
      | gateway: gateway,
        session_generation: acknowledgement.runner_session_generation,
        reconnect_attempt: 0
    }

    resume_after_registration(state)
  end

  defp handle_control_operation_result(
         :registration,
         {:ok, _gateway, %RunnerTask.RegistrationAck{status: :rejected, reason: reason}},
         state
       ),
       do: stop_rejected_registration(state, reason)

  defp handle_control_operation_result(:registration, _error, state), do: reconnect(state)

  defp handle_control_operation_result(
         :preparation,
         {:ok, payload, lease_id, pending_runtime_inputs},
         state
       ) do
    start_prepared_executor(
      %{state | pending_runtime_inputs: pending_runtime_inputs},
      payload,
      lease_id
    )
  end

  defp handle_control_operation_result(:preparation, {:reconnect, pending}, state),
    do: reconnect(%{state | pending_runtime_inputs: pending, phase: :preparing})

  defp handle_control_operation_result(
         :preparation,
         {:error, reason, pending, lease_id},
         state
       ),
       do:
         report_preparation_failure(
           %{state | pending_runtime_inputs: pending},
           state.assignment,
           lease_id,
           reason
         )

  defp handle_control_operation_result(:preparation, {:error, reason}, state),
    do: report_preparation_failure(state, state.assignment, nil, reason)

  defp handle_control_operation_result(:renewal, {expires_at, {:ok, _task}}, state) do
    assignment = %{state.assignment | lease_expires_at: expires_at}
    if state.manifest_lease_id, do: _ = ManifestStore.renew(state.manifest_lease_id, expires_at)
    {:noreply, schedule_lease_renewal(%{state | assignment: assignment})}
  end

  defp handle_control_operation_result(:renewal, {_expires_at, _error}, state),
    do: reconnect_after_lease_failure(state, state.assignment)

  defp handle_control_operation_result(:renewal, _error, state),
    do: reconnect_after_lease_failure(state, state.assignment)

  defp handle_control_operation_result(:logs, {:ok, %RunnerTask.LogAck{}}, state) do
    state = %{state | log_sequence: state.log_sequence + 1, pending_log_batch: nil}
    send(self(), :flush_logs)
    {:noreply, state}
  end

  defp handle_control_operation_result(:logs, _error, state), do: reconnect(state)

  defp handle_control_operation_result(
         :result,
         {:ok, %RunnerTask.ResultAck{status: :persisted}},
         state
       ) do
    :ok = TaskResultBuffer.acknowledge_result()
    if state.manifest_lease_id, do: :ok = ManifestStore.release(state.manifest_lease_id)
    state = clear_assignment(state)

    if state.draining? do
      state.exit_fun.(0)
      {:stop, :normal, state}
    else
      send(self(), :claim)
      {:noreply, state}
    end
  end

  defp handle_control_operation_result(:result, {:error, reason}, state) do
    cond do
      stale_assignment_error?(reason) -> abandon_stale_assignment(state)
      permanent_control_rejection?(reason) -> handle_rejected_result(state, reason)
      pending_result_invalid?() -> handle_rejected_result(state, reason)
      true -> reconnect(state)
    end
  end

  defp handle_control_operation_result(
         :result,
         {:ok, %RunnerTask.ResultAck{status: :stale}},
         state
       ),
       do: abandon_stale_assignment(state)

  defp handle_control_operation_result(
         :result,
         {:ok, %RunnerTask.ResultAck{status: :rejected} = ack},
         state
       ),
       do: handle_rejected_result(state, {:result_ack_rejected, ack.task_id})

  defp handle_control_operation_result(:result, _error, state), do: reconnect(state)

  defp handle_control_operation_result({:cancellation, _command_id}, _result, state),
    do: {:noreply, state}

  defp handle_control_operation_result(kind, _result, state)
       when kind in [:lease_cancellation],
       do: {:noreply, state}

  # A permanent rejection will recur on every delivery attempt, so retrying
  # through reconnects can only wedge the assignment until its lease expires.
  # Replace the rejected result with a coherent unknown-outcome report once;
  # if even that is rejected, abandon the assignment and let the control
  # plane's lease expiry own the task's fate.
  defp handle_rejected_result(state, reason) do
    case TaskResultBuffer.pending_result() do
      %RunnerTask.Result{error: %RunnerError{type: :runner_task_result_rejected}} ->
        Logger.error(
          "runner task fallback result was rejected as well; abandoning the assignment",
          reason: reason
        )

        abandon_stale_assignment(state)

      %RunnerTask.Result{} = rejected ->
        Logger.error(
          "runner task result rejected as invalid; delivering unknown-outcome fallback",
          reason: reason
        )

        error =
          RunnerError.new(
            type: :runner_task_result_rejected,
            phase: :runner_task_reporting,
            message: "Control plane rejected the runner task result as invalid",
            details: %{rejection: inspect(reason, limit: 20, printable_limit: 1_024)},
            retryable?: false,
            outcome: :unknown
          )

        {outcome, retry_class} = RunnerTask.classify_failure(rejected.task_kind, error)

        fallback = %{
          rejected
          | outcome: outcome,
            retry_class: retry_class,
            result: nil,
            error: error,
            finished_at: DateTime.utc_now()
        }

        :ok = TaskResultBuffer.acknowledge_result()
        :ok = buffer_result(fallback)
        send(self(), :deliver_result)
        {:noreply, state}

      nil ->
        abandon_stale_assignment(state)
    end
  end

  # If the runner's own contract validation rejects the pending result, the
  # control plane will reject it on every attempt as well.
  defp pending_result_invalid? do
    case TaskResultBuffer.pending_result() do
      %RunnerTask.Result{} = result -> RunnerTask.Result.validate(result) != :ok
      _other -> false
    end
  end

  defp permanent_control_rejection?(%{kind: kind}) when kind in [:invalid, :conflict], do: true

  defp permanent_control_rejection?(
         {:invalid_runner_task_retry_classification, _kind, _outcome, _retry_class, _error}
       ),
       do: true

  defp permanent_control_rejection?({:invalid_runner_task_result, _kind, _outcome, _result}),
    do: true

  defp permanent_control_rejection?({:invalid_runner_task_error, _outcome, _error}), do: true
  defp permanent_control_rejection?({:invalid_runner_error, _error}), do: true
  defp permanent_control_rejection?({:invalid_runner_task_message, _tag, _field}), do: true

  # Runtime-input resolution messages are validated by the control plane, and
  # a validation failure recurs verbatim on every resend. Reconnecting would
  # wedge the assignment: renewals keep the lease alive, so recovery never
  # intervenes either.
  defp permanent_control_rejection?({:invalid_runtime_inputs_resolved, _status}), do: true
  defp permanent_control_rejection?(:invalid_runtime_input_payload_fingerprint), do: true

  defp permanent_control_rejection?(_reason), do: false

  # Last gate before a result is buffered for delivery. An incoherent
  # outcome/retry-class pair would be rejected by the control plane on every
  # delivery attempt, wedging the assignment, so reclassify it from the error
  # envelope itself and report loudly instead of shipping it.
  defp buffer_result(%RunnerTask.Result{} = message) do
    message =
      case RunnerTask.Result.validate(message) do
        :ok ->
          message

        {:error, _reason} = invalid ->
          coerced = coerce_result_classification(message)

          Logger.error(
            "runner task result was incoherent and has been reclassified " <>
              "(task_id=#{message.task_id} outcome=#{inspect(message.outcome)} " <>
              "retry_class=#{inspect(message.retry_class)} -> " <>
              "outcome=#{inspect(coerced.outcome)} retry_class=#{inspect(coerced.retry_class)})",
            reason: invalid
          )

          coerced
      end

    TaskResultBuffer.put_result(message)
  end

  defp coerce_result_classification(%RunnerTask.Result{error: %RunnerError{} = error} = message) do
    {outcome, retry_class} = RunnerTask.classify_failure(message.task_kind, error)
    %{message | outcome: outcome, retry_class: retry_class}
  end

  defp coerce_result_classification(%RunnerTask.Result{} = message), do: message

  defp protocol_result(assignment, %RunnerResult{status: :ok} = result) do
    result_message(assignment, result, :succeeded, :terminal, nil)
  end

  defp protocol_result(assignment, %ExecutorResult{} = result) do
    result_message(
      assignment,
      result.result,
      result.outcome,
      result.retry_class,
      result.error
    )
  end

  defp protocol_result(assignment, %RunnerResult{status: :cancelled, error: nil} = result) do
    result_message(assignment, result, :cancelled, :terminal, nil)
  end

  defp protocol_result(assignment, %RunnerResult{error: error} = result) do
    error = error || RunnerError.new(outcome: :unknown, retryable?: false)
    {outcome, retry_class} = RunnerTask.classify_failure(assignment.task_kind, error)
    result_message(assignment, result, outcome, retry_class, error)
  end

  defp lease_lost_result(assignment, %RunnerResult{} = result) do
    error =
      RunnerError.new(
        type: :runner_task_lease_lost,
        phase: :runner_task_execution,
        message: "Runner task lease expired before renewal was acknowledged",
        retryable?: false,
        outcome: :unknown
      )

    result =
      %{result | status: :error, asset_results: [], error: error}

    result_message(assignment, result, :unknown, :unknown_do_not_retry, error)
  end

  defp result_message(assignment, result, outcome, retry_class, error) do
    %RunnerTask.Result{
      workspace_id: assignment.workspace_id,
      task_id: assignment.task_id,
      task_kind: assignment.task_kind,
      runner_instance_id: assignment.runner_instance_id,
      runner_session_generation: assignment.runner_session_generation,
      assignment_generation: assignment.assignment_generation,
      outcome: outcome,
      retry_class: retry_class,
      result: result,
      error: error,
      finished_at: DateTime.utc_now()
    }
  end

  defp started_message(state, assignment) do
    %RunnerTask.Started{
      workspace_id: assignment.workspace_id,
      task_id: assignment.task_id,
      runner_instance_id: state.runner_instance_id,
      runner_session_generation: state.session_generation,
      assignment_generation: assignment.assignment_generation,
      issued_at: assignment.assigned_at,
      occurred_at: DateTime.utc_now()
    }
  end

  defp request(state, message),
    do:
      safe_gateway_call(fn ->
        ControlPlaneConnection.request(state.gateway, message)
      end)

  defp registration(state) do
    %RunnerTask.Registration{
      runner_instance_id: state.runner_instance_id,
      boot_id: state.boot_id,
      runner_session_generation: state.session_generation || 0,
      beam_node: Atom.to_string(node()),
      runner_pool: state.runner_pool,
      required_runner_release_id: state.required_runner_release_id,
      lifecycle_mode: state.lifecycle_mode,
      supported_task_kinds: @supported_task_kinds,
      capabilities: @capabilities,
      active_assignment: active_assignment(state)
    }
  end

  defp register_with_control_plane(state, agent_pid) do
    with {:ok, gateway} <-
           safe_gateway_call(fn -> ControlPlaneConnection.gateway(state.connection) end),
         registration = registration(state),
         {:ok, %RunnerTask.RegistrationAck{} = acknowledgement} <-
           safe_gateway_call(fn ->
             ControlPlaneConnection.register(gateway, registration, agent_pid)
           end) do
      {:ok, gateway, acknowledgement}
    end
  end

  defp safe_gateway_call(fun) do
    fun.()
  catch
    :exit, reason -> {:error, {:control_plane_call_failed, reason}}
  end

  defp wait_for_work(state, %RunnerTask.NoWork{action: :stop}) do
    state.exit_fun.(0)
    {:stop, :normal, state}
  end

  defp wait_for_work(%{final_claim?: true} = state, %RunnerTask.NoWork{}) do
    state.exit_fun.(0)
    {:stop, :normal, state}
  end

  defp wait_for_work(state, %RunnerTask.NoWork{wait_ms: wait_ms}) do
    timer = Process.send_after(self(), :idle_expired, wait_ms)
    {:noreply, %{state | phase: :waiting, idle_timer: timer}}
  end

  defp reconnect(state) do
    ControlPlaneConnection.reconnect(state.connection)
    delay = reconnect_delay(state.reconnect_attempt)
    Process.send_after(self(), :connect, delay)

    resume_phase =
      cond do
        is_nil(state.assignment) -> nil
        state.phase == :connecting -> state.resume_phase || infer_resume_phase(state)
        true -> state.phase
      end

    {:noreply,
     %{
       state
       | gateway: nil,
         phase: :connecting,
         resume_phase: resume_phase,
         reconnect_attempt: min(state.reconnect_attempt + 1, 8)
     }}
  end

  defp reconnect_delay(attempt) do
    ceiling = min(250 * round(:math.pow(2, attempt)), 30_000)
    max(1, div(ceiling, 2) + :rand.uniform(max(div(ceiling, 2), 1)))
  end

  # A stale-resume rejection means the control plane has already fenced the
  # assignment this runner was trying to resume: the runner holds nothing it
  # is allowed to report. A resident runner drops the fenced assignment and
  # registers fresh so one lost task never costs the pool its only runner; an
  # elastic runner exits and lets its pool replace it.
  defp stop_rejected_registration(
         %{lifecycle_mode: :resident, draining?: false} = state,
         :stale_runner_task_resume = reason
       ) do
    Logger.warning(
      "runner registration resume rejected as stale; dropping the fenced assignment",
      reason: reason
    )

    state = drop_fenced_assignment(state)
    send(self(), :connect)
    {:noreply, %{state | phase: :connecting}}
  end

  defp stop_rejected_registration(state, reason) do
    Logger.error("runner registration rejected: #{inspect(reason)}", reason: reason)

    if is_pid(state.executor) do
      _ = safe_cancel_executor(state.executor, :stale_runner_task_resume)
    end

    if state.manifest_lease_id, do: ManifestStore.release(state.manifest_lease_id)
    state.exit_fun.(1)
    {:stop, :normal, %{state | draining?: true, phase: :draining}}
  end

  # Tears down everything belonging to an assignment the control plane has
  # fenced away. The pending result (if any) can never be accepted for a
  # fenced assignment, so it is dropped rather than delivered. A manifest
  # lease acquired inside a preparation op that is killed here never reached
  # state.manifest_lease_id; it self-expires at its horizon.
  defp drop_fenced_assignment(state) do
    state =
      state
      |> stop_control_operation(:preparation)
      |> stop_control_operation(:lease_cancellation)

    if is_pid(state.executor) do
      _ = safe_cancel_executor(state.executor, :stale_runner_task_resume)
      _ = DynamicSupervisor.terminate_child(FavnRunner.TaskExecutorSupervisor, state.executor)
    end

    state = clear_executor_monitor(state)
    TaskResultBuffer.reset()
    if state.manifest_lease_id, do: ManifestStore.release(state.manifest_lease_id)
    clear_assignment(state)
  end

  defp safe_cancel_executor(executor, reason) do
    TaskExecutor.cancel(executor, reason)
  catch
    :exit, _reason -> {:error, :executor_unavailable}
  end

  defp schedule_lease_renewal(state) do
    if state.lease_timer, do: Process.cancel_timer(state.lease_timer)
    if state.lease_deadline_timer, do: Process.cancel_timer(state.lease_deadline_timer)

    assignment = state.assignment

    remaining_ms =
      max(DateTime.diff(assignment.lease_expires_at, DateTime.utc_now(), :millisecond), 0)

    renew_after_ms = max(min(div(lease_ms(), 3), div(max(remaining_ms, 1), 3)), 1)

    %{
      state
      | lease_timer: Process.send_after(self(), :renew_lease, renew_after_ms),
        lease_deadline_timer:
          Process.send_after(
            self(),
            {:lease_deadline, assignment.task_id, assignment.assignment_generation,
             assignment.lease_expires_at},
            remaining_ms
          )
    }
  end

  defp reconnect_after_lease_failure(state, assignment) do
    remaining_ms =
      max(DateTime.diff(assignment.lease_expires_at, DateTime.utc_now(), :millisecond), 0)

    if remaining_ms > 0 do
      delay = min(max(div(remaining_ms, 3), 50), 1_000)
      state = %{state | lease_timer: Process.send_after(self(), :renew_lease, delay)}
      reconnect(state)
    else
      state = %{
        state
        | phase: :lease_lost,
          lease_timer: nil,
          draining?: drain_after_lease_loss?(state)
      }

      if is_pid(state.executor) do
        _ = TaskExecutor.cancel(state.executor, :runner_task_lease_lost)
        {:noreply, state}
      else
        if is_nil(TaskResultBuffer.pending_result()) do
          :ok = buffer_result(lease_lost_protocol_result(assignment))
        end

        send(self(), :deliver_result)
        {:noreply, %{state | phase: :reporting}}
      end
    end
  end

  # Losing one lease ends an elastic runner's useful life; its pool replaces
  # it. A resident runner reports the loss and keeps serving the pool.
  defp drain_after_lease_loss?(state),
    do: state.lifecycle_mode == :elastic or state.draining?

  defp resume_after_registration(%{assignment: nil} = state) do
    send(self(), :claim)
    {:noreply, %{state | phase: :idle, resume_phase: nil}}
  end

  defp resume_after_registration(%{phase: phase} = state)
       when phase in [:reporting, :lease_lost] do
    if TaskResultBuffer.pending_result(), do: send(self(), :deliver_result)
    {:noreply, %{state | resume_phase: nil}}
  end

  defp resume_after_registration(state) do
    phase = state.resume_phase || infer_resume_phase(state)
    state = %{state | phase: phase, resume_phase: nil}

    case phase do
      :preparing ->
        begin_assignment(state)

      phase when phase in [:reporting, :lease_lost] ->
        send(self(), :deliver_result)
        {:noreply, state}

      :running ->
        send(self(), :renew_lease)
        send(self(), :flush_logs)
        {:noreply, state}
    end
  end

  defp infer_resume_phase(state) do
    cond do
      not is_nil(TaskResultBuffer.pending_result()) -> :reporting
      is_pid(state.executor) -> :running
      true -> :preparing
    end
  end

  defp stale_assignment_error?(%{kind: kind}) when kind in [:fenced, :not_found], do: true

  defp stale_assignment_error?({:stale_runner_task_assignment, _details}), do: true
  defp stale_assignment_error?(:stale_runner_task_assignment), do: true
  defp stale_assignment_error?(_reason), do: false

  defp abandon_stale_assignment(%{lifecycle_mode: :resident, draining?: false} = state) do
    :ok = TaskResultBuffer.acknowledge_result()
    if state.manifest_lease_id, do: ManifestStore.release(state.manifest_lease_id)
    state = clear_assignment(state)
    send(self(), :claim)
    {:noreply, state}
  end

  defp abandon_stale_assignment(state) do
    :ok = TaskResultBuffer.acknowledge_result()
    if state.manifest_lease_id, do: ManifestStore.release(state.manifest_lease_id)
    state.exit_fun.(0)
    {:stop, :normal, %{state | draining?: true, phase: :draining}}
  end

  defp lease_lost_runner_result(%RunnerWork{} = work) do
    %RunnerResult{
      run_id: work.run_id,
      manifest_version_id: work.manifest_version_id,
      manifest_content_hash: work.manifest_content_hash,
      required_runner_release_id: work.required_runner_release_id,
      status: :error,
      asset_results: [],
      error: RunnerError.new(outcome: :unknown, retryable?: false),
      metadata: RunnerWork.lifecycle_metadata(work)
    }
  end

  defp lease_lost_protocol_result(%RunnerTask.Assignment{task_kind: :asset_attempt} = assignment) do
    result = lease_lost_runner_result(assignment.payload)
    lease_lost_result(assignment, result)
  end

  defp lease_lost_protocol_result(%RunnerTask.Assignment{} = assignment) do
    retry_class = RunnerTask.default_retry_class(assignment.task_kind)

    {outcome, error_outcome, retryable?} =
      case retry_class do
        :safe_to_retry -> {:failed, :safe_failure, true}
        :reconcile_before_retry -> {:unknown, :unknown, false}
      end

    error =
      RunnerError.new(
        type: :runner_task_lease_lost,
        phase: :runner_task_execution,
        message: "Runner task lease expired before renewal was acknowledged",
        retryable?: retryable?,
        outcome: error_outcome
      )

    result_message(assignment, nil, outcome, retry_class, error)
  end

  defp executor_stopped_protocol_result(%RunnerTask.Assignment{} = assignment, reason) do
    retry_class = RunnerTask.default_retry_class(assignment.task_kind)

    {outcome, error_outcome, retryable?} =
      case retry_class do
        :safe_to_retry -> {:failed, :safe_failure, true}
        :reconcile_before_retry -> {:unknown, :unknown, false}
        :unknown_do_not_retry -> {:unknown, :unknown, false}
      end

    error =
      RunnerError.normalize({:runner_task_executor_stopped, reason},
        type: :runner_task_executor_stopped,
        phase: :runner_task_execution,
        retryable?: retryable?,
        outcome: error_outcome
      )

    result_message(assignment, nil, outcome, retry_class, error)
  end

  defp schedule_log_flush(state) do
    if state.log_timer, do: Process.cancel_timer(state.log_timer)
    %{state | log_timer: Process.send_after(self(), :flush_logs, @log_flush_ms)}
  end

  defp cancel_idle_timer(state) do
    if state.idle_timer, do: Process.cancel_timer(state.idle_timer)
    %{state | idle_timer: nil}
  end

  defp clear_assignment(state) do
    if state.lease_timer, do: Process.cancel_timer(state.lease_timer)
    if state.lease_deadline_timer, do: Process.cancel_timer(state.lease_deadline_timer)
    if state.log_timer, do: Process.cancel_timer(state.log_timer)

    state =
      state
      |> stop_control_operation(:renewal)
      |> stop_control_operation(:logs)
      |> stop_control_operation(:result)

    %{
      state
      | assignment: nil,
        manifest_lease_id: nil,
        executor: nil,
        executor_monitor: nil,
        pending_runtime_inputs: nil,
        lease_timer: nil,
        lease_deadline_timer: nil,
        log_timer: nil,
        log_sequence: 0,
        pending_log_batch: nil,
        reconnect_attempt: 0,
        resume_phase: nil,
        final_claim?: false,
        phase: :idle
    }
  end

  defp clear_executor_monitor(%{executor_monitor: monitor} = state)
       when is_reference(monitor) do
    Process.demonitor(monitor, [:flush])
    %{state | executor_monitor: nil}
  end

  defp clear_executor_monitor(state), do: state

  defp active_assignment(%{assignment: nil}), do: nil

  defp active_assignment(%{assignment: assignment}),
    do: %{
      workspace_id: assignment.workspace_id,
      task_id: assignment.task_id,
      assignment_generation: assignment.assignment_generation
    }

  defp cancellation_matches?(cancellation, assignment, state) do
    cancellation.workspace_id == assignment.workspace_id and
      cancellation.task_id == assignment.task_id and
      cancellation.runner_instance_id == state.runner_instance_id and
      cancellation.runner_session_generation == state.session_generation and
      cancellation.assignment_generation == assignment.assignment_generation
  end

  defp lease_ms, do: Application.get_env(:favn_runner, :runner_task_lease_ms, @default_lease_ms)

  defp cancellation_ack(state, assignment, cancellation, status) do
    %RunnerTask.CancellationAck{
      workspace_id: assignment.workspace_id,
      task_id: assignment.task_id,
      runner_instance_id: state.runner_instance_id,
      runner_session_generation: state.session_generation,
      assignment_generation: assignment.assignment_generation,
      command_id: cancellation.command_id,
      status: status,
      issued_at: cancellation.requested_at,
      acknowledged_at: DateTime.utc_now()
    }
  end

  defp prestart_cancelled_result(assignment, cancellation) do
    result_message(
      assignment,
      nil,
      :cancelled,
      :terminal,
      RunnerError.cancelled(cancellation.reason)
    )
  end

  defp start_control_operation(state, kind, fun) do
    if control_operation?(state, kind) do
      state
    else
      owner = self()

      {pid, ref} =
        spawn_monitor(fn ->
          result =
            try do
              fun.()
            rescue
              exception -> {:error, {:control_operation_exception, exception}}
            catch
              caught_kind, reason -> {:error, {:control_operation_caught, caught_kind, reason}}
            end

          send(owner, {:runner_agent_operation, self(), kind, result})
        end)

      put_in(state.control_operations[kind], %{pid: pid, ref: ref})
    end
  end

  defp control_operation?(state, kind), do: Map.has_key?(state.control_operations, kind)

  defp stop_control_operation(state, kind) do
    case Map.pop(state.control_operations, kind) do
      {nil, _operations} ->
        state

      {%{pid: pid, ref: ref}, operations} ->
        Process.demonitor(ref, [:flush])
        if Process.alive?(pid), do: Process.exit(pid, :kill)
        %{state | control_operations: operations}
    end
  end

  defp pop_control_operation(state, kind, pid) do
    case Map.get(state.control_operations, kind) do
      %{pid: ^pid, ref: ref} ->
        Process.demonitor(ref, [:flush])
        {:ok, %{state | control_operations: Map.delete(state.control_operations, kind)}}

      _other ->
        :error
    end
  end

  defp pop_control_operation_by_ref(state, ref) do
    case Enum.find(state.control_operations, fn {_kind, operation} -> operation.ref == ref end) do
      {kind, _operation} ->
        {:ok, kind, %{state | control_operations: Map.delete(state.control_operations, kind)}}

      nil ->
        :error
    end
  end

  defp emit_mailbox_pressure(state) do
    threshold =
      Application.get_env(
        :favn_runner,
        :runner_task_mailbox_pressure_threshold,
        @mailbox_pressure_threshold
      )

    case Process.info(self(), :message_queue_len) do
      {:message_queue_len, length} when length >= threshold ->
        :telemetry.execute(
          [:favn, :runner, :task_mailbox, :pressure],
          %{message_queue_len: length},
          %{
            runner_instance_id: state.runner_instance_id,
            task_id: state.assignment && state.assignment.task_id
          }
        )

      _other ->
        :ok
    end
  end

  defp random_id(prefix),
    do: prefix <> "_" <> Base.url_encode64(:crypto.strong_rand_bytes(18), padding: false)
end
