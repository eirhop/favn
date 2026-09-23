defmodule FavnOrchestrator.RunServer do
  @moduledoc """
  Process owner for one manifest-pinned orchestrator run.

  The server advances the non-blocking execution state machine from runner,
  retry, admission, cancellation, and post-step worker messages. Terminal
  persistence is retried in-process so transient storage failures do not
  discard the final outcome. A write rejected by the run-ownership fence on the
  run-start, step, or terminal path is never retried: the process stops with
  `run_ownership_lost` and recovery proceeds under the newer owner. Admission
  and bookkeeping preserve their original commands for safe persistence replay;
  a process exit leaves durable tasks for the ownership recovery sweep.
  """

  use GenServer

  alias Favn.Manifest.Version
  alias FavnOrchestrator.OperationalEvents
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.Persistence.Results.RunOwnership, as: Ownership
  alias FavnOrchestrator.RunExecutionCleanup
  alias FavnOrchestrator.RunManager
  alias FavnOrchestrator.RunServer.Execution
  alias FavnOrchestrator.RunServer.FailureCleanup
  alias FavnOrchestrator.RunServer.Execution.RunExecutionState
  alias FavnOrchestrator.RunServer.Persistence
  alias FavnOrchestrator.RunServer.PersistenceRetry
  alias FavnOrchestrator.RunServer.Snapshots
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Runs

  @type init_arg :: %{
          required(:run_state) => RunState.t(),
          required(:version) => Version.t(),
          optional(:recovering?) => boolean(),
          optional(:capacity_managed?) => boolean(),
          required(:storage_ownership) => Ownership.t(),
          required(:lease_keeper) => pid()
        }

  @terminal_persist_retry_ms 1_000
  @execution_persist_retry_ms 1_000

  @doc "Starts an unregistered process for one run snapshot and manifest version."
  @spec start_link(init_arg()) :: GenServer.on_start()
  def start_link(args) when is_map(args), do: GenServer.start_link(__MODULE__, args)

  @impl true
  def init(%{run_state: %RunState{}, version: %Version{}, lease_keeper: keeper} = args)
      when is_pid(keeper) do
    Process.put(:favn_managed_run, true)

    {:ok, Map.put(args, :awaiting_activation, true)}
  end

  def init(_args), do: {:stop, :run_lifecycle_required}

  @impl true
  def handle_continue(:execute, %{run_state: run_state, version: version} = state) do
    cond do
      FailureCleanup.pending?(run_state) ->
        with {:ok, state, owned_run} <- bind_storage_ownership(state, run_state) do
          start_failure_cleanup(state, owned_run, version)
        else
          {:error, reason} -> {:stop, {:shutdown, {:run_ownership_unavailable, reason}}, state}
        end

      RunState.finalized?(run_state) ->
        :ok = Execution.release_manifest_lease(run_state)
        :ok = RunExecutionCleanup.release_admission(run_state)

        {:stop, :normal,
         state |> Map.put(:run_state, run_state) |> Map.put(:execution_state, nil)}

      true ->
        with {:ok, state, owned_run} <- bind_storage_ownership(state, run_state) do
          continue_start(state, owned_run, version)
        else
          {:error, reason} -> {:stop, {:shutdown, {:run_ownership_unavailable, reason}}, state}
        end
    end
  end

  defp continue_start(%{recovering?: true} = state, %RunState{} = claimed, version) do
    context = SystemContext.workspace(claimed.workspace_id, :run_worker)

    case Runs.get(context, claimed.id) do
      {:ok, saved} ->
        run = copy_storage_fence(saved, claimed)
        state = %{state | run_state: run}

        cond do
          FailureCleanup.pending?(run) ->
            start_failure_cleanup(state, run, version)

          RunState.finalized?(run) ->
            stop_normally(state, run)

          run.status == :pending ->
            persist_run_start(state, RunState.transition(run, status: :running), version)

          true ->
            start_execution(state, run, version)
        end

      {:error, reason} ->
        {:stop, {:shutdown, {:run_recovery_snapshot_unavailable, reason}}, state}
    end
  end

  defp continue_start(state, %RunState{} = run_state, %Version{} = version) do
    running = RunState.transition(run_state, status: :running)
    persist_run_start(state, running, version)
  end

  @impl true
  def handle_info(:activate, %{awaiting_activation: true, lease_keeper: keeper} = state) do
    case FavnOrchestrator.RunLeaseKeeper.transfer(keeper, self()) do
      :ok ->
        {:noreply,
         state |> Map.delete(:awaiting_activation) |> Map.put(:awaiting_lease_ready, true)}

      error ->
        {:stop, error, state}
    end
  end

  def handle_info(
        {:lease_challenge, keeper, generation, challenge},
        %{lease_keeper: keeper, storage_ownership: %{fencing_token: generation}} = state
      ) do
    send(keeper, {:lease_response, self(), generation, challenge})

    if Map.get(state, :awaiting_lease_ready, false) do
      send(RunManager, {:coordinator_ready, self(), state.storage_ownership})
      send(self(), :await_initial_permit)
      {:noreply, Map.delete(state, :awaiting_lease_ready)}
    else
      {:noreply, state}
    end
  end

  def handle_info(:await_initial_permit, state) do
    case FavnOrchestrator.RunLeaseKeeper.ready(state.run_state) do
      :ok ->
        {:noreply, state, {:continue, :execute}}

      {:error, _} ->
        Process.send_after(self(), :await_initial_permit, 100)
        {:noreply, state}
    end
  end

  def handle_info(:continue_failure_cleanup, %{failure_cleanup: cleanup} = state) do
    operation = FailureCleanup.operation(cleanup)

    case account_helper_memory(state, cleanup.run, {cleanup, operation, operation}) do
      :ok ->
        task =
          FavnOrchestrator.RunHelper.async(cleanup.run, fn ->
            FailureCleanup.perform(operation)
          end)

        {:noreply, Map.put(state, :cleanup_operation, %{ref: task.ref, operation: operation})}

      {:error, reason} ->
        {:stop, {:shutdown, {:cleanup_memory_limit, reason}}, state}
    end
  end

  def handle_info(
        {ref, result},
        %{cleanup_operation: %{ref: ref, operation: operation}, failure_cleanup: cleanup} = state
      ) do
    Process.demonitor(ref, [:flush])
    state = Map.delete(state, :cleanup_operation)

    case FailureCleanup.apply_result(cleanup, operation, result) do
      {:cont, next} ->
        send(self(), :continue_failure_cleanup)
        {:noreply, %{state | failure_cleanup: next, run_state: next.run}}

      {:done, next} ->
        stop_normally(state, next.run)

      {kind, reason} when kind in [:retry, :error] ->
        OperationalEvents.emit(
          :run_cleanup_deferred,
          %{},
          %{run_id: cleanup.run.id, reason_code: "cleanup_reconciliation_pending"},
          level: :warning
        )

        {:stop, {:shutdown, {:cleanup_deferred, reason}}, state}
    end
  end

  def handle_info(
        {:DOWN, ref, :process, _pid, reason},
        %{cleanup_operation: %{ref: ref}} = state
      ),
      do: {:stop, {:shutdown, {:cleanup_worker_lost, reason}}, state}

  def handle_info({ref, result}, %{failure_operation: %{ref: ref}} = state) do
    Process.demonitor(ref, [:flush])

    case result do
      {:ok, failed} -> stop_normally(state, failed)
      {:error, reason} -> {:stop, {:shutdown, {:failure_persistence_unconfirmed, reason}}, state}
    end
  end

  def handle_info(
        {:DOWN, ref, :process, _pid, reason},
        %{failure_operation: %{ref: ref}} = state
      ),
      do: {:stop, {:shutdown, {:failure_persistence_unconfirmed, reason}}, state}

  def handle_info({:favn_run_cancel_requested, _reason}, %{failure_cleanup: _} = state),
    do: {:noreply, state}

  def handle_info(:drain_deferred_execution, %{execution_persist_pending: _} = state),
    do: {:noreply, Map.delete(state, :deferred_drain_scheduled)}

  def handle_info(:drain_deferred_execution, state) do
    state = Map.delete(state, :deferred_drain_scheduled)

    case Map.get(state, :deferred_execution_events, []) do
      [] ->
        {:noreply, state}

      [message | rest] ->
        next = Map.put(state, :deferred_execution_events, rest)

        case handle_info(message, next) do
          {:noreply, next} -> {:noreply, replay_deferred_execution_events(next)}
          other -> other
        end
    end
  end

  # Receipts precede the deferral clauses: they release the sequence mutation gate.
  def handle_info(
        {ref, result},
        %{
          execution_persist_pending: %{
            ref: ref,
            operation: operation,
            base_sequence: sequence,
            generation: generation
          },
          execution_state: execution
        } = state
      ) do
    Process.demonitor(ref, [:flush])

    if execution.run.event_seq == sequence and execution.run.storage_fencing_token == generation do
      state = Map.delete(state, :execution_persist_pending)

      execution =
        case Map.get(state, :cancel_latched) do
          {:favn_run_cancel_requested, reason} -> %{execution | cancel_requested: reason}
          nil -> execution
        end

      handle_execution_result(state, Execution.finish_operation(execution, operation, result))
    else
      {:stop, {:shutdown, :execution_receipt_mismatch}, state}
    end
  end

  def handle_info(
        {:DOWN, ref, :process, _pid, _reason},
        %{execution_persist_pending: %{ref: ref, operation: _}} = state
      ),
      do: {:stop, {:shutdown, :execution_operation_unconfirmed}, state}

  def handle_info(
        {:favn_run_cancel_requested, _} = message,
        %{execution_persist_pending: %{operation: _}} = state
      ),
      do: {:noreply, state |> Map.put(:cancel_latched, message) |> defer_execution_event(message)}

  def handle_info(:recover_next, %{execution_persist_pending: _} = state),
    do: {:noreply, defer_execution_event(state, :recover_next)}

  def handle_info(:recover_next, state), do: handle_execution_event(state, :recover_next)

  @impl true
  def handle_info(:continue_execution, %{execution_persist_pending: _} = state),
    do: {:noreply, defer_execution_event(state, :continue_execution)}

  def handle_info(:continue_execution, state), do: continue_execution(state)

  def handle_info(
        {:retry_run_start_persist, token},
        %{run_start_persist_pending: %{token: token, run: running, version: version}} = state
      ) do
    state
    |> Map.delete(:run_start_persist_pending)
    |> persist_run_start(running, version)
  end

  def handle_info(
        {:retry_execution_persist, token},
        %{
          execution_persist_pending: %{token: token, retry: %PersistenceRetry{} = retry},
          execution_state: %RunExecutionState{} = execution_state
        } = state
      ) do
    state = Map.delete(state, :execution_persist_pending)
    handle_execution_result(state, Execution.retry_persistence(execution_state, retry))
  end

  def handle_info({:runner_result, _, _} = message, %{execution_persist_pending: _} = state),
    do: {:noreply, defer_execution_event(state, message)}

  def handle_info(
        {:runner_task_result, _, _, _} = message,
        %{execution_persist_pending: _} = state
      ),
      do: {:noreply, defer_execution_event(state, message)}

  def handle_info(
        {:runner_task_started, _, _, _} = message,
        %{execution_persist_pending: _} = state
      ),
      do: {:noreply, defer_execution_event(state, message)}

  def handle_info(
        {ref, _result} = message,
        %{
          execution_persist_pending: _,
          execution_state: %RunExecutionState{post_step_continuations: continuations}
        } = state
      )
      when is_reference(ref) and is_map_key(continuations, ref),
      do: {:noreply, defer_execution_event(state, message)}

  def handle_info({:DOWN, _, :process, _, _} = message, %{execution_persist_pending: _} = state),
    do: {:noreply, defer_execution_event(state, message)}

  def handle_info({:attempt_timeout, _, _} = message, %{execution_persist_pending: _} = state),
    do: {:noreply, defer_execution_event(state, message)}

  def handle_info({:retry_attempt, _} = message, %{execution_persist_pending: _} = state),
    do: {:noreply, defer_execution_event(state, message)}

  def handle_info(
        {:stage_admission_timeout, _} = message,
        %{execution_persist_pending: _} = state
      ),
      do: {:noreply, defer_execution_event(state, message)}

  def handle_info(
        {:execution_admission_wakeup, _, _} = message,
        %{execution_persist_pending: _} = state
      ),
      do: {:noreply, defer_execution_event(state, message)}

  def handle_info({kind, _token} = message, %{execution_persist_pending: _} = state)
      when kind in [:registration_retry, :registration_deadline],
      do: {:noreply, defer_execution_event(state, message)}

  def handle_info({kind, _token} = message, state)
      when kind in [:registration_retry, :registration_deadline],
      do: handle_execution_event(state, message)

  def handle_info({:runner_result, execution_id, result}, state),
    do: handle_execution_event(state, {:runner_result, execution_id, result})

  def handle_info({:runner_task_result, _workspace_id, task_id, task}, state),
    do: handle_execution_event(state, {:runner_task_result, task_id, task})

  def handle_info(
        {:runner_task_started, _workspace_id, task_id, task},
        %{execution_state: %RunExecutionState{}} = state
      ),
      do: handle_execution_event(state, {:runner_task_started, task_id, task})

  def handle_info(
        {ref, result},
        %{execution_state: %RunExecutionState{post_step_continuations: continuations}} = state
      )
      when is_reference(ref) and is_map_key(continuations, ref),
      do: handle_execution_event(state, {:post_step_reply, ref, result})

  def handle_info(
        {:DOWN, monitor_ref, :process, _pid, reason},
        %{execution_state: %RunExecutionState{} = execution_state} = state
      ) do
    cond do
      execution_id = Map.get(execution_state.await_monitors, monitor_ref) ->
        handle_execution_event(state, {:runner_await_down, execution_id, monitor_ref, reason})

      Map.has_key?(execution_state.post_step_continuations, monitor_ref) ->
        handle_execution_event(state, {:post_step_worker_down, monitor_ref, reason})

      true ->
        {:noreply, state}
    end
  end

  def handle_info({:attempt_timeout, execution_id, timer_ref}, state),
    do: handle_execution_event(state, {:attempt_timeout, execution_id, timer_ref})

  def handle_info({:retry_attempt, timer_ref}, state),
    do: handle_execution_event(state, {:retry_attempt, timer_ref})

  def handle_info({:stage_admission_timeout, timer_ref}, state),
    do: handle_execution_event(state, {:stage_admission_timeout, timer_ref})

  def handle_info({:execution_admission_wakeup, waiter_id, generation}, state),
    do: handle_execution_event(state, {:execution_admission_wakeup, waiter_id, generation})

  def handle_info(
        {:retry_terminal_persist, token},
        %{terminal_persist_pending: %{token: token} = pending} = state
      ) do
    retry_terminal_persist(state, pending)
  end

  def handle_info(
        {:favn_run_cancel_requested, _reason} = message,
        %{execution_persist_pending: %{retry: %PersistenceRetry{event_type: event_type}}} =
          state
      )
      when event_type in [
             :resource_outcomes,
             :step_finished,
             :step_failed,
             :step_timed_out,
             :step_cancelled,
             :step_settled,
             :registration_retry_scheduled
           ],
      do: {:noreply, defer_execution_event(state, message)}

  def handle_info(
        {:favn_run_cancel_requested, reason},
        %{execution_state: %RunExecutionState{} = execution_state} = state
      ) do
    state = cancel_pending_execution_resume(state)
    execution_state = %{execution_state | run: latest_run_snapshot(execution_state.run)}

    execution_state =
      if execution_state.stage_state,
        do: %{
          execution_state
          | stage_state: %{execution_state.stage_state | run: execution_state.run}
        },
        else: execution_state

    handle_execution_result(state, Execution.cancel(execution_state, reason))
  end

  # Without execution state there is nothing to unwind here: pending start or
  # terminal persists detect the durable cancel evidence through sequence
  # fencing on their next attempt. Log the dropped hint so it is observable.
  def handle_info({:favn_run_cancel_requested, reason}, state) do
    OperationalEvents.emit(
      :run_cancel_hint_dropped,
      %{},
      %{run_id: current_run_id(state), reason: reason},
      level: :warning
    )

    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp persist_run_start(state, %RunState{} = running, %Version{} = version) do
    case Persistence.persist_run_step(running, :run_started, %{status: running.status}) do
      :ok ->
        start_execution(state, running, version)

      {:error, :external_cancel} ->
        :ok = RunExecutionCleanup.release_admission(running)
        stop_normally(state, Snapshots.cancelled_snapshot(running))

      {:error, :cancellation_race} ->
        stop_for_cancellation_recovery(state, running)

      {:error, :fenced} ->
        stop_on_fenced_write(state, running, :run_started)

      {:error, reason} ->
        schedule_run_start_persist_retry(state, running, version, reason)
    end
  end

  defp start_execution(state, %RunState{} = running, %Version{} = version) do
    running = RunState.with_snapshot_hash(running)

    case Execution.start_state(running, version) do
      {:ok, execution_state} ->
        case resize_execution_memory(state, running, execution_state) do
          :ok ->
            state
            |> Map.delete(:version)
            |> Map.put(:run_state, running)
            |> Map.put(:execution_state, execution_state)
            |> continue_execution()

          {:error, {:run_plan_exceeds_node_capacity, _, _} = reason} ->
            terminal =
              Snapshots.snapshot_update(running,
                status: :error,
                error: reason
              )

            finalize_terminal(state, terminal)

          {:error, reason} ->
            :ok = Execution.release_manifest_lease(running)
            {:stop, {:shutdown, reason}, state}
        end

      {:recovery_required, reason} ->
        FavnOrchestrator.RunServer.RecoveryAttention.record(
          running,
          {:restore_execution_inputs, reason}
        )

        {:stop, {:shutdown, :run_execution_recovery_required},
         Map.put(state, :run_state, running)}

      {:terminal, terminal} ->
        :ok = Execution.release_manifest_lease(running)
        finalize_terminal(state, terminal)
    end
  end

  defp start_failure_cleanup(state, run, version) do
    case FailureCleanup.new(run, version) do
      {:ok, cleanup} ->
        send(self(), :continue_failure_cleanup)

        {:noreply,
         state
         |> Map.put(:execution_state, nil)
         |> Map.put(:run_state, run)
         |> Map.put(:failure_cleanup, cleanup)}

      {:error, reason} ->
        {:stop, {:shutdown, {:cleanup_inputs_unavailable, reason}}, state}
    end
  end

  defp account_helper_memory(%{capacity_managed?: true}, run, retained),
    do: RunManager.resize_active_run_memory(run, retained)

  defp account_helper_memory(_state, _run, _retained), do: :ok

  defp resize_execution_memory(
         %{capacity_managed?: true},
         %RunState{} = run,
         %RunExecutionState{} = execution_state
       ),
       do: RunManager.resize_active_run_memory(run, execution_state)

  defp resize_execution_memory(_state, %RunState{}, %RunExecutionState{}), do: :ok

  defp schedule_run_start_persist_retry(state, running, version, reason) do
    token = make_ref()
    Process.send_after(self(), {:retry_run_start_persist, token}, execution_persist_retry_ms())

    OperationalEvents.emit(
      :run_start_persist_retry_scheduled,
      %{},
      %{run_id: running.id, reason: reason},
      level: :warning
    )

    {:noreply,
     state
     |> Map.put(:run_state, running)
     |> Map.put(:run_start_persist_pending, %{
       token: token,
       run: running,
       version: version,
       reason: reason
     })}
  end

  defp continue_execution(%{execution_state: nil} = state), do: {:noreply, state}

  defp continue_execution(%{execution_state: %RunExecutionState{} = execution_state} = state) do
    handle_execution_result(state, Execution.handle_event(execution_state, :continue))
  end

  defp handle_execution_event(
         %{execution_state: %RunExecutionState{} = execution_state} = state,
         event
       ) do
    handle_execution_result(state, Execution.handle_event(execution_state, event))
  end

  defp handle_execution_event(state, _event), do: {:noreply, state}

  defp handle_execution_result(state, {:operation, execution, operation}) do
    retained = {execution, operation, operation, Map.get(state, :deferred_execution_events, [])}

    case account_helper_memory(state, execution.run, retained) do
      :ok ->
        task =
          FavnOrchestrator.RunHelper.async(execution.run, fn ->
            Execution.perform_operation(operation)
          end)

        pending = %{
          ref: task.ref,
          operation: operation,
          base_sequence: execution.run.event_seq,
          generation: execution.run.storage_fencing_token
        }

        {:noreply,
         state
         |> Map.put(:run_state, execution.run)
         |> Map.put(:execution_state, execution)
         |> Map.put(:execution_persist_pending, pending)}

      {:error, reason} ->
        {:stop, {:shutdown, {:execution_memory_limit, reason}}, state}
    end
  end

  defp handle_execution_result(state, {:cont, %RunExecutionState{} = execution_state}) do
    next =
      state
      |> Map.put(:run_state, execution_state.run)
      |> Map.put(:execution_state, execution_state)
      |> replay_deferred_execution_events()

    if get_in(state, [:execution_state, Access.key(:recovery)]) do
      case resize_execution_memory(next, execution_state.run, execution_state) do
        :ok ->
          {:noreply, next}

        {:error, reason} ->
          handle_execution_result(
            next,
            {:recovery_required, execution_state, {:recovery_memory_limit, reason}}
          )
      end
    else
      {:noreply, next}
    end
  end

  defp handle_execution_result(state, {:terminal, %RunState{} = terminal}) do
    finalize_terminal(state, terminal)
  end

  defp handle_execution_result(
         state,
         {:ownership_gate, %RunExecutionState{} = execution_state, %PersistenceRetry{} = retry}
       ) do
    handle_execution_result(state, Execution.resume_persisted_retry(execution_state, retry))
  end

  defp handle_execution_result(state, {:persist_retry, execution, _retry, :cancellation_race}),
    do: stop_for_cancellation_recovery(%{state | execution_state: execution}, execution.run)

  defp handle_execution_result(
         state,
         {:persist_retry, %RunExecutionState{} = execution_state, %PersistenceRetry{} = retry,
          :fenced}
       ) do
    state
    |> Map.put(:run_state, execution_state.run)
    |> Map.put(:execution_state, execution_state)
    |> stop_on_fenced_write(execution_state.run, retry.event_type)
  end

  defp handle_execution_result(
         state,
         {:recovery_required, %RunExecutionState{} = execution,
          {:registration_retry_exhausted, _} = reason}
       ) do
    execution = Execution.stop_for_recovery(execution)
    run = execution.run
    task = FavnOrchestrator.RunHelper.async(run, fn -> FailureCleanup.fail(run, reason) end)

    {:noreply,
     state
     |> Map.put(:execution_state, nil)
     |> Map.put(:run_state, run)
     |> Map.put(:failure_operation, %{ref: task.ref})}
  end

  defp handle_execution_result(
         state,
         {:recovery_required, %RunExecutionState{} = execution_state, reason}
       ) do
    unless transient_recovery?(reason),
      do: FavnOrchestrator.RunServer.RecoveryAttention.record(execution_state.run, reason)

    {:stop, {:shutdown, :run_execution_recovery_required},
     state
     |> Map.put(:run_state, execution_state.run)
     |> Map.put(:execution_state, Execution.stop_for_recovery(execution_state))}
  end

  defp handle_execution_result(
         state,
         {:persist_retry, %RunExecutionState{} = execution_state, %PersistenceRetry{} = retry,
          reason}
       ) do
    retry = PersistenceRetry.rejected(retry, reason)
    token = make_ref()

    Process.send_after(
      self(),
      {:retry_execution_persist, token},
      execution_persist_retry_ms()
    )

    OperationalEvents.emit(
      :run_execution_persist_retry_scheduled,
      %{},
      Map.merge(PersistenceRetry.diagnostics(retry), %{
        run_id: retry.run.id,
        event_type: retry.event_type,
        reason: reason
      }),
      level: :warning
    )

    {:noreply,
     state
     |> Map.put(:run_state, execution_state.run)
     |> Map.put(:execution_state, execution_state)
     |> Map.put(:execution_persist_pending, %{token: token, retry: retry, reason: reason})}
  end

  defp transient_recovery?(%FavnOrchestrator.Persistence.Error{retryable?: true, kind: kind})
       when kind in [:conflict, :unavailable, :timeout], do: true

  defp transient_recovery?(reason) when is_tuple(reason),
    do: reason |> Tuple.to_list() |> Enum.any?(&transient_recovery?/1)

  defp transient_recovery?(_), do: false

  defp finalize_terminal(state, %RunState{} = terminal) do
    state = stop_post_step_workers(state)

    cond do
      terminal.status == :cancelled and persisted_cancelled?(terminal) ->
        :ok = RunExecutionCleanup.release_admission(terminal)
        stop_normally(state, terminal)

      terminal.status != :cancelled and persisted_cancelled?(terminal) ->
        :ok = RunExecutionCleanup.release_admission(terminal)
        stop_normally(state, terminal)

      true ->
        terminal_event_type = Persistence.terminal_event_type(terminal)

        finalized =
          RunState.transition(terminal,
            metadata: Map.put(terminal.metadata, :terminal_event_type, terminal_event_type)
          )

        persist_terminal_or_retry(state, finalized, terminal_event_type)
    end
  end

  defp persist_terminal_or_retry(state, %RunState{} = finalized, terminal_event_type) do
    data = %{status: finalized.status, error: finalized.error}

    case Persistence.persist_run_step(finalized, terminal_event_type, data) do
      :ok ->
        :ok = RunExecutionCleanup.release_admission(finalized)

        stop_normally(state, finalized)

      {:error, :external_cancel} ->
        cancelled = Snapshots.cancelled_snapshot(finalized)
        :ok = RunExecutionCleanup.release_admission(cancelled)

        stop_normally(state, cancelled)

      {:error, :cancellation_race} ->
        refresh_terminal_after_cancellation(state, finalized)

      {:error, :fenced} ->
        stop_on_fenced_write(state, finalized, terminal_event_type)

      {:error, reason} ->
        schedule_terminal_persist_retry(state, finalized, terminal_event_type, data, reason, 1)
    end
  end

  defp retry_terminal_persist(state, pending) do
    finalized = pending.terminal

    case Persistence.persist_run_step(finalized, pending.event_type, pending.data) do
      :ok ->
        :ok = RunExecutionCleanup.release_admission(finalized)

        state
        |> Map.delete(:terminal_persist_pending)
        |> stop_normally(finalized)

      {:error, :external_cancel} ->
        cancelled = Snapshots.cancelled_snapshot(finalized)
        :ok = RunExecutionCleanup.release_admission(cancelled)

        state
        |> Map.delete(:terminal_persist_pending)
        |> stop_normally(cancelled)

      {:error, :cancellation_race} ->
        refresh_terminal_after_cancellation(state, finalized)

      {:error, :fenced} ->
        state
        |> Map.delete(:terminal_persist_pending)
        |> stop_on_fenced_write(finalized, pending.event_type)

      {:error, reason} ->
        schedule_terminal_persist_retry(
          state,
          finalized,
          pending.event_type,
          pending.data,
          reason,
          pending.attempt + 1
        )
    end
  end

  defp schedule_terminal_persist_retry(
         state,
         %RunState{} = terminal,
         event_type,
         data,
         reason,
         attempt
       ) do
    token = make_ref()
    Process.send_after(self(), {:retry_terminal_persist, token}, @terminal_persist_retry_ms)

    OperationalEvents.emit(
      :run_terminal_persist_retry_scheduled,
      %{},
      %{run_id: terminal.id, event_type: event_type, attempt: attempt, reason: reason},
      level: :warning
    )

    {:noreply,
     state
     |> Map.put(:run_state, terminal)
     |> Map.put(:execution_state, nil)
     |> Map.put(:terminal_persist_pending, %{
       token: token,
       terminal: terminal,
       event_type: event_type,
       data: data,
       reason: reason,
       attempt: attempt
     })}
  end

  # A fenced write proves a newer owner exists. Retrying could never succeed and
  # would only delay recovery, so the process stops the way a failed renewal does.
  defp refresh_terminal_after_cancellation(state, terminal) do
    context = SystemContext.workspace(terminal.workspace_id, :run_worker)
    event_type = Persistence.terminal_event_type(terminal)

    case Runs.get(context, terminal.id) do
      {:ok, latest} ->
        if RunState.terminal_status?(latest.status) do
          :ok = RunExecutionCleanup.release_admission(terminal)
          stop_normally(state, latest)
        else
          refreshed =
            latest
            |> copy_storage_fence(terminal)
            |> RunState.transition(
              status: terminal.status,
              error: terminal.error,
              result: terminal.result,
              metadata: Map.merge(terminal.metadata, latest.metadata)
            )

          persist_terminal_or_retry(state, refreshed, event_type)
        end

      {:error, reason} ->
        schedule_terminal_persist_retry(
          state,
          terminal,
          event_type,
          %{status: terminal.status, error: terminal.error},
          reason,
          1
        )
    end
  end

  defp stop_for_cancellation_recovery(state, run) do
    state =
      case Map.get(state, :execution_state) do
        %RunExecutionState{} = execution ->
          %{state | execution_state: Execution.stop_for_recovery(execution)}

        nil ->
          state
      end

    stop_normally(state, run)
  end

  defp stop_on_fenced_write(state, %RunState{} = run, event_type) do
    state =
      case Map.get(state, :execution_state) do
        %RunExecutionState{} = execution ->
          Map.put(state, :execution_state, Execution.stop_for_recovery(execution))

        nil ->
          state
      end

    OperationalEvents.emit(
      :run_ownership_lost,
      %{},
      %{workspace_id: run.workspace_id, run_id: run.id, event_type: event_type, reason: :fenced},
      level: :error
    )

    {:stop, {:shutdown, :run_ownership_lost}, Map.put(state, :run_state, run)}
  end

  defp stop_post_step_workers(%{execution_state: %RunExecutionState{} = execution_state} = state),
    do: Map.put(state, :execution_state, Execution.stop_post_step_workers(execution_state))

  defp stop_post_step_workers(state), do: state

  defp current_run_id(%{run_state: %RunState{id: run_id}}), do: run_id
  defp current_run_id(_state), do: nil

  defp persisted_cancelled?(%RunState{workspace_id: workspace_id, id: run_id})
       when is_binary(workspace_id) do
    context = SystemContext.workspace(workspace_id, :run_worker)
    match?({:ok, %RunState{status: :cancelled}}, Runs.get(context, run_id))
  end

  defp persisted_cancelled?(%RunState{}), do: false

  defp latest_run_snapshot(%RunState{workspace_id: workspace_id, id: run_id} = fallback)
       when is_binary(workspace_id) do
    context = SystemContext.workspace(workspace_id, :run_worker)

    case Runs.get(context, run_id) do
      {:ok, %RunState{} = run} ->
        run |> Map.put(:result, fallback.result) |> copy_storage_fence(fallback)

      _ ->
        fallback
    end
  end

  defp latest_run_snapshot(%RunState{} = fallback), do: fallback

  defp defer_execution_event(state, message) do
    queue = Map.get(state, :deferred_execution_events, [])
    queue = if message in queue, do: queue, else: queue ++ [message]
    next = Map.put(state, :deferred_execution_events, queue)

    case account_helper_memory(
           next,
           state.execution_state.run,
           {state.execution_state, Map.get(state, :execution_persist_pending), queue}
         ) do
      :ok -> next
      {:error, _} -> exit({:shutdown, :deferred_execution_memory_limit})
    end
  end

  defp replay_deferred_execution_events(state) do
    if Map.get(state, :deferred_execution_events, []) != [] and
         not Map.has_key?(state, :execution_persist_pending) and
         not Map.get(state, :deferred_drain_scheduled, false) do
      send(self(), :drain_deferred_execution)
      Map.put(state, :deferred_drain_scheduled, true)
    else
      state
    end
  end

  defp cancel_pending_execution_resume(
         %{
           execution_state: %RunExecutionState{paused_admission: pause}
         } = state
       )
       when is_map(pause) do
    state =
      case Map.pop(state, :execution_persist_pending) do
        {%{token: _token}, next} ->
          next

        {nil, next} ->
          next
      end

    state
  end

  defp cancel_pending_execution_resume(state), do: state

  defp execution_persist_retry_ms do
    case Application.get_env(
           :favn_orchestrator,
           :execution_persist_retry_ms,
           @execution_persist_retry_ms
         ) do
      value when is_integer(value) and value > 0 -> value
      _invalid -> @execution_persist_retry_ms
    end
  end

  defp bind_storage_ownership(
         %{storage_ownership: %Ownership{} = ownership} = state,
         %RunState{workspace_id: workspace_id, id: run_id} = run
       )
       when workspace_id == ownership.workspace_id and run_id == ownership.run_id do
    context = SystemContext.workspace(workspace_id, :run_worker)
    owned_run = RunState.with_storage_fence(run, ownership.owner_id, ownership.fencing_token)

    next_state =
      state
      |> Map.put(:storage_context, context)
      |> Map.put(:run_state, owned_run)

    {:ok, next_state, owned_run}
  end

  defp bind_storage_ownership(%{storage_ownership: %Ownership{}}, %RunState{}),
    do: {:error, :recovery_ownership_mismatch}

  defp stop_normally(state, %RunState{} = run) do
    :ok = release_manifest_lease(state)
    {:stop, :normal, state |> Map.put(:run_state, run) |> Map.put(:execution_state, nil)}
  end

  defp release_manifest_lease(_state), do: :ok

  defp copy_storage_fence(%RunState{} = run, %RunState{} = authority) do
    case {authority.storage_owner_id, authority.storage_fencing_token} do
      {owner_id, token} when is_binary(owner_id) and is_integer(token) ->
        RunState.with_storage_fence(run, owner_id, token)

      _missing ->
        run
    end
  end

  @impl true
  def terminate(_reason, state) do
    state =
      case Map.get(state, :execution_state) do
        %RunExecutionState{} = execution_state ->
          Map.put(state, :execution_state, Execution.stop_for_recovery(execution_state))

        nil ->
          state
      end

    _ = stop_post_step_workers(state)
    release_manifest_lease(state)
    :ok
  end
end
