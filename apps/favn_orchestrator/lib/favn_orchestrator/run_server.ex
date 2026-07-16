defmodule FavnOrchestrator.RunServer do
  @moduledoc """
  Process owner for one manifest-pinned orchestrator run.

  The server advances the non-blocking execution state machine from runner,
  retry, admission, and cancellation messages. Terminal persistence is retried
  in-process so transient storage failures do not discard the final outcome.
  """

  use GenServer

  alias Favn.Manifest.Version
  alias FavnOrchestrator.CancellationOutcome
  alias FavnOrchestrator.OperationalEvents
  alias FavnOrchestrator.RunExecutionCleanup
  alias FavnOrchestrator.RunExecutionOwnership
  alias FavnOrchestrator.RunServer.Execution
  alias FavnOrchestrator.RunServer.Execution.RunExecutionState
  alias FavnOrchestrator.RunServer.Persistence
  alias FavnOrchestrator.RunServer.PersistenceRetry
  alias FavnOrchestrator.RunServer.Snapshots
  alias FavnOrchestrator.RunState

  @type init_arg :: %{
          required(:run_state) => RunState.t(),
          required(:version) => Version.t(),
          optional(:recovering?) => boolean()
        }

  @terminal_persist_retry_ms 1_000
  @execution_persist_retry_ms 1_000

  @doc "Starts an unregistered process for one run snapshot and manifest version."
  @spec start_link(init_arg()) :: GenServer.on_start()
  def start_link(args) when is_map(args), do: GenServer.start_link(__MODULE__, args)

  @impl true
  def init(%{run_state: %RunState{}, version: %Version{}} = args) do
    {:ok, args, {:continue, :execute}}
  end

  @impl true
  def handle_continue(:execute, %{run_state: run_state, version: version} = state) do
    if RunState.finalized?(run_state) do
      :ok = RunExecutionCleanup.release_admission(run_state.id)
      {:stop, :normal, state |> Map.put(:run_state, run_state) |> Map.put(:execution_state, nil)}
    else
      continue_start(state, run_state, version)
    end
  end

  defp continue_start(%{recovering?: true} = state, %RunState{} = run_state, %Version{} = version) do
    start_execution(state, run_state, version)
  end

  defp continue_start(state, %RunState{} = run_state, %Version{} = version) do
    running = RunState.transition(run_state, status: :running)
    persist_run_start(state, running, version)
  end

  @impl true
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

  def handle_info({:runner_result, execution_id, result}, state),
    do: handle_execution_event(state, {:runner_result, execution_id, result})

  def handle_info(
        {:DOWN, monitor_ref, :process, _pid, reason},
        %{execution_state: %RunExecutionState{} = execution_state} = state
      ) do
    case Map.get(execution_state.await_monitors, monitor_ref) do
      nil ->
        {:noreply, state}

      execution_id ->
        handle_execution_event(state, {:runner_await_down, execution_id, monitor_ref, reason})
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
        {:favn_run_cancel_requested, reason},
        %{execution_state: %RunExecutionState{} = execution_state} = state
      ) do
    execution_state = %{execution_state | run: latest_run_snapshot(execution_state.run)}
    terminal = Execution.cancel(execution_state, reason)
    finalize_terminal(state, terminal)
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp persist_run_start(state, %RunState{} = running, %Version{} = version) do
    case Persistence.persist_run_step(running, :run_started, %{status: running.status}) do
      :ok ->
        start_execution(state, running, version)

      {:error, :external_cancel} ->
        :ok = RunExecutionCleanup.release_admission(running.id)
        {:stop, :normal, %{state | run_state: Snapshots.cancelled_snapshot(running)}}

      {:error, reason} ->
        schedule_run_start_persist_retry(state, running, version, reason)
    end
  end

  defp start_execution(state, %RunState{} = running, %Version{} = version) do
    case Execution.start_state(running, version) do
      {:ok, execution_state} ->
        state
        |> Map.put(:run_state, running)
        |> Map.put(:execution_state, execution_state)
        |> continue_execution()

      {:terminal, terminal} ->
        finalize_terminal(state, terminal)
    end
  end

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

  defp continue_execution(%{execution_state: %RunExecutionState{} = execution_state} = state) do
    handle_execution_result(state, Execution.handle_event(execution_state, :continue))
  end

  defp handle_execution_event(
         %{execution_state: %RunExecutionState{} = execution_state} = state,
         event
       ) do
    handle_execution_result(state, Execution.handle_event(execution_state, event))
  end

  defp handle_execution_result(state, {:cont, %RunExecutionState{} = execution_state}) do
    next =
      state
      |> Map.put(:run_state, execution_state.run)
      |> Map.put(:execution_state, execution_state)
      |> replay_deferred_execution_events()

    {:noreply, next}
  end

  defp handle_execution_result(state, {:terminal, %RunState{} = terminal}) do
    finalize_terminal(state, terminal)
  end

  defp handle_execution_result(
         state,
         {:persist_retry, %RunExecutionState{} = execution_state, %PersistenceRetry{} = retry,
          reason}
       ) do
    token = make_ref()

    Process.send_after(
      self(),
      {:retry_execution_persist, token},
      execution_persist_retry_ms()
    )

    OperationalEvents.emit(
      :run_execution_persist_retry_scheduled,
      %{},
      %{
        run_id: retry.run.id,
        event_type: retry.event_type,
        reason: reason
      },
      level: :warning
    )

    {:noreply,
     state
     |> Map.put(:run_state, execution_state.run)
     |> Map.put(:execution_state, execution_state)
     |> Map.put(:execution_persist_pending, %{token: token, retry: retry, reason: reason})}
  end

  defp finalize_terminal(state, %RunState{} = terminal) do
    cond do
      terminal.status == :cancelled and persisted_cancelled?(terminal.id) ->
        :ok = RunExecutionCleanup.release_admission(terminal.id)
        {:stop, :normal, state |> Map.put(:run_state, terminal) |> Map.put(:execution_state, nil)}

      terminal.status != :cancelled and Persistence.externally_cancelled?(terminal.id) ->
        :ok = RunExecutionCleanup.release_admission(terminal.id)
        {:stop, :normal, state |> Map.put(:run_state, terminal) |> Map.put(:execution_state, nil)}

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
        maybe_complete_active_ownerships(finalized)
        :ok = RunExecutionCleanup.release_admission(finalized.id)

        {:stop, :normal,
         state |> Map.put(:run_state, finalized) |> Map.put(:execution_state, nil)}

      {:error, :external_cancel} ->
        cancelled = Snapshots.cancelled_snapshot(finalized)
        :ok = RunExecutionCleanup.release_admission(cancelled.id)

        {:stop, :normal,
         state |> Map.put(:run_state, cancelled) |> Map.put(:execution_state, nil)}

      {:error, reason} ->
        schedule_terminal_persist_retry(state, finalized, terminal_event_type, data, reason, 1)
    end
  end

  defp retry_terminal_persist(state, pending) do
    finalized = pending.terminal

    case Persistence.persist_run_step(finalized, pending.event_type, pending.data) do
      :ok ->
        maybe_complete_active_ownerships(finalized)
        :ok = RunExecutionCleanup.release_admission(finalized.id)

        {:stop, :normal,
         state
         |> Map.put(:run_state, finalized)
         |> Map.put(:execution_state, nil)
         |> Map.delete(:terminal_persist_pending)}

      {:error, :external_cancel} ->
        cancelled = Snapshots.cancelled_snapshot(finalized)
        :ok = RunExecutionCleanup.release_admission(cancelled.id)

        {:stop, :normal,
         state
         |> Map.put(:run_state, cancelled)
         |> Map.put(:execution_state, nil)
         |> Map.delete(:terminal_persist_pending)}

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

  defp maybe_complete_active_ownerships(%RunState{} = run) do
    if ownership_completion_safe?(run) do
      complete_active_ownerships(run.id)
    else
      :ok
    end
  end

  defp complete_active_ownerships(run_id) do
    case RunExecutionOwnership.complete_active(run_id) do
      :ok ->
        :ok

      {:error, reason} ->
        OperationalEvents.emit(
          :run_execution_ownership_completion_failed,
          %{},
          %{run_id: run_id, reason: reason},
          level: :warning
        )

        :ok
    end
  end

  defp ownership_completion_safe?(%RunState{metadata: metadata}) when is_map(metadata) do
    outcomes = Map.get(metadata, :cancel_outcomes, Map.get(metadata, "cancel_outcomes", []))

    ledger_error =
      Map.get(
        metadata,
        :cancellation_ledger_persist_error,
        Map.get(metadata, "cancellation_ledger_persist_error")
      )

    is_nil(ledger_error) and
      (outcomes == [] or Enum.all?(outcomes, &CancellationOutcome.confirmed?/1))
  end

  defp ownership_completion_safe?(%RunState{}), do: true

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

  defp persisted_cancelled?(run_id) do
    match?({:ok, %RunState{status: :cancelled}}, FavnOrchestrator.Storage.get_run(run_id))
  end

  defp latest_run_snapshot(%RunState{id: run_id} = fallback) do
    case FavnOrchestrator.Storage.get_run(run_id) do
      {:ok, %RunState{} = run} -> run
      _ -> fallback
    end
  end

  defp defer_execution_event(state, message) do
    Map.update(state, :deferred_execution_events, [message], &[message | &1])
  end

  defp replay_deferred_execution_events(state) do
    state
    |> Map.get(:deferred_execution_events, [])
    |> Enum.reverse()
    |> Enum.each(&send(self(), &1))

    Map.delete(state, :deferred_execution_events)
  end

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
end
