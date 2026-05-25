defmodule FavnOrchestrator.RunServer do
  @moduledoc false

  use GenServer

  alias Favn.Manifest.Version
  alias FavnOrchestrator.ExecutionAdmission
  alias FavnOrchestrator.OperationalEvents
  alias FavnOrchestrator.RunExecutionOwnership
  alias FavnOrchestrator.RunServer.Execution
  alias FavnOrchestrator.RunServer.Execution.RunExecutionState
  alias FavnOrchestrator.RunServer.Persistence
  alias FavnOrchestrator.RunServer.Snapshots
  alias FavnOrchestrator.RunState

  @type init_arg :: %{
          required(:run_state) => RunState.t(),
          required(:version) => Version.t()
        }

  @terminal_persist_retry_ms 1_000

  @spec start_link(init_arg()) :: GenServer.on_start()
  def start_link(args) when is_map(args), do: GenServer.start_link(__MODULE__, args)

  @impl true
  def init(%{run_state: %RunState{}, version: %Version{}} = args) do
    {:ok, args, {:continue, :execute}}
  end

  @impl true
  def handle_continue(:execute, %{run_state: run_state, version: version} = state) do
    if RunState.finalized?(run_state) do
      :ok = ExecutionAdmission.release_run(run_state.id)
      {:stop, :normal, state |> Map.put(:run_state, run_state) |> Map.put(:execution_state, nil)}
    else
      running = RunState.transition(run_state, status: :running)

      case Persistence.persist_run_step(running, :run_started, %{status: running.status}) do
        :ok ->
          case Execution.start_state(running, version) do
            {:ok, execution_state} ->
              state
              |> Map.put(:run_state, running)
              |> Map.put(:execution_state, execution_state)
              |> continue_execution()

            {:terminal, terminal} ->
              finalize_terminal(state, terminal)
          end

        {:error, :external_cancel} ->
          {:stop, :normal, %{state | run_state: Snapshots.cancelled_snapshot(running)}}
      end
    end
  end

  @impl true
  def handle_info(:continue_execution, state), do: continue_execution(state)

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
    {:noreply,
     state
     |> Map.put(:run_state, execution_state.run)
     |> Map.put(:execution_state, execution_state)}
  end

  defp handle_execution_result(state, {:terminal, %RunState{} = terminal}) do
    finalize_terminal(state, terminal)
  end

  defp finalize_terminal(state, %RunState{} = terminal) do
    cond do
      terminal.status == :cancelled and persisted_cancelled?(terminal.id) ->
        :ok = ExecutionAdmission.release_run(terminal.id)
        :ok = ExecutionAdmission.cancel_run_waits(terminal.id)
        {:stop, :normal, state |> Map.put(:run_state, terminal) |> Map.put(:execution_state, nil)}

      terminal.status != :cancelled and Persistence.externally_cancelled?(terminal.id) ->
        :ok = ExecutionAdmission.release_run(terminal.id)
        :ok = ExecutionAdmission.cancel_run_waits(terminal.id)
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
        maybe_complete_active_ownerships(finalized.id)
        :ok = ExecutionAdmission.release_run(finalized.id)
        :ok = ExecutionAdmission.cancel_run_waits(finalized.id)

        {:stop, :normal,
         state |> Map.put(:run_state, finalized) |> Map.put(:execution_state, nil)}

      {:error, :external_cancel} ->
        cancelled = Snapshots.cancelled_snapshot(finalized)
        :ok = ExecutionAdmission.release_run(cancelled.id)
        :ok = ExecutionAdmission.cancel_run_waits(cancelled.id)

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
        maybe_complete_active_ownerships(finalized.id)
        :ok = ExecutionAdmission.release_run(finalized.id)
        :ok = ExecutionAdmission.cancel_run_waits(finalized.id)

        {:stop, :normal,
         state
         |> Map.put(:run_state, finalized)
         |> Map.put(:execution_state, nil)
         |> Map.delete(:terminal_persist_pending)}

      {:error, :external_cancel} ->
        cancelled = Snapshots.cancelled_snapshot(finalized)
        :ok = ExecutionAdmission.release_run(cancelled.id)
        :ok = ExecutionAdmission.cancel_run_waits(cancelled.id)

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

  defp maybe_complete_active_ownerships(run_id) do
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
end
