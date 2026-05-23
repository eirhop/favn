defmodule FavnOrchestrator.RunServer do
  @moduledoc false

  use GenServer

  alias Favn.Manifest.Version
  alias FavnOrchestrator.ExecutionAdmission
  alias FavnOrchestrator.RunServer.Execution
  alias FavnOrchestrator.RunServer.Execution.RunExecutionState
  alias FavnOrchestrator.RunServer.Persistence
  alias FavnOrchestrator.RunServer.Snapshots
  alias FavnOrchestrator.RunState

  @type init_arg :: %{
          required(:run_state) => RunState.t(),
          required(:version) => Version.t()
        }

  @spec start_link(init_arg()) :: GenServer.on_start()
  def start_link(args) when is_map(args), do: GenServer.start_link(__MODULE__, args)

  @impl true
  def init(%{run_state: %RunState{}, version: %Version{}} = args) do
    {:ok, args, {:continue, :execute}}
  end

  @impl true
  def handle_continue(:execute, %{run_state: run_state, version: version} = state) do
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
        {:favn_run_cancel_requested, reason},
        %{execution_state: %RunExecutionState{} = execution_state} = state
      ) do
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
    if Persistence.externally_cancelled?(terminal.id) do
      :ok = ExecutionAdmission.release_run(terminal.id)
      :ok = ExecutionAdmission.cancel_run_waits(terminal.id)
      {:stop, :normal, state |> Map.put(:run_state, terminal) |> Map.put(:execution_state, nil)}
    else
      :ok = ExecutionAdmission.release_run(terminal.id)
      :ok = ExecutionAdmission.cancel_run_waits(terminal.id)
      terminal_event_type = Persistence.terminal_event_type(terminal)

      finalized =
        RunState.transition(terminal,
          metadata: Map.put(terminal.metadata, :terminal_event_type, terminal_event_type)
        )

      case Persistence.persist_run_step(finalized, terminal_event_type, %{
             status: finalized.status,
             error: finalized.error
           }) do
        :ok ->
          {:stop, :normal,
           state |> Map.put(:run_state, finalized) |> Map.put(:execution_state, nil)}

        {:error, :external_cancel} ->
          {:stop, :normal,
           state
           |> Map.put(:run_state, Snapshots.cancelled_snapshot(finalized))
           |> Map.put(:execution_state, nil)}
      end
    end
  end
end
