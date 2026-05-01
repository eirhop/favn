defmodule FavnOrchestrator.RunServer do
  @moduledoc false

  use GenServer

  alias Favn.Manifest.Version
  alias FavnOrchestrator.RunServer.Execution
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
        terminal = Execution.execute_plan(running, version)

        if Persistence.externally_cancelled?(terminal.id) do
          {:stop, :normal, %{state | run_state: terminal}}
        else
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
              {:stop, :normal, %{state | run_state: finalized}}

            {:error, :external_cancel} ->
              {:stop, :normal, %{state | run_state: Snapshots.cancelled_snapshot(finalized)}}
          end
        end

      {:error, :external_cancel} ->
        {:stop, :normal, %{state | run_state: Snapshots.cancelled_snapshot(running)}}
    end
  end
end
