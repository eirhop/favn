defmodule FavnOrchestrator.TransitionWriter do
  @moduledoc """
  Writes authoritative run transitions and publishes live events after successful writes.
  """

  alias FavnOrchestrator.Events
  alias FavnOrchestrator.Projector
  alias FavnOrchestrator.RunEvent
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage

  @spec persist_transition(RunState.t(), atom(), map()) :: :ok | {:error, term()}
  def persist_transition(%RunState{} = run_state, event_type, data \\ %{})
      when is_atom(event_type) and is_map(data) do
    event = Projector.run_event(run_state, event_type, data)

    with :ok <- Storage.persist_run_transition(run_state, RunEvent.to_map(event)) do
      Events.broadcast_run_event(event)
    end
  end
end
