defmodule FavnOrchestrator.TransitionWriter do
  @moduledoc """
  Writes authoritative run transitions and publishes live events after successful writes.
  """

  alias FavnOrchestrator.Backfill
  alias FavnOrchestrator.Events
  alias FavnOrchestrator.Projector
  alias FavnOrchestrator.RunEvent
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage

  require Logger

  @spec persist_transition(RunState.t(), atom(), map()) :: :ok | {:error, term()}
  def persist_transition(%RunState{} = run_state, event_type, data \\ %{})
      when is_atom(event_type) and is_map(data) do
    event = Projector.run_event(run_state, event_type, data)

    case Storage.persist_run_transition(run_state, RunEvent.to_map(event)) do
      :ok ->
        Events.broadcast_run_event(event)
        project_derived_state(run_state, event_type, data)
        :ok

      :idempotent ->
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp project_derived_state(%RunState{} = run_state, event_type, data) do
    safe_project(Backfill.Projector, run_state, event_type, data)
    safe_project(Backfill.CoverageProjector, run_state, event_type, data)
  end

  defp safe_project(projector, %RunState{} = run_state, event_type, data) do
    case projector.project_transition(run_state, event_type, data) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning("derived projection failed: #{inspect(projector)} #{inspect(reason)}")
    end
  rescue
    error ->
      Logger.warning(
        "derived projection raised: #{inspect(projector)} #{Exception.message(error)}"
      )
  catch
    kind, reason ->
      Logger.warning(
        "derived projection exited: #{inspect(projector)} #{inspect({kind, reason})}"
      )
  end
end
