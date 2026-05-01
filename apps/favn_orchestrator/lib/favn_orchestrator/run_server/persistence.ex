defmodule FavnOrchestrator.RunServer.Persistence do
  @moduledoc false

  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.TransitionWriter

  def persist_run_step(%RunState{} = run_state, event_type, data) do
    case TransitionWriter.persist_transition(run_state, event_type, data) do
      :ok ->
        :ok

      {:error, reason} when reason in [:stale_write, :conflicting_snapshot] ->
        if externally_cancelled?(run_state.id) do
          {:error, :external_cancel}
        else
          raise "unexpected run snapshot conflict: #{inspect(reason)}"
        end

      {:error, reason} ->
        raise "failed to persist run snapshot: #{inspect(reason)}"
    end
  end

  def terminal_event_type(%RunState{status: :ok}), do: :run_finished
  def terminal_event_type(%RunState{status: :cancelled}), do: :run_cancelled
  def terminal_event_type(%RunState{status: :timed_out}), do: :run_timed_out
  def terminal_event_type(%RunState{}), do: :run_failed

  def externally_cancelled?(run_id) when is_binary(run_id) do
    case Storage.get_run(run_id) do
      {:ok, %RunState{status: :cancelled}} -> true
      _ -> false
    end
  end
end
