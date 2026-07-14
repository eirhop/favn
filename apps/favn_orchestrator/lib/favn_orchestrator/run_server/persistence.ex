defmodule FavnOrchestrator.RunServer.Persistence do
  @moduledoc """
  Durable run-transition boundary for the run server.

  Stale or conflicting writes are translated to external cancellation only when
  the latest stored snapshot contains explicit cancellation evidence.
  """

  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.TransitionWriter

  @doc "Persists one run snapshot and its matching event atomically."
  @spec persist_run_step(RunState.t(), atom(), map()) :: :ok | {:error, term()}
  def persist_run_step(%RunState{} = run_state, event_type, data) do
    durable_run = durable_snapshot(run_state)

    case TransitionWriter.persist_transition(durable_run, event_type, data) do
      :ok ->
        :ok

      {:error, reason} when reason in [:stale_write, :conflicting_snapshot] ->
        if externally_cancelled?(run_state.id) do
          {:error, :external_cancel}
        else
          {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp durable_snapshot(%RunState{} = run_state) do
    if RunState.finalized?(run_state) or is_nil(run_state.result) do
      run_state
    else
      run_state |> Map.put(:result, nil) |> RunState.with_snapshot_hash()
    end
  end

  @doc "Returns the durable terminal event for a terminal run snapshot."
  @spec terminal_event_type(RunState.t()) :: atom()
  def terminal_event_type(%RunState{status: status}),
    do: RunState.terminal_event_type(status) || :run_failed

  @doc "Returns true only for explicit cancellation in the latest stored snapshot."
  @spec externally_cancelled?(String.t()) :: boolean()
  def externally_cancelled?(run_id) when is_binary(run_id) do
    case Storage.get_run(run_id) do
      {:ok, %RunState{status: :cancelled}} ->
        true

      {:ok, %RunState{metadata: metadata}} when is_map(metadata) ->
        Map.get(metadata, :cancel_requested) == true or
          Map.get(metadata, "cancel_requested") == true

      _ ->
        false
    end
  end
end
