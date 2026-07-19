defmodule FavnOrchestrator.RunServer.Persistence do
  @moduledoc """
  Durable run-transition boundary for the run server.

  Stale or conflicting writes are translated to external cancellation only when
  the latest stored snapshot contains explicit cancellation evidence.
  """

  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Runs
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.TransitionWriter

  @doc "Persists one run snapshot and its matching event atomically."
  @spec persist_run_step(RunState.t(), atom(), map()) :: :ok | {:error, term()}
  def persist_run_step(%RunState{} = run_state, event_type, data) do
    durable_run = RunState.for_step_persistence(run_state)

    case persist_transition(durable_run, event_type, data) do
      :ok ->
        :ok

      {:error, reason} when reason in [:stale_write, :conflicting_snapshot] ->
        if externally_cancelled?(run_state) do
          {:error, :external_cancel}
        else
          {:error, reason}
        end

      {:error, %Error{kind: :conflict} = reason} ->
        if externally_cancelled?(run_state),
          do: {:error, :external_cancel},
          else: {:error, reason}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc "Returns the durable terminal event for a terminal run snapshot."
  @spec terminal_event_type(RunState.t()) :: atom()
  def terminal_event_type(%RunState{status: status}),
    do: RunState.terminal_event_type(status) || :run_failed

  @doc "Returns true only for explicit cancellation in the latest stored snapshot."
  @spec externally_cancelled?(RunState.t()) :: boolean()
  def externally_cancelled?(%RunState{workspace_id: workspace_id, id: run_id})
      when is_binary(workspace_id) do
    context = SystemContext.workspace(workspace_id, :run_worker)

    case Runs.get(context, run_id) do
      {:ok, %RunState{status: :cancelled}} ->
        true

      {:ok, %RunState{metadata: metadata}} when is_map(metadata) ->
        Map.get(metadata, :cancel_requested) == true or
          Map.get(metadata, "cancel_requested") == true

      _ ->
        false
    end
  end

  @spec externally_cancelled?(%{workspace_id: String.t(), run_id: String.t()}) :: boolean()
  def externally_cancelled?(%{workspace_id: workspace_id, run_id: run_id})
      when is_binary(workspace_id) and is_binary(run_id) do
    context = SystemContext.workspace(workspace_id, :run_worker)

    cancelled_run?(Runs.get(context, run_id))
  end

  defp persist_transition(%RunState{workspace_id: workspace_id} = run, event_type, data)
       when is_binary(workspace_id) do
    context = SystemContext.workspace(workspace_id, :run_worker)
    TransitionWriter.persist_transition(context, run, event_type, data)
  end

  defp cancelled_run?({:ok, %RunState{status: :cancelled}}), do: true

  defp cancelled_run?({:ok, %RunState{metadata: metadata}}) when is_map(metadata) do
    Map.get(metadata, :cancel_requested) == true or
      Map.get(metadata, "cancel_requested") == true
  end

  defp cancelled_run?(_result), do: false
end
