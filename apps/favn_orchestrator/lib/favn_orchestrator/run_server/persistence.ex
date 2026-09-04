defmodule FavnOrchestrator.RunServer.Persistence do
  @moduledoc """
  Durable run-transition boundary for the run server.

  Stale or conflicting writes are translated to external cancellation only when
  the latest stored snapshot contains explicit cancellation evidence. A write
  rejected by the run-ownership fence is returned as `{:error, :fenced}`; the
  run server stops on it instead of retrying, because a newer owner exists.
  """

  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Runs
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.TransitionWriter

  @doc "Persists one run snapshot and its matching event atomically."
  @spec persist_run_step(RunState.t(), atom(), map()) ::
          :ok | {:error, :external_cancel | :fenced | term()}
  def persist_run_step(%RunState{} = run_state, event_type, data) do
    durable_run = RunState.for_step_persistence(run_state)

    case persist_transition(durable_run, event_type, data) do
      :ok ->
        :ok

      {:error, %Error{kind: :fenced}} ->
        {:error, :fenced}

      {:error, reason} when reason in [:stale_write, :conflicting_snapshot] ->
        cancellation_conflict(run_state, reason)

      {:error, %Error{kind: :conflict} = reason} ->
        cancellation_conflict(run_state, reason)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp cancellation_conflict(run, reason) do
    case cancellation_state(run) do
      :cancelled -> {:error, :external_cancel}
      :requested -> {:error, :cancellation_race}
      nil -> {:error, reason}
    end
  end

  @doc "Returns the durable terminal event for a terminal run snapshot."
  @spec terminal_event_type(RunState.t()) :: atom()
  def terminal_event_type(%RunState{status: status}),
    do: RunState.terminal_event_type(status) || :run_failed

  @doc "Returns true when the latest snapshot has cancellation intent or a cancelled outcome."
  @spec externally_cancelled?(RunState.t() | %{workspace_id: String.t(), run_id: String.t()}) ::
          boolean()
  def externally_cancelled?(run), do: cancellation_state(run) in [:requested, :cancelled]

  @doc "Distinguishes durable cancellation intent from the immutable terminal outcome."
  @spec cancellation_state(RunState.t() | %{workspace_id: String.t(), run_id: String.t()}) ::
          :requested | :cancelled | nil
  def cancellation_state(%RunState{workspace_id: workspace_id, id: run_id}),
    do: cancellation_state(%{workspace_id: workspace_id, run_id: run_id})

  def cancellation_state(%{workspace_id: workspace_id, run_id: run_id})
      when is_binary(workspace_id) and is_binary(run_id) do
    context = SystemContext.workspace(workspace_id, :run_worker)

    case Runs.get(context, run_id) do
      {:ok, %RunState{status: :cancelled}} ->
        :cancelled

      {:ok, %RunState{metadata: metadata}} when is_map(metadata) ->
        if Map.get(metadata, :cancel_requested) == true or
             Map.get(metadata, "cancel_requested") == true,
           do: :requested

      _ ->
        nil
    end
  end

  defp persist_transition(%RunState{workspace_id: workspace_id} = run, event_type, data)
       when is_binary(workspace_id) do
    context = SystemContext.workspace(workspace_id, :run_worker)
    TransitionWriter.persist_transition(context, run, event_type, data)
  end
end
