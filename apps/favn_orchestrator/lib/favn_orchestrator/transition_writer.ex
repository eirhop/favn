defmodule FavnOrchestrator.TransitionWriter do
  @moduledoc """
  Writes authoritative run transitions and publishes live events after successful writes.
  """

  alias FavnOrchestrator.Events
  alias FavnOrchestrator.OperationalEvents
  alias FavnOrchestrator.Projector
  alias FavnOrchestrator.Runs
  alias FavnOrchestrator.RunEvent
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.Persistence.Results.RunCommitted

  @doc "Persists one workspace-scoped authoritative transition through Storage V2."
  @spec persist_transition(WorkspaceContext.t(), RunState.t(), atom(), map()) ::
          :ok | {:error, term()}
  def persist_transition(
        %WorkspaceContext{} = context,
        %RunState{} = run_state,
        event_type,
        data
      ) do
    persist_transition(context, run_state, event_type, data, [])
  end

  @spec persist_transition(WorkspaceContext.t(), RunState.t(), atom(), map(), keyword()) ::
          :ok | {:ok, boolean()} | {:error, term()}
  def persist_transition(
        %WorkspaceContext{} = context,
        %RunState{} = run_state,
        event_type,
        data,
        opts
      )
      when is_atom(event_type) and is_map(data) and is_list(opts) do
    event = Projector.run_event(run_state, event_type, data)

    result =
      if run_state.event_seq == 1 do
        Runs.create(context, run_state, event,
          command_id: Keyword.get(opts, :command_id),
          idempotency: Keyword.get(opts, :idempotency),
          pipeline_refs: Keyword.get(opts, :pipeline_refs, [])
        )
      else
        Runs.commit(context, run_state, event,
          command_id: Keyword.get(opts, :command_id),
          idempotency: Keyword.get(opts, :idempotency),
          owner_id: Keyword.get(opts, :owner_id, run_state.storage_owner_id),
          fencing_token: Keyword.get(opts, :fencing_token, run_state.storage_fencing_token)
        )
      end

    case result do
      {:ok, committed} ->
        :ok = publish_committed(context, committed)

        if Keyword.get(opts, :return_commit?, false),
          do: {:ok, committed.replayed?},
          else: :ok

      {:error, reason} ->
        emit_transition_failure(run_state, event_type, reason)
        {:error, reason}
    end
  end

  @doc false
  @spec publish_committed(WorkspaceContext.t(), RunCommitted.t()) :: :ok
  def publish_committed(%WorkspaceContext{} = context, %RunCommitted{} = committed) do
    unless committed.replayed? do
      event = RunEvent.from_map(committed.event)
      emit_persisted_transition(committed.run, event.event_type)
      Events.broadcast_run_event(context.workspace_id, event)
    end

    :ok
  end

  defp emit_persisted_transition(run_state, event_type) do
    OperationalEvents.emit(:run_transition_persisted, %{count: 1}, %{
      workspace_id: run_state.workspace_id,
      run_id: run_state.id,
      event_type: event_type,
      status: run_state.status,
      submit_kind: run_state.submit_kind
    })
  end

  defp emit_transition_failure(run_state, event_type, reason) do
    OperationalEvents.emit(
      :run_transition_failed,
      %{},
      %{
        workspace_id: run_state.workspace_id,
        run_id: run_state.id,
        event_type: event_type,
        reason: reason
      },
      level: :error
    )
  end
end
