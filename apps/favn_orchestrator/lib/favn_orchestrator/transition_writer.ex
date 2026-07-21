defmodule FavnOrchestrator.TransitionWriter do
  @moduledoc """
  Writes authoritative run transitions and publishes live events after successful writes.
  """

  alias FavnOrchestrator.Events
  alias FavnOrchestrator.LogWriter
  alias FavnOrchestrator.OperationalEvents
  alias FavnOrchestrator.Projector
  alias FavnOrchestrator.Runs
  alias FavnOrchestrator.RunEvent
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.Persistence.Results.RunCommitted
  alias FavnOrchestrator.Storage.JsonSafe

  require Logger

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
      safe_emit_transition_log(context, committed.event)
    end

    :ok
  end

  defp emit_persisted_transition(run_state, event_type) do
    OperationalEvents.emit(:run_transition_persisted, %{count: 1}, %{
      workspace_id: run_state.workspace_id,
      run_id: run_state.id,
      event_type: event_type,
      status: run_state.status,
      submit_kind: run_state.submit_kind,
      required_runner_release_id: run_state.required_runner_release_id
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
        required_runner_release_id: run_state.required_runner_release_id,
        reason: reason
      },
      level: :error
    )
  end

  defp safe_emit_transition_log(%WorkspaceContext{} = context, event) when is_map(event) do
    event = RunEvent.from_map(event)

    if event.entity == :step do
      entry = transition_log_entry(event)

      case LogWriter.write(context, entry, occurred_at: event.occurred_at) do
        {:ok, _entries} -> :ok
        {:error, reason} -> Logger.warning("transition log write failed: #{inspect(reason)}")
      end
    else
      :ok
    end
  rescue
    error -> Logger.warning("transition log write raised: #{Exception.message(error)}")
  catch
    kind, reason -> Logger.warning("transition log write exited: #{inspect({kind, reason})}")
  end

  defp transition_log_entry(%RunEvent{} = event) do
    %{
      run_id: event.run_id,
      asset_step_id: data_field(event, :asset_step_id),
      node_key: data_field(event, :node_key),
      asset_ref: event.asset_ref,
      runner_execution_id: data_field(event, :runner_execution_id),
      attempt: data_field(event, :attempt),
      occurred_at: event.occurred_at,
      level: transition_log_level(event.event_type),
      source: :orchestrator,
      message: transition_log_message(event.event_type),
      metadata: transition_log_metadata(event),
      producer_id: "orchestrator:#{event.run_id}",
      producer_sequence: event.sequence
    }
  end

  defp transition_log_level(event_type)
       when event_type in [:step_failed, :step_timed_out, :step_cancelled, :step_blocked],
       do: :error

  defp transition_log_level(:step_retry_scheduled), do: :warning
  defp transition_log_level(_event_type), do: :info

  defp transition_log_message(:step_started), do: "asset execution started"
  defp transition_log_message(:step_retry_started), do: "asset execution retry started"
  defp transition_log_message(:step_finished), do: "asset execution finished"
  defp transition_log_message(:step_failed), do: "asset execution failed"
  defp transition_log_message(:step_timed_out), do: "asset execution timed out"
  defp transition_log_message(:step_cancelled), do: "asset execution cancelled"
  defp transition_log_message(:step_retry_scheduled), do: "asset execution retry scheduled"
  defp transition_log_message(:step_skipped_fresh), do: "asset skipped because it is fresh"
  defp transition_log_message(:step_blocked), do: "asset execution blocked"

  defp transition_log_message(event_type) when is_atom(event_type),
    do: event_type |> Atom.to_string() |> String.replace("_", " ")

  defp transition_log_message(event_type), do: to_string(event_type)

  defp transition_log_metadata(%RunEvent{} = event) do
    %{
      event_type: event.event_type,
      status: event.status,
      stage: event.stage,
      attempt: data_field(event, :attempt),
      max_attempts: data_field(event, :max_attempts),
      freshness_key: data_field(event, :freshness_key),
      result_status: data_field(event, :result_status),
      error: JsonSafe.error(data_field(event, :error)),
      reason: JsonSafe.error(data_field(event, :reason))
    }
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Map.new()
  end

  defp data_field(%RunEvent{data: data}, key) when is_map(data) do
    Map.get(data, key) || Map.get(data, Atom.to_string(key))
  end

  defp data_field(%RunEvent{}, _key), do: nil
end
