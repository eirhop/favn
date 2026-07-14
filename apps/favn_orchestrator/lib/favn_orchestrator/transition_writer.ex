defmodule FavnOrchestrator.TransitionWriter do
  @moduledoc """
  Writes authoritative run transitions and publishes live events after successful writes.
  """

  alias FavnOrchestrator.AssetWindowProjector
  alias FavnOrchestrator.Backfill
  alias FavnOrchestrator.Events
  alias FavnOrchestrator.LogWriter
  alias FavnOrchestrator.OperationalEvents
  alias FavnOrchestrator.ProjectionDiagnostics
  alias FavnOrchestrator.Projector
  alias FavnOrchestrator.RunEvent
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.Storage.JsonSafe

  require Logger

  @spec persist_transition(RunState.t(), atom(), map()) :: :ok | {:error, term()}
  def persist_transition(%RunState{} = run_state, event_type, data \\ %{})
      when is_atom(event_type) and is_map(data) do
    event = Projector.run_event(run_state, event_type, data)

    case Storage.persist_run_transition(run_state, RunEvent.to_map(event)) do
      :ok ->
        OperationalEvents.emit(:run_transition_persisted, %{count: 1}, %{
          run_id: run_state.id,
          event_type: event_type,
          status: run_state.status,
          submit_kind: run_state.submit_kind
        })

        event |> hydrate_persisted_event() |> Events.broadcast_run_event()
        project_derived_state(run_state, event_type, data)
        safe_emit_transition_log(event)
        :ok

      :idempotent ->
        :ok

      {:error, reason} ->
        OperationalEvents.emit(
          :run_transition_failed,
          %{},
          %{
            run_id: run_state.id,
            event_type: event_type,
            reason: reason
          },
          level: :error
        )

        {:error, reason}
    end
  end

  defp hydrate_persisted_event(%RunEvent{} = event) do
    case Storage.list_run_events(event.run_id,
           after_sequence: event.sequence - 1,
           limit: 1
         ) do
      {:ok, [persisted | _]} ->
        RunEvent.from_map(persisted)

      {:ok, []} ->
        Logger.warning("persisted run event missing after transition write")
        event

      {:error, reason} ->
        Logger.warning("persisted run event hydration failed: #{inspect(reason)}")
        event
    end
  end

  defp project_derived_state(%RunState{} = run_state, event_type, data) do
    safe_project(Backfill.Projector, run_state, event_type, data)
    safe_project(Backfill.CoverageProjector, run_state, event_type, data)
    safe_project(AssetWindowProjector, run_state, event_type, data)
    safe_project(FavnOrchestrator.TargetStatus.Projector, run_state, event_type, data)
  end

  defp safe_emit_transition_log(%RunEvent{entity: :step} = event) do
    entry = %{
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

    case LogWriter.write(entry) do
      {:ok, _entries} -> :ok
      {:error, reason} -> Logger.warning("transition log write failed: #{inspect(reason)}")
    end
  rescue
    error -> Logger.warning("transition log write raised: #{Exception.message(error)}")
  catch
    kind, reason -> Logger.warning("transition log write exited: #{inspect({kind, reason})}")
  end

  defp safe_emit_transition_log(%RunEvent{}), do: :ok

  defp transition_log_level(event_type)
       when event_type in [:step_failed, :step_timed_out, :step_cancelled, :step_blocked],
       do: :error

  defp transition_log_level(:step_retry_scheduled), do: :warning
  defp transition_log_level(_event_type), do: :info

  defp transition_log_message(:step_started), do: "asset execution started"
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

  defp safe_project(projector, %RunState{} = run_state, event_type, data) do
    case projector.project_transition(run_state, event_type, data) do
      :ok ->
        :ok

      {:error, reason} ->
        ProjectionDiagnostics.record_failure(projector, run_state, event_type, reason)

        OperationalEvents.emit(
          :projection_degraded,
          %{},
          %{
            projector: inspect(projector),
            run_id: run_state.id,
            event_type: event_type,
            reason: reason
          },
          level: :warning
        )

        Logger.warning("derived projection failed: #{inspect(projector)} #{inspect(reason)}")
    end
  rescue
    error ->
      ProjectionDiagnostics.record_failure(projector, run_state, event_type, error)

      OperationalEvents.emit(
        :projection_degraded,
        %{},
        %{
          projector: inspect(projector),
          run_id: run_state.id,
          event_type: event_type,
          reason: error
        },
        level: :warning
      )

      Logger.warning(
        "derived projection raised: #{inspect(projector)} #{Exception.message(error)}"
      )
  catch
    kind, reason ->
      ProjectionDiagnostics.record_failure(projector, run_state, event_type, {kind, reason})

      OperationalEvents.emit(
        :projection_degraded,
        %{},
        %{
          projector: inspect(projector),
          run_id: run_state.id,
          event_type: event_type,
          reason: {kind, reason}
        },
        level: :warning
      )

      Logger.warning(
        "derived projection exited: #{inspect(projector)} #{inspect({kind, reason})}"
      )
  end
end
