defmodule FavnOrchestrator.Scheduler.StateStore do
  @moduledoc false

  alias Favn.Scheduler.State
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.Storage.JsonSafe

  @state_fields [
    :pipeline_module,
    :schedule_id,
    :schedule_fingerprint,
    :last_evaluated_at,
    :activation_state,
    :last_due_at,
    :last_submitted_due_at,
    :in_flight_run_id,
    :queued_due_at,
    :last_scheduler_error,
    :updated_at,
    :version
  ]

  @spec load(%{optional(module()) => map()}) ::
          {:ok, %{optional(module()) => State.t()}} | {:error, term()}
  def load(entries) when is_map(entries) do
    now = DateTime.utc_now()

    Enum.reduce_while(entries, {:ok, %{}}, fn {pipeline_module, entry}, {:ok, acc} ->
      case load_entry(pipeline_module, entry, now) do
        {:ok, state} -> {:cont, {:ok, Map.put(acc, pipeline_module, state)}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  @spec persist_evaluation(State.t(), State.t()) ::
          {:ok, State.t()} | {:error, term(), State.t()}
  def persist_evaluation(%State{} = previous, %State{} = next) do
    if changed?(previous, next) do
      persisted = with_next_version(previous, next)
      key = {persisted.pipeline_module, persisted.schedule_id}

      case Storage.put_scheduler_state(key, persisted) do
        :ok -> {:ok, persisted}
        {:error, reason} -> {:error, reason, persisted}
      end
    else
      {:ok, next}
    end
  end

  @spec flush_dirty(map()) :: {:ok, map()} | {:error, term(), map()}
  def flush_dirty(runtime_state) when is_map(runtime_state) do
    dirty_states = Map.get(runtime_state, :dirty_states, %{})

    Enum.reduce_while(dirty_states, {:ok, runtime_state}, fn
      {pipeline_module, dirty_state}, {:ok, acc} ->
        key = {pipeline_module, dirty_state.schedule_id}

        case Storage.put_scheduler_state(key, dirty_state) do
          :ok -> {:cont, {:ok, clear_dirty(acc, pipeline_module, dirty_state)}}
          {:error, reason} -> {:halt, {:error, reason, record_failure(acc, reason)}}
        end
    end)
  end

  @spec record_failure(map(), term()) :: map()
  def record_failure(runtime_state, reason) when is_map(runtime_state) do
    Map.put(runtime_state, :last_persist_error, persist_error(reason))
  end

  @spec persist_error(term()) :: map()
  def persist_error(reason) do
    %{reason: JsonSafe.error(reason), occurred_at: DateTime.utc_now()}
  end

  defp load_entry(pipeline_module, entry, now) do
    key = {pipeline_module, entry.schedule.name}

    case Storage.get_scheduler_state(key) do
      {:ok, nil} -> create_entry_state(key, pipeline_module, entry, now)
      {:ok, stored} when is_map(stored) -> reconcile_entry_state(key, stored, entry, now)
      {:error, reason} -> {:error, reason}
    end
  end

  defp create_entry_state(key, pipeline_module, entry, now) do
    state = %State{
      pipeline_module: pipeline_module,
      schedule_id: entry.schedule.name,
      schedule_fingerprint: entry.schedule_fingerprint,
      activation_state: :pending_activation,
      updated_at: now,
      version: 1
    }

    case Storage.put_scheduler_state(key, state) do
      :ok -> {:ok, state}
      {:error, reason} -> {:error, reason}
    end
  end

  defp reconcile_entry_state(key, stored, entry, now) do
    previous = normalize(stored)
    next = reconcile(previous, entry, now)

    if previous != next do
      persisted = with_next_version(previous, next)

      case Storage.put_scheduler_state(key, persisted) do
        :ok -> {:ok, persisted}
        {:error, reason} -> {:error, reason}
      end
    else
      {:ok, next}
    end
  end

  defp reconcile(%State{} = state, entry, now) do
    if state.schedule_fingerprint == entry.schedule_fingerprint do
      state
      |> bootstrap_activation(entry)
      |> Map.put(:pipeline_module, entry.module)
      |> Map.put(:schedule_id, entry.schedule.name)
    else
      %State{
        pipeline_module: entry.module,
        schedule_id: entry.schedule.name,
        schedule_fingerprint: entry.schedule_fingerprint,
        activation_state: :needs_review,
        updated_at: now
      }
    end
  end

  defp normalize(%State{} = state), do: state

  defp normalize(state) when is_map(state) do
    state
    |> Map.take(@state_fields)
    |> then(&struct(State, &1))
  end

  defp bootstrap_activation(%State{activation_state: nil} = state, entry) do
    %{state | activation_state: initial_activation(state, entry)}
  end

  defp bootstrap_activation(%State{} = state, _entry), do: state

  defp initial_activation(%State{} = state, entry) do
    cond do
      entry.schedule.active != true -> :disabled
      runtime_cursor?(state) -> :enabled
      true -> :pending_activation
    end
  end

  defp runtime_cursor?(%State{} = state) do
    Enum.any?(
      [
        state.last_due_at,
        state.last_submitted_due_at,
        state.in_flight_run_id,
        state.queued_due_at
      ],
      &(not is_nil(&1))
    )
  end

  defp changed?(previous, next) do
    previous.schedule_id != next.schedule_id or
      previous.schedule_fingerprint != next.schedule_fingerprint or
      previous.activation_state != next.activation_state or
      previous.last_due_at != next.last_due_at or
      previous.last_submitted_due_at != next.last_submitted_due_at or
      previous.in_flight_run_id != next.in_flight_run_id or
      previous.queued_due_at != next.queued_due_at or
      previous.last_scheduler_error != next.last_scheduler_error
  end

  defp with_next_version(previous, %State{} = next) do
    %{next | version: next_version(previous)}
  end

  defp next_version(%State{version: version}) when is_integer(version) and version > 0,
    do: version + 1

  defp next_version(_previous), do: 1

  defp clear_dirty(runtime_state, pipeline_module, state) do
    %{
      runtime_state
      | states: Map.put(runtime_state.states, pipeline_module, state),
        dirty_states: Map.delete(runtime_state.dirty_states, pipeline_module),
        last_persist_error: nil
    }
  end
end
