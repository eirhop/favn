defmodule FavnOrchestrator.Scheduler.Diagnostics do
  @moduledoc false

  alias FavnOrchestrator.Scheduler.Cron
  alias FavnOrchestrator.SchedulerEntry

  @spec payload(map()) :: map()
  def payload(state) when is_map(state) do
    states = Map.values(state.states)

    %{
      running?: true,
      manifest_version_id: manifest_version_id(state.version),
      entry_count: map_size(state.entries),
      active_schedule_count: count_entries(state.entries, & &1.schedule.active),
      inactive_schedule_count: count_entries(state.entries, &(not &1.schedule.active)),
      in_flight_schedule_count: Enum.count(states, &is_binary(Map.get(&1, :in_flight_run_id))),
      queued_schedule_count: Enum.count(states, &present?(Map.get(&1, :queued_due_at))),
      dirty_scheduler_state_count: map_size(state.dirty_states),
      last_scheduler_persist_error: state.last_persist_error,
      state_summary: state_summary(state),
      tick_ms: state.tick_ms,
      auto_tick?: state.auto_tick?
    }
  end

  @spec entries(map(), DateTime.t()) :: [SchedulerEntry.t()]
  def entries(state, %DateTime{} = now) when is_map(state) do
    state.entries
    |> Enum.map(fn {pipeline_module, entry} ->
      entry = Map.put(entry, :next_due_at, next_due_at(entry, now))
      SchedulerEntry.from_runtime(entry, Map.get(state.states, pipeline_module))
    end)
    |> Enum.sort_by(&{inspect(&1.pipeline_module), inspect(&1.schedule_id)})
  end

  defp state_summary(state) do
    entries =
      state.entries
      |> Map.values()
      |> Enum.map(&state_entry(&1, Map.get(state.states, &1.module), state.version))
      |> Enum.sort_by(&{to_string(&1.schedule_id), &1.id})

    %{
      state_count: map_size(state.states),
      evaluated_count: count_states(state.states, &present?(&1.last_evaluated_at)),
      due_cursor_count: count_states(state.states, &present?(&1.last_due_at)),
      submitted_cursor_count: count_states(state.states, &present?(&1.last_submitted_due_at)),
      in_flight_count: count_states(state.states, &is_binary(&1.in_flight_run_id)),
      queued_count: count_states(state.states, &present?(&1.queued_due_at)),
      dirty_count: map_size(state.dirty_states),
      updated_count: count_states(state.states, &present?(&1.updated_at)),
      entries: entries
    }
  end

  defp state_entry(entry, state, version) do
    schedule_id = entry.schedule.name

    %{
      id: state_entry_id(version, entry, schedule_id),
      schedule_id: schedule_id,
      active?: entry.schedule.active == true,
      evaluated?: present?(field(state, :last_evaluated_at)),
      due?: present?(field(state, :last_due_at)),
      submitted?: present?(field(state, :last_submitted_due_at)),
      in_flight?: is_binary(field(state, :in_flight_run_id)),
      queued?: present?(field(state, :queued_due_at)),
      updated?: present?(field(state, :updated_at))
    }
  end

  defp state_entry_id(version, entry, schedule_id) do
    source = [
      manifest_version_id(version),
      inspect(entry.module),
      to_string(entry.id),
      to_string(schedule_id),
      entry.schedule_fingerprint || ""
    ]

    digest =
      :sha256
      |> :crypto.hash(Enum.join(source, ":"))
      |> Base.url_encode64(padding: false)
      |> binary_part(0, 16)

    "scheduler_state:" <> digest
  end

  defp count_states(states, fun) do
    states
    |> Map.values()
    |> Enum.count(fun)
  end

  defp count_entries(entries, fun) do
    entries
    |> Map.values()
    |> Enum.count(fun)
  end

  defp manifest_version_id(nil), do: nil
  defp manifest_version_id(version), do: version.manifest_version_id

  defp present?(nil), do: false
  defp present?(_value), do: true

  defp field(nil, _key), do: nil
  defp field(state, key), do: Map.get(state, key)

  defp next_due_at(%{schedule: %{cron: cron, timezone: timezone}}, %DateTime{} = now)
       when is_binary(cron) and is_binary(timezone),
       do: Cron.next_due(cron, timezone, now)

  defp next_due_at(_entry, _now), do: nil
end
