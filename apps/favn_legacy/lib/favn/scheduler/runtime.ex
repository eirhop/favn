defmodule Favn.Scheduler.Runtime do
  @moduledoc false
  use GenServer
  require Logger

  alias Favn.Scheduler.Cron
  alias Favn.Scheduler.Registry
  alias Favn.Scheduler.State
  alias Favn.Scheduler.Storage
  alias Favn.Window.Anchor

  @default_tick_ms 15_000

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def reload, do: GenServer.call(__MODULE__, :reload, :infinity)
  def tick, do: GenServer.call(__MODULE__, :tick, :infinity)
  def scheduled, do: GenServer.call(__MODULE__, :scheduled, :infinity)

  @impl true
  def init(_opts) do
    case load_runtime() do
      {:ok, state} ->
        schedule_tick(next_tick_delay_ms(state.tick_ms))
        {:ok, state}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_call(:reload, _from, _state) do
    reply = load_runtime()

    case reply do
      {:ok, next} ->
        {:reply, :ok, next}

      {:error, reason} ->
        {:reply, {:error, reason}, %{entries: %{}, states: %{}, tick_ms: @default_tick_ms}}
    end
  end

  def handle_call(:tick, _from, state) do
    {:reply, :ok, evaluate_all(state)}
  end

  def handle_call(:scheduled, _from, state) do
    {:reply, Map.values(state.entries), state}
  end

  @impl true
  def handle_info(:tick, state) do
    next = evaluate_all(state)
    schedule_tick(next_tick_delay_ms(next.tick_ms))
    {:noreply, next}
  end

  defp load_runtime do
    with {:ok, entries} <- Registry.discover(),
         {:ok, states} <- load_states(entries) do
      {:ok, %{entries: entries, states: states, tick_ms: tick_ms()}}
    end
  end

  defp load_states(entries) do
    states =
      Enum.reduce_while(entries, {:ok, %{}}, fn {pipeline_module, entry}, {:ok, acc} ->
        case Storage.get_state(pipeline_module, entry.schedule.id) do
          {:ok, nil} ->
            base = %State{
              pipeline_module: pipeline_module,
              schedule_id: entry.schedule.id,
              schedule_fingerprint: entry.schedule_fingerprint
            }

            {:cont, {:ok, Map.put(acc, pipeline_module, base)}}

          {:ok, %State{} = stored} ->
            value =
              if stored.schedule_fingerprint == entry.schedule_fingerprint do
                %{stored | schedule_id: entry.schedule.id}
              else
                %State{
                  pipeline_module: pipeline_module,
                  schedule_id: entry.schedule.id,
                  schedule_fingerprint: entry.schedule_fingerprint
                }
              end

            if stored.schedule_fingerprint != entry.schedule_fingerprint do
              :ok = Storage.put_state(value)
            end

            {:cont, {:ok, Map.put(acc, pipeline_module, value)}}

          {:error, reason} ->
            {:halt, {:error, reason}}
        end
      end)

    states
  end

  defp evaluate_all(state) do
    now = DateTime.utc_now()

    next_states =
      Enum.reduce(state.entries, state.states, fn {pipeline_module, entry}, acc ->
        current = Map.get(acc, pipeline_module, %State{pipeline_module: pipeline_module})

        updated =
          if entry.schedule.active do
            evaluate_entry(entry, current, now)
          else
            %{current | last_evaluated_at: now, updated_at: now}
          end

        if persist_state_change?(current, updated) do
          :ok = Storage.put_state(updated)
        end

        Map.put(acc, pipeline_module, updated)
      end)

    %{state | states: next_states}
  end

  defp evaluate_entry(entry, state, now) do
    state = reconcile_in_flight(state)

    latest_due = Cron.latest_due(entry.schedule.cron, entry.schedule.timezone, now)

    cond do
      is_nil(latest_due) ->
        %{state | last_evaluated_at: now, updated_at: now}

      is_nil(state.last_due_at) ->
        %{state | last_due_at: latest_due, last_evaluated_at: now, updated_at: now}

      true ->
        selected =
          select_occurrences(
            entry.schedule.missed,
            entry.schedule.cron,
            entry.schedule.timezone,
            state.last_due_at,
            latest_due
          )

        {next_state, submitted_due_ats_reversed} =
          state
          |> maybe_submit_queued(entry, latest_due, now)
          |> submit_due_occurrences(entry, selected, latest_due, now)

        submitted_due_ats = Enum.reverse(submitted_due_ats_reversed)

        cursor_due =
          next_cursor_due(
            entry.schedule.missed,
            state.last_due_at,
            latest_due,
            selected,
            submitted_due_ats
          )

        next_state
        |> Map.put(:last_due_at, cursor_due)
        |> Map.put(:last_evaluated_at, now)
        |> Map.put(:updated_at, now)
    end
  end

  defp maybe_submit_queued(%State{queued_due_at: nil} = state, _entry, _latest_due, _now),
    do: {state, []}

  defp maybe_submit_queued(%State{in_flight_run_id: run_id} = state, _entry, _latest_due, _now)
       when is_binary(run_id),
       do: {state, []}

  defp maybe_submit_queued(state, entry, latest_due, now) do
    case submit_occurrence(state, entry, state.queued_due_at, latest_due, now) do
      {:ok, next} -> {%{next | queued_due_at: nil}, [state.queued_due_at]}
      {:error, _} -> {state, []}
    end
  end

  defp submit_due_occurrences({state, submitted_reversed}, _entry, [], _latest_due, _now),
    do: {state, submitted_reversed}

  defp submit_due_occurrences({state, submitted_reversed}, entry, [due | rest], latest_due, now) do
    state = reconcile_in_flight(state)

    case entry.schedule.overlap do
      :allow ->
        {next, submitted_next} =
          case submit_occurrence(state, entry, due, latest_due, now, track_in_flight?: false) do
            {:ok, value} -> {value, [due | submitted_reversed]}
            {:error, _} -> {state, submitted_reversed}
          end

        submit_due_occurrences({next, submitted_next}, entry, rest, latest_due, now)

      :forbid ->
        if is_binary(state.in_flight_run_id) do
          {state, submitted_reversed}
        else
          case submit_occurrence(state, entry, due, latest_due, now) do
            {:ok, value} -> {value, [due | submitted_reversed]}
            {:error, _} -> {state, submitted_reversed}
          end
        end

      :queue_one ->
        if is_binary(state.in_flight_run_id) do
          {%{state | queued_due_at: List.last([due | rest])}, submitted_reversed}
        else
          case submit_occurrence(state, entry, due, latest_due, now) do
            {:ok, value} ->
              next = if rest == [], do: value, else: %{value | queued_due_at: List.last(rest)}
              {next, [due | submitted_reversed]}

            {:error, _} ->
              {state, submitted_reversed}
          end
        end
    end
  end

  defp submit_occurrence(state, entry, due_at, latest_due, now, opts \\ []) do
    track_in_flight? = Keyword.get(opts, :track_in_flight?, true)

    trigger = %{
      kind: :schedule,
      pipeline: %{module: entry.module, id: entry.id},
      schedule: %{
        id: entry.schedule.id,
        ref: entry.schedule.ref,
        cron: entry.schedule.cron,
        timezone: entry.schedule.timezone,
        overlap: entry.schedule.overlap,
        missed: entry.schedule.missed,
        active: entry.schedule.active
      },
      occurrence: %{
        due_at: due_at,
        occurrence_key: occurrence_key(entry.module, due_at),
        recovery: if(DateTime.compare(due_at, latest_due) == :eq, do: :on_time, else: :missed)
      },
      evaluated_at: now
    }

    run_opts = [trigger: trigger]
    run_opts = maybe_put_anchor(run_opts, entry.window, due_at, entry.schedule.timezone)

    case Favn.run_pipeline(entry.module, run_opts) do
      {:ok, run_id} ->
        next =
          if track_in_flight? do
            %{state | in_flight_run_id: run_id, last_submitted_due_at: due_at, updated_at: now}
          else
            %{state | last_submitted_due_at: due_at, updated_at: now}
          end

        {:ok, next}

      {:error, reason} ->
        Logger.warning("scheduler submit failed for #{inspect(entry.module)}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp maybe_put_anchor(run_opts, nil, _due_at, _timezone), do: run_opts

  defp maybe_put_anchor(run_opts, kind, due_at, timezone) when kind in [:hour, :day, :month] do
    local = DateTime.shift_zone!(due_at, timezone)
    start_at = floor_kind(local, kind)
    end_at = shift_kind(start_at, kind)
    anchor = Anchor.new!(kind, start_at, end_at, timezone: timezone)
    Keyword.put(run_opts, :anchor_window, anchor)
  end

  defp select_occurrences(:all, cron, timezone, last_due_at, latest_due) do
    Cron.occurrences_between(cron, timezone, last_due_at, latest_due)
  end

  defp select_occurrences(:skip, cron, timezone, last_due_at, latest_due) do
    case Cron.last_occurrence_between(cron, timezone, last_due_at, latest_due) do
      nil -> []
      due_at -> [due_at]
    end
  end

  defp select_occurrences(:one, cron, timezone, last_due_at, latest_due) do
    case Cron.first_occurrence_between(cron, timezone, last_due_at, latest_due) do
      nil -> []
      due_at -> [due_at]
    end
  end

  defp next_cursor_due(_policy, previous_due_at, _latest_due, _selected, []), do: previous_due_at

  defp next_cursor_due(:skip, _previous_due_at, latest_due, _selected, _submitted), do: latest_due

  defp next_cursor_due(:one, _previous_due_at, _latest_due, _selected, submitted),
    do: List.last(submitted)

  defp next_cursor_due(:all, _previous_due_at, _latest_due, _selected, submitted),
    do: List.last(submitted)

  defp persist_state_change?(previous, next) do
    previous.schedule_id != next.schedule_id or
      previous.schedule_fingerprint != next.schedule_fingerprint or
      previous.last_due_at != next.last_due_at or
      previous.last_submitted_due_at != next.last_submitted_due_at or
      previous.in_flight_run_id != next.in_flight_run_id or
      previous.queued_due_at != next.queued_due_at
  end

  defp reconcile_in_flight(%State{in_flight_run_id: nil} = state), do: state

  defp reconcile_in_flight(%State{in_flight_run_id: run_id} = state) do
    case Favn.get_run(run_id) do
      {:ok, run} when run.status == :running -> state
      {:ok, _terminal} -> %{state | in_flight_run_id: nil}
      {:error, :not_found} -> %{state | in_flight_run_id: nil}
      {:error, _} -> state
    end
  end

  defp occurrence_key(module, %DateTime{} = due_at) do
    "schedule:" <> Atom.to_string(module) <> ":" <> DateTime.to_iso8601(due_at)
  end

  defp tick_ms do
    case Application.get_env(:favn, :scheduler, []) do
      opts when is_list(opts) -> max(Keyword.get(opts, :tick_ms, @default_tick_ms), 100)
      _ -> @default_tick_ms
    end
  end

  defp schedule_tick(ms), do: Process.send_after(self(), :tick, ms)

  defp next_tick_delay_ms(base_tick_ms) do
    now = DateTime.utc_now()
    ms_to_next_minute = 60_000 - now.second * 1_000 - div(elem(now.microsecond, 0), 1_000)
    max(100, min(base_tick_ms, ms_to_next_minute))
  end

  defp floor_kind(dt, :hour), do: %{dt | minute: 0, second: 0, microsecond: {0, 0}}

  defp floor_kind(dt, :day), do: %{dt | hour: 0, minute: 0, second: 0, microsecond: {0, 0}}

  defp floor_kind(dt, :month),
    do: %{dt | day: 1, hour: 0, minute: 0, second: 0, microsecond: {0, 0}}

  defp shift_kind(dt, :hour), do: DateTime.add(dt, 3600, :second)
  defp shift_kind(dt, :day), do: DateTime.add(dt, 1, :day)

  defp shift_kind(%DateTime{} = dt, :month) do
    date = DateTime.to_date(dt)
    total = date.year * 12 + date.month
    year = div(total, 12)
    month = rem(total, 12) + 1
    {:ok, new_date} = Date.new(year, month, 1)
    {:ok, naive} = NaiveDateTime.new(new_date, ~T[00:00:00.000000])
    DateTime.from_naive!(naive, dt.time_zone)
  end
end
