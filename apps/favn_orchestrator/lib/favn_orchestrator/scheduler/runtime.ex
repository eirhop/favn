defmodule FavnOrchestrator.Scheduler.Runtime do
  @moduledoc false
  use GenServer
  require Logger

  alias Favn.Manifest.Index
  alias Favn.Manifest.PipelineResolver
  alias Favn.Scheduler.State
  alias Favn.Window.Policy
  alias FavnOrchestrator.BoundedDispatcher
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.OperationalEvents
  alias FavnOrchestrator.Scheduler.Cron
  alias FavnOrchestrator.Scheduler.ManifestEntries
  alias FavnOrchestrator.SchedulerError
  alias FavnOrchestrator.SchedulerEntry
  alias FavnOrchestrator.Storage

  @default_tick_ms 15_000
  @default_max_missed_all_occurrences 1_000
  @default_submission_budget 25

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def reload(server \\ __MODULE__), do: GenServer.call(server, :reload, :infinity)
  def tick(server \\ __MODULE__), do: GenServer.call(server, :tick, :infinity)
  def scheduled(server \\ __MODULE__), do: GenServer.call(server, :scheduled, :infinity)
  def diagnostics(server \\ __MODULE__), do: GenServer.call(server, :diagnostics, :infinity)

  def inspect_entries(server \\ __MODULE__),
    do: GenServer.call(server, :inspect_entries, :infinity)

  @impl true
  def init(opts) do
    tick_ms = max(Keyword.get(opts, :tick_ms, configured_tick_ms()), 100)
    auto_tick? = Keyword.get(opts, :auto_tick?, true)

    case load_runtime(tick_ms, auto_tick?) do
      {:ok, state} ->
        if state.auto_tick?, do: schedule_tick(next_tick_delay_ms(state.tick_ms))
        emit_scheduler_loaded(state)
        {:ok, state}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_call(:reload, _from, state) do
    with {:ok, state} <- flush_dirty_states(state),
         {:ok, next} <- load_runtime(state.tick_ms, state.auto_tick?) do
      emit_scheduler_loaded(next)
      {:reply, :ok, next}
    else
      {:error, reason, next} ->
        {:reply, {:error, reason}, next}

      {:error, reason} ->
        {:reply, {:error, reason}, failed_runtime_state(state, reason)}
    end
  end

  def handle_call(:tick, _from, state) do
    case evaluate_all(state) do
      {:ok, next} -> {:reply, :ok, next}
      {:error, reason, next} -> {:reply, {:error, reason}, next}
    end
  end

  def handle_call(:scheduled, _from, state), do: {:reply, Map.values(state.entries), state}

  def handle_call(:diagnostics, _from, state),
    do: {:reply, {:ok, diagnostics_payload(state)}, state}

  def handle_call(:inspect_entries, _from, state) do
    now = DateTime.utc_now()

    entries =
      state.entries
      |> Enum.map(fn {pipeline_module, entry} ->
        entry = Map.put(entry, :next_due_at, next_due_at(entry, now))
        SchedulerEntry.from_runtime(entry, Map.get(state.states, pipeline_module))
      end)
      |> Enum.sort_by(&{inspect(&1.pipeline_module), inspect(&1.schedule_id)})

    {:reply, entries, state}
  end

  defp failed_runtime_state(state, reason) do
    %{
      entries: %{},
      states: %{},
      dirty_states: %{},
      last_persist_error: persist_error(reason),
      tick_ms: state.tick_ms,
      auto_tick?: state.auto_tick?,
      index: nil,
      version: nil
    }
  end

  defp empty_runtime_state(tick_ms, auto_tick?) do
    %{
      entries: %{},
      states: %{},
      dirty_states: %{},
      last_persist_error: nil,
      tick_ms: tick_ms,
      auto_tick?: auto_tick?,
      index: nil,
      version: nil
    }
  end

  @impl true
  def handle_info(:tick, state) do
    next =
      case evaluate_all(state) do
        {:ok, next} ->
          next

        {:error, reason, next} ->
          Logger.error("scheduler tick failed", reason: inspect(reason))
          next
      end

    if next.auto_tick?, do: schedule_tick(next_tick_delay_ms(next.tick_ms))
    {:noreply, next}
  end

  defp load_runtime(tick_ms, auto_tick?) do
    with {:ok, version, index} <- load_active_manifest_index(),
         {:ok, entries} <- ManifestEntries.discover(version, index),
         {:ok, states} <- load_states(entries) do
      {:ok,
       %{
         entries: entries,
         states: states,
         dirty_states: %{},
         last_persist_error: nil,
         tick_ms: tick_ms,
         auto_tick?: auto_tick?,
         index: index,
         version: version
       }}
    else
      {:empty, reason} when reason in [:active_manifest_not_set, :manifest_version_not_found] ->
        {:ok, empty_runtime_state(tick_ms, auto_tick?)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp diagnostics_payload(state) do
    states = Map.values(state.states)

    %{
      running?: true,
      manifest_version_id: manifest_version_id(state.version),
      entry_count: map_size(state.entries),
      active_schedule_count: count_entries(state.entries, fn entry -> entry.schedule.active end),
      inactive_schedule_count:
        count_entries(state.entries, fn entry -> not entry.schedule.active end),
      in_flight_schedule_count: Enum.count(states, &is_binary(Map.get(&1, :in_flight_run_id))),
      queued_schedule_count: Enum.count(states, &(not is_nil(Map.get(&1, :queued_due_at)))),
      dirty_scheduler_state_count: map_size(Map.get(state, :dirty_states, %{})),
      last_scheduler_persist_error: Map.get(state, :last_persist_error),
      state_summary: scheduler_state_summary(state),
      tick_ms: state.tick_ms,
      auto_tick?: state.auto_tick?
    }
  end

  defp scheduler_state_summary(state) do
    entries =
      state.entries
      |> Map.values()
      |> Enum.map(&scheduler_state_entry(&1, Map.get(state.states, &1.module), state.version))
      |> Enum.sort_by(&{to_string(&1.schedule_id), &1.id})

    %{
      state_count: map_size(state.states),
      evaluated_count: count_states(state.states, &present?(&1.last_evaluated_at)),
      due_cursor_count: count_states(state.states, &present?(&1.last_due_at)),
      submitted_cursor_count: count_states(state.states, &present?(&1.last_submitted_due_at)),
      in_flight_count: count_states(state.states, &is_binary(&1.in_flight_run_id)),
      queued_count: count_states(state.states, &present?(&1.queued_due_at)),
      dirty_count: map_size(Map.get(state, :dirty_states, %{})),
      updated_count: count_states(state.states, &present?(&1.updated_at)),
      entries: entries
    }
  end

  defp scheduler_state_entry(entry, state, version) do
    schedule_id = entry.schedule.name

    %{
      id:
        scheduler_state_entry_id(
          version,
          entry.module,
          entry.id,
          schedule_id,
          entry.schedule_fingerprint
        ),
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

  defp scheduler_state_entry_id(
         version,
         entry_module,
         entry_id,
         schedule_id,
         schedule_fingerprint
       ) do
    source = [
      manifest_version_id(version),
      inspect(entry_module),
      to_string(entry_id),
      to_string(schedule_id),
      schedule_fingerprint || ""
    ]

    digest =
      :sha256
      |> :crypto.hash(Enum.join(source, ":"))
      |> Base.url_encode64(padding: false)
      |> binary_part(0, 16)

    "scheduler_state:" <> digest
  end

  defp count_states(states, fun) when is_map(states) and is_function(fun, 1) do
    states
    |> Map.values()
    |> Enum.count(fun)
  end

  defp present?(nil), do: false
  defp present?(_value), do: true

  defp field(nil, _key), do: nil
  defp field(state, key) when is_map(state), do: Map.get(state, key)

  defp emit_scheduler_loaded(state) do
    OperationalEvents.emit(:scheduler_loaded, %{entry_count: map_size(state.entries)}, %{
      manifest_version_id: manifest_version_id(state.version),
      auto_tick?: state.auto_tick?
    })
  end

  defp count_entries(entries, fun) when is_map(entries) and is_function(fun, 1) do
    entries
    |> Map.values()
    |> Enum.count(fun)
  end

  defp manifest_version_id(nil), do: nil
  defp manifest_version_id(%{manifest_version_id: manifest_version_id}), do: manifest_version_id

  defp load_active_manifest_index do
    with {:ok, manifest_version_id} <- ManifestStore.get_active_manifest(),
         {:ok, version} <- ManifestStore.get_manifest(manifest_version_id),
         {:ok, index} <- Index.build_from_version(version) do
      {:ok, version, index}
    else
      {:error, reason} when reason in [:active_manifest_not_set, :manifest_version_not_found] ->
        {:empty, reason}

      {:error, _reason} = error ->
        error
    end
  end

  defp load_states(entries) do
    now = DateTime.utc_now()

    Enum.reduce_while(entries, {:ok, %{}}, fn {pipeline_module, entry}, {:ok, acc} ->
      key = {pipeline_module, entry.schedule.name}

      case Storage.get_scheduler_state(key) do
        {:ok, nil} ->
          base = %State{
            pipeline_module: pipeline_module,
            schedule_id: entry.schedule.name,
            schedule_fingerprint: entry.schedule_fingerprint,
            activation_state: :pending_activation,
            updated_at: now,
            version: 1
          }

          case Storage.put_scheduler_state(key, base) do
            :ok -> {:cont, {:ok, Map.put(acc, pipeline_module, base)}}
            {:error, reason} -> {:halt, {:error, reason}}
          end

        {:ok, %State{} = stored} ->
          stored = normalize_scheduler_state(stored)

          value =
            if stored.schedule_fingerprint == entry.schedule_fingerprint do
              stored
              |> bootstrap_activation_state(entry)
              |> Map.put(:schedule_id, entry.schedule.name)
            else
              %State{
                pipeline_module: pipeline_module,
                schedule_id: entry.schedule.name,
                schedule_fingerprint: entry.schedule_fingerprint,
                activation_state: :needs_review,
                updated_at: now
              }
            end

          if stored.schedule_fingerprint != entry.schedule_fingerprint or
               stored.activation_state != value.activation_state do
            persisted = persisted_scheduler_state(stored, value)

            case Storage.put_scheduler_state(key, persisted) do
              :ok -> {:cont, {:ok, Map.put(acc, pipeline_module, persisted)}}
              {:error, reason} -> {:halt, {:error, reason}}
            end
          else
            {:cont, {:ok, Map.put(acc, pipeline_module, value)}}
          end

        {:ok, stored} when is_map(stored) ->
          state = normalize_scheduler_state(stored)
          value = bootstrap_activation_state(state, entry)

          if state.activation_state != value.activation_state do
            persisted = persisted_scheduler_state(state, value)

            case Storage.put_scheduler_state(key, persisted) do
              :ok -> {:cont, {:ok, Map.put(acc, pipeline_module, persisted)}}
              {:error, reason} -> {:halt, {:error, reason}}
            end
          else
            {:cont, {:ok, Map.put(acc, pipeline_module, value)}}
          end

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
  end

  defp evaluate_all(%{index: nil} = state), do: flush_dirty_states(state)

  defp evaluate_all(state) do
    with {:ok, state} <- flush_dirty_states(state) do
      do_evaluate_all(state)
    end
  end

  defp do_evaluate_all(state) do
    now = DateTime.utc_now()

    result =
      Enum.reduce_while(state.entries, {:ok, state.states, configured_submission_budget()}, fn
        {pipeline_module, entry}, {:ok, acc, remaining_budget} ->
          current = Map.get(acc, pipeline_module, %State{pipeline_module: pipeline_module})

          {updated, remaining_budget} =
            cond do
              not effective_enabled?(entry, current) ->
                {%{current | last_evaluated_at: now, updated_at: now}, remaining_budget}

              remaining_budget <= 0 ->
                {%{current | last_evaluated_at: now, updated_at: now}, remaining_budget}

              true ->
                evaluate_entry(entry, state.index, state.version, current, now, remaining_budget)
            end

          next_acc =
            if persist_state_change?(current, updated) do
              persisted = persisted_scheduler_state(current, updated)
              key = {pipeline_module, entry.schedule.name}

              case Storage.put_scheduler_state(key, persisted) do
                :ok ->
                  Map.put(acc, pipeline_module, persisted)

                {:error, reason} ->
                  {:error, reason, Map.put(acc, pipeline_module, persisted), pipeline_module,
                   persisted}
              end
            else
              Map.put(acc, pipeline_module, updated)
            end

          case next_acc do
            {:error, reason, failed_acc, dirty_key, dirty_state} ->
              {:halt, {:error, reason, failed_acc, dirty_key, dirty_state}}

            next_acc ->
              {:cont, {:ok, next_acc, remaining_budget}}
          end
      end)

    case result do
      {:ok, next_states, _remaining_budget} ->
        {:ok, %{state | states: next_states, last_persist_error: nil}}

      {:error, reason, next_states, dirty_key, dirty_state} ->
        next = %{
          state
          | states: next_states,
            dirty_states: Map.put(state.dirty_states, dirty_key, dirty_state),
            last_persist_error: persist_error(reason)
        }

        {:error, reason, next}
    end
  end

  defp flush_dirty_states(state) do
    dirty_states = Map.get(state, :dirty_states, %{})

    Enum.reduce_while(dirty_states, {:ok, state}, fn {pipeline_module, dirty_state}, {:ok, acc} ->
      case Storage.put_scheduler_state({pipeline_module, dirty_state.schedule_id}, dirty_state) do
        :ok ->
          {:cont,
           {:ok,
            %{
              acc
              | states: Map.put(acc.states, pipeline_module, dirty_state),
                dirty_states: Map.delete(acc.dirty_states, pipeline_module),
                last_persist_error: nil
            }}}

        {:error, reason} ->
          {:halt, {:error, reason, %{acc | last_persist_error: persist_error(reason)}}}
      end
    end)
  end

  defp persist_error(reason), do: %{reason: inspect(reason), occurred_at: DateTime.utc_now()}

  defp evaluate_entry(entry, index, version, state, now, remaining_budget) do
    state = reconcile_in_flight(state)
    latest_due = Cron.latest_due(entry.schedule.cron, entry.schedule.timezone, now)

    cond do
      is_nil(latest_due) ->
        error = SchedulerError.new(:compute_due, :invalid_cron_or_timezone, now)

        {%{state | last_evaluated_at: now, last_scheduler_error: error, updated_at: now},
         remaining_budget}

      is_nil(state.last_due_at) ->
        {%{state | last_due_at: latest_due, last_evaluated_at: now, updated_at: now},
         remaining_budget}

      true ->
        selected =
          select_occurrences(
            entry,
            entry.schedule.missed,
            entry.schedule.cron,
            entry.schedule.timezone,
            state.last_due_at,
            latest_due
          )

        {next_state, submitted_due_ats_reversed, remaining_budget} =
          state
          |> maybe_submit_queued(entry, index, version, latest_due, now, remaining_budget)
          |> submit_due_occurrences(entry, index, version, selected, latest_due, now)

        submitted_due_ats = Enum.reverse(submitted_due_ats_reversed)

        next_state =
          next_state
          |> Map.put(
            :last_due_at,
            next_cursor_due(
              entry.schedule.missed,
              state.last_due_at,
              latest_due,
              selected,
              submitted_due_ats
            )
          )
          |> Map.put(:last_evaluated_at, now)
          |> Map.put(:updated_at, now)

        {next_state, remaining_budget}
    end
  end

  defp maybe_submit_queued(
         state,
         _entry,
         _index,
         _version,
         _latest_due,
         _now,
         remaining_budget
       )
       when remaining_budget <= 0,
       do: {state, [], remaining_budget}

  defp maybe_submit_queued(
         %State{queued_due_at: nil} = state,
         _entry,
         _index,
         _version,
         _latest_due,
         _now,
         remaining_budget
       ),
       do: {state, [], remaining_budget}

  defp maybe_submit_queued(
         %State{in_flight_run_id: run_id} = state,
         _entry,
         _index,
         _version,
         _latest_due,
         _now,
         remaining_budget
       )
       when is_binary(run_id), do: {state, [], remaining_budget}

  defp maybe_submit_queued(state, entry, index, version, latest_due, now, remaining_budget) do
    case submit_occurrence(state, entry, index, version, state.queued_due_at, latest_due, now) do
      {:ok, next} ->
        {%{next | queued_due_at: nil}, [state.queued_due_at], remaining_budget - 1}

      {:error, reason} ->
        {%{state | last_scheduler_error: SchedulerError.new(:submit_run, reason, now)}, [],
         remaining_budget - 1}
    end
  end

  defp submit_due_occurrences(
         {state, submitted_reversed, remaining_budget},
         _entry,
         _index,
         _version,
         [],
         _latest_due,
         _now
       ),
       do: {state, submitted_reversed, remaining_budget}

  defp submit_due_occurrences(
         {state, submitted_reversed, remaining_budget},
         _entry,
         _index,
         _version,
         _occurrences,
         _latest_due,
         _now
       )
       when remaining_budget <= 0,
       do: {state, submitted_reversed, remaining_budget}

  defp submit_due_occurrences(
         {state, submitted_reversed, remaining_budget},
         entry,
         index,
         version,
         [due | rest],
         latest_due,
         now
       ) do
    state = reconcile_in_flight(state)

    case entry.schedule.overlap do
      :allow ->
        {next, submitted_next} =
          case submit_occurrence(state, entry, index, version, due, latest_due, now,
                 track_in_flight?: false
               ) do
            {:ok, value} ->
              {value, [due | submitted_reversed]}

            {:error, reason} ->
              {record_scheduler_error(state, :submit_run, reason, now), submitted_reversed}
          end

        submit_due_occurrences(
          {next, submitted_next, remaining_budget - 1},
          entry,
          index,
          version,
          rest,
          latest_due,
          now
        )

      :forbid ->
        if is_binary(state.in_flight_run_id) do
          {state, submitted_reversed, remaining_budget}
        else
          case submit_occurrence(state, entry, index, version, due, latest_due, now) do
            {:ok, value} ->
              {value, [due | submitted_reversed], remaining_budget - 1}

            {:error, reason} ->
              {record_scheduler_error(state, :submit_run, reason, now), submitted_reversed,
               remaining_budget - 1}
          end
        end

      :queue_one ->
        if is_binary(state.in_flight_run_id) do
          {%{state | queued_due_at: List.last([due | rest])}, submitted_reversed,
           remaining_budget}
        else
          case submit_occurrence(state, entry, index, version, due, latest_due, now) do
            {:ok, value} ->
              next = if rest == [], do: value, else: %{value | queued_due_at: List.last(rest)}
              {next, [due | submitted_reversed], remaining_budget - 1}

            {:error, reason} ->
              {record_scheduler_error(state, :submit_run, reason, now), submitted_reversed,
               remaining_budget - 1}
          end
        end
    end
  end

  defp submit_occurrence(state, entry, index, version, due_at, latest_due, now, opts \\ []) do
    track_in_flight? = Keyword.get(opts, :track_in_flight?, true)
    trigger = build_trigger(entry, due_at, latest_due, now)

    with {:ok, anchor_window} <-
           maybe_anchor_window(entry.window, due_at, entry.schedule.timezone),
         {:ok, resolution} <-
           PipelineResolver.resolve(index, entry.pipeline, resolve_opts(trigger, anchor_window)),
         {:ok, run_id} <-
           BoundedDispatcher.run(fn ->
             FavnOrchestrator.submit_pipeline_run(resolution.target_refs,
               manifest_version_id: version.manifest_version_id,
               trigger: trigger,
               dependencies: resolution.dependencies,
               anchor_window: anchor_window,
               _pipeline_context: resolution.pipeline_ctx,
               _submit_ref: entry.module,
               _submit_kind: :pipeline
             )
           end) do
      next =
        if track_in_flight? do
          %{
            state
            | in_flight_run_id: run_id,
              last_submitted_due_at: due_at,
              last_scheduler_error: nil,
              updated_at: now
          }
        else
          %{state | last_submitted_due_at: due_at, last_scheduler_error: nil, updated_at: now}
        end

      {:ok, next}
    else
      {:error, reason} ->
        Logger.warning("scheduler submit failed for #{inspect(entry.module)}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp resolve_opts(trigger, nil), do: [trigger: trigger]
  defp resolve_opts(trigger, anchor_window), do: [trigger: trigger, anchor_window: anchor_window]

  defp build_trigger(entry, due_at, latest_due, now) do
    %{
      kind: :schedule,
      pipeline: %{module: entry.module, id: entry.id},
      schedule: %{
        id: entry.schedule.name,
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
  end

  defp maybe_anchor_window(nil, _due_at, _timezone), do: {:ok, nil}

  defp maybe_anchor_window(%Policy{} = policy, due_at, timezone) do
    with {:ok, policy} <- Policy.validate(policy),
         {:ok, anchor_window} <- Policy.resolve_scheduled(policy, due_at, timezone) do
      {:ok, anchor_window}
    else
      {:error, reason} -> {:error, {:invalid_scheduled_window_policy, reason}}
    end
  end

  defp maybe_anchor_window(window, _due_at, _timezone) do
    {:error, {:invalid_scheduled_window_policy, {:invalid_scheduler_window, window}}}
  end

  defp select_occurrences(entry, :all, cron, timezone, last_due_at, latest_due) do
    limit = configured_max_missed_all_occurrences()
    selected = Cron.occurrences_between(cron, timezone, last_due_at, latest_due, limit: limit + 1)

    if length(selected) > limit do
      Logger.warning(
        "scheduler missed occurrence catch-up capped " <>
          "pipeline=#{inspect(entry.module)} " <>
          "schedule_id=#{inspect(entry.schedule.name)} " <>
          "schedule_ref=#{inspect(entry.schedule.ref)} " <>
          "cron=#{inspect(cron)} " <>
          "cap=#{limit} selected=#{limit} observed=#{length(selected)}"
      )

      Enum.take(selected, limit)
    else
      selected
    end
  end

  defp select_occurrences(_entry, :skip, cron, timezone, last_due_at, latest_due) do
    case Cron.last_occurrence_between(cron, timezone, last_due_at, latest_due) do
      nil -> []
      due_at -> [due_at]
    end
  end

  defp select_occurrences(_entry, :one, cron, timezone, last_due_at, latest_due) do
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
      previous.activation_state != next.activation_state or
      previous.last_due_at != next.last_due_at or
      previous.last_submitted_due_at != next.last_submitted_due_at or
      previous.in_flight_run_id != next.in_flight_run_id or
      previous.queued_due_at != next.queued_due_at or
      previous.last_scheduler_error != next.last_scheduler_error
  end

  defp reconcile_in_flight(%State{in_flight_run_id: nil} = state), do: state

  defp reconcile_in_flight(%State{in_flight_run_id: run_id} = state) do
    case Storage.get_run(run_id) do
      {:ok, %{status: :running}} -> state
      {:ok, _terminal} -> %{state | in_flight_run_id: nil}
      {:error, :not_found} -> %{state | in_flight_run_id: nil}
      {:error, _reason} -> state
    end
  end

  defp occurrence_key(module, %DateTime{} = due_at),
    do: "schedule:" <> Atom.to_string(module) <> ":" <> DateTime.to_iso8601(due_at)

  defp normalize_scheduler_state(%State{} = state) do
    state
    |> Map.from_struct()
    |> then(&struct(State, &1))
  end

  defp normalize_scheduler_state(state) when is_map(state) do
    allowed = [
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

    state
    |> Map.take(allowed)
    |> then(&struct(State, &1))
  end

  defp persisted_scheduler_state(previous, %State{} = next) do
    %{next | version: next_scheduler_version(previous)}
  end

  defp next_scheduler_version(%State{version: version}) when is_integer(version) and version > 0,
    do: version + 1

  defp next_scheduler_version(_previous), do: 1

  defp bootstrap_activation_state(%State{activation_state: nil} = state, entry) do
    %{state | activation_state: initial_activation_state(state, entry)}
  end

  defp bootstrap_activation_state(%State{} = state, _entry), do: state

  defp initial_activation_state(%State{} = state, entry) do
    has_runtime_cursor? =
      not is_nil(state.last_due_at) or not is_nil(state.last_submitted_due_at) or
        not is_nil(state.in_flight_run_id) or not is_nil(state.queued_due_at)

    cond do
      entry.schedule.active != true -> :disabled
      has_runtime_cursor? -> :enabled
      true -> :pending_activation
    end
  end

  defp effective_enabled?(entry, %State{} = state),
    do: entry.schedule.active == true and state.activation_state == :enabled

  defp record_scheduler_error(%State{} = state, phase, reason, %DateTime{} = now) do
    %{state | last_scheduler_error: SchedulerError.new(phase, reason, now), updated_at: now}
  end

  defp next_due_at(%{schedule: %{cron: cron, timezone: timezone}}, %DateTime{} = now)
       when is_binary(cron) and is_binary(timezone),
       do: Cron.next_due(cron, timezone, now)

  defp next_due_at(_entry, _now), do: nil

  defp configured_tick_ms do
    case Application.get_env(:favn_orchestrator, :scheduler, []) do
      opts when is_list(opts) -> Keyword.get(opts, :tick_ms, @default_tick_ms)
      _ -> @default_tick_ms
    end
  end

  defp configured_max_missed_all_occurrences do
    case Application.get_env(:favn_orchestrator, :scheduler, []) do
      opts when is_list(opts) ->
        case Keyword.get(opts, :max_missed_all_occurrences, @default_max_missed_all_occurrences) do
          value when is_integer(value) and value > 0 -> value
          _other -> @default_max_missed_all_occurrences
        end

      _other ->
        @default_max_missed_all_occurrences
    end
  end

  defp configured_submission_budget do
    case Application.get_env(:favn_orchestrator, :scheduler, []) do
      opts when is_list(opts) ->
        case Keyword.get(opts, :submission_budget, @default_submission_budget) do
          value when is_integer(value) and value > 0 -> value
          _other -> @default_submission_budget
        end

      _other ->
        @default_submission_budget
    end
  end

  defp schedule_tick(ms), do: Process.send_after(self(), :tick, ms)

  defp next_tick_delay_ms(base_tick_ms) do
    now = DateTime.utc_now()
    ms_to_next_minute = 60_000 - now.second * 1_000 - div(elem(now.microsecond, 0), 1_000)
    max(100, min(base_tick_ms, ms_to_next_minute))
  end
end
