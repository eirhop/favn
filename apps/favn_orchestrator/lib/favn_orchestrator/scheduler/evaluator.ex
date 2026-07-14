defmodule FavnOrchestrator.Scheduler.Evaluator do
  @moduledoc false

  require Logger

  alias Favn.Scheduler.State
  alias Favn.Window.Policy
  alias FavnOrchestrator.BoundedDispatcher
  alias FavnOrchestrator.Redaction
  alias FavnOrchestrator.RunManager
  alias FavnOrchestrator.Scheduler.Cron
  alias FavnOrchestrator.Scheduler.StateStore
  alias FavnOrchestrator.SchedulerError
  alias FavnOrchestrator.Storage

  @default_max_missed_all_occurrences 1_000
  @default_submission_budget 25

  @spec evaluate(map()) :: {:ok, map()} | {:error, term(), map()}
  def evaluate(%{version: nil} = runtime_state), do: StateStore.flush_dirty(runtime_state)

  def evaluate(runtime_state) when is_map(runtime_state) do
    if map_size(runtime_state.dirty_states) > 0 do
      StateStore.flush_dirty(runtime_state)
    else
      evaluate_entries(runtime_state, DateTime.utc_now())
    end
  end

  defp evaluate_entries(runtime_state, now) do
    initial = {:ok, runtime_state.states, configured_submission_budget()}

    result =
      Enum.reduce_while(runtime_state.entries, initial, fn
        {pipeline_module, entry}, {:ok, states, budget} ->
          current = Map.fetch!(states, pipeline_module)
          {updated, remaining_budget} = evaluate_if_enabled(entry, current, now, budget)

          case StateStore.persist_evaluation(current, updated) do
            {:ok, persisted} ->
              {:cont, {:ok, Map.put(states, pipeline_module, persisted), remaining_budget}}

            {:error, reason, dirty_state} ->
              {:halt,
               {:error, reason, Map.put(states, pipeline_module, dirty_state), pipeline_module,
                dirty_state}}
          end
      end)

    evaluation_result(runtime_state, result)
  end

  defp evaluate_if_enabled(entry, current, now, budget) do
    if effective_enabled?(entry, current) and budget > 0 do
      evaluate_entry(entry, current, now, budget)
    else
      {%{current | last_evaluated_at: now, updated_at: now}, budget}
    end
  end

  defp evaluation_result(runtime_state, {:ok, states, _remaining_budget}) do
    {:ok, %{runtime_state | states: states, last_persist_error: nil}}
  end

  defp evaluation_result(
         runtime_state,
         {:error, reason, states, dirty_key, dirty_state}
       ) do
    next = %{
      runtime_state
      | states: states,
        dirty_states: Map.put(runtime_state.dirty_states, dirty_key, dirty_state)
    }

    {:error, reason, StateStore.record_failure(next, reason)}
  end

  defp evaluate_entry(entry, state, now, remaining_budget) do
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
        evaluate_due_occurrences(entry, state, latest_due, now, remaining_budget)
    end
  end

  defp evaluate_due_occurrences(entry, state, latest_due, now, remaining_budget) do
    selected =
      select_occurrences(
        entry,
        entry.schedule.missed,
        entry.schedule.cron,
        entry.schedule.timezone,
        state.last_due_at,
        latest_due
      )

    {next_state, submitted_reversed, remaining_budget} =
      state
      |> maybe_submit_queued(entry, latest_due, now, remaining_budget)
      |> submit_due_occurrences(entry, selected, latest_due, now)

    submitted = Enum.reverse(submitted_reversed)

    next_state = %{
      next_state
      | last_due_at:
          next_cursor_due(
            entry.schedule.missed,
            state.last_due_at,
            latest_due,
            submitted
          ),
        last_evaluated_at: now,
        updated_at: now
    }

    {next_state, remaining_budget}
  end

  defp maybe_submit_queued(state, entry, latest_due, now, remaining_budget) do
    cond do
      remaining_budget <= 0 ->
        {state, [], remaining_budget}

      is_nil(state.queued_due_at) ->
        {state, [], remaining_budget}

      is_binary(state.in_flight_run_id) ->
        {state, [], remaining_budget}

      true ->
        submit_queued(state, entry, latest_due, now, remaining_budget)
    end
  end

  defp submit_queued(state, entry, latest_due, now, remaining_budget) do
    case submit_occurrence(state, entry, state.queued_due_at, latest_due, now) do
      {:ok, next} ->
        {%{next | queued_due_at: nil}, [state.queued_due_at], remaining_budget - 1}

      {:error, reason} ->
        {record_scheduler_error(state, :submit_run, reason, now), [], remaining_budget - 1}
    end
  end

  defp submit_due_occurrences(
         {state, submitted, remaining_budget},
         _entry,
         _occurrences,
         _latest_due,
         _now
       )
       when remaining_budget <= 0,
       do: {state, submitted, remaining_budget}

  defp submit_due_occurrences(result, _entry, [], _latest_due, _now), do: result

  defp submit_due_occurrences(
         {state, submitted, remaining_budget},
         entry,
         [due | rest],
         latest_due,
         now
       ) do
    state = reconcile_in_flight(state)

    context = %{
      entry: entry,
      due: due,
      rest: rest,
      latest_due: latest_due,
      now: now
    }

    {next, submitted, remaining_budget, continue?} =
      submit_for_overlap(
        entry.schedule.overlap,
        state,
        submitted,
        remaining_budget,
        context
      )

    if continue? do
      submit_due_occurrences(
        {next, submitted, remaining_budget},
        entry,
        rest,
        latest_due,
        now
      )
    else
      {next, submitted, remaining_budget}
    end
  end

  defp submit_for_overlap(:allow, state, submitted, budget, context) do
    %{entry: entry, due: due, latest_due: latest_due, now: now} = context

    case submit_occurrence(state, entry, due, latest_due, now, track_in_flight?: false) do
      {:ok, next} ->
        {next, [due | submitted], budget - 1, true}

      {:error, reason} ->
        {record_scheduler_error(state, :submit_run, reason, now), submitted, budget - 1, true}
    end
  end

  defp submit_for_overlap(
         :forbid,
         %State{in_flight_run_id: run_id} = state,
         submitted,
         budget,
         _context
       )
       when is_binary(run_id),
       do: {state, submitted, budget, false}

  defp submit_for_overlap(:forbid, state, submitted, budget, context) do
    %{entry: entry, due: due, latest_due: latest_due, now: now} = context

    case submit_occurrence(state, entry, due, latest_due, now) do
      {:ok, next} ->
        {next, [due | submitted], budget - 1, true}

      {:error, reason} ->
        {record_scheduler_error(state, :submit_run, reason, now), submitted, budget - 1, true}
    end
  end

  defp submit_for_overlap(
         :queue_one,
         %State{in_flight_run_id: run_id} = state,
         submitted,
         budget,
         %{due: due, rest: rest}
       )
       when is_binary(run_id) do
    {%{state | queued_due_at: latest_occurrence(due, rest)}, submitted, budget, false}
  end

  defp submit_for_overlap(:queue_one, state, submitted, budget, context) do
    %{entry: entry, due: due, rest: rest, latest_due: latest_due, now: now} = context

    case submit_occurrence(state, entry, due, latest_due, now) do
      {:ok, next} ->
        next = if rest == [], do: next, else: %{next | queued_due_at: List.last(rest)}
        {next, [due | submitted], budget - 1, true}

      {:error, reason} ->
        {record_scheduler_error(state, :submit_run, reason, now), submitted, budget - 1, true}
    end
  end

  defp submit_for_overlap(overlap, state, submitted, budget, %{now: now}) do
    reason = {:invalid_scheduler_overlap, overlap}
    {record_scheduler_error(state, :evaluate, reason, now), submitted, budget, false}
  end

  defp submit_occurrence(state, entry, due_at, latest_due, now, opts \\ []) do
    track_in_flight? = Keyword.get(opts, :track_in_flight?, true)
    trigger = build_trigger(entry, due_at, latest_due, now)
    occurrence_key = trigger.occurrence.occurrence_key
    run_id = scheduled_run_id(occurrence_key)

    with {:ok, anchor_window} <-
           maybe_anchor_window(entry.window, due_at, entry.schedule.timezone),
         {:ok, run_id} <-
           find_or_submit_occurrence(run_id, occurrence_key, entry, trigger, anchor_window) do
      {:ok, record_submission(state, run_id, due_at, now, track_in_flight?)}
    else
      {:error, reason} ->
        Logger.warning(
          "scheduler submission failed pipeline=#{inspect(entry.module)} " <>
            "reason=#{safe_diagnostic(reason)}"
        )

        {:error, reason}
    end
  end

  defp find_or_submit_occurrence(run_id, occurrence_key, entry, trigger, anchor_window) do
    case Storage.get_run(run_id) do
      {:ok, run} ->
        if persisted_occurrence_key(run) == occurrence_key do
          {:ok, run_id}
        else
          {:error, {:scheduled_run_id_conflict, run_id}}
        end

      {:error, :not_found} ->
        BoundedDispatcher.run(fn ->
          RunManager.submit_pipeline_module_run(entry.module,
            run_id: run_id,
            manifest_version_id: entry.manifest_version_id,
            trigger: trigger,
            anchor_window: anchor_window
          )
        end)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp record_submission(state, run_id, due_at, now, true) do
    %{
      state
      | in_flight_run_id: run_id,
        last_submitted_due_at: due_at,
        last_scheduler_error: nil,
        updated_at: now
    }
  end

  defp record_submission(state, _run_id, due_at, now, false) do
    %{state | last_submitted_due_at: due_at, last_scheduler_error: nil, updated_at: now}
  end

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
        occurrence_key: occurrence_key(entry, due_at),
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

  defp maybe_anchor_window(window, _due_at, _timezone),
    do: {:error, {:invalid_scheduled_window_policy, {:invalid_scheduler_window, window}}}

  defp select_occurrences(entry, :all, cron, timezone, last_due_at, latest_due) do
    limit = configured_max_missed_all_occurrences()
    selected = Cron.occurrences_between(cron, timezone, last_due_at, latest_due, limit: limit + 1)

    if length(selected) > limit do
      Logger.warning(
        "scheduler missed occurrence catch-up capped " <>
          "pipeline=#{inspect(entry.module)} " <>
          "schedule_id=#{inspect(entry.schedule.name)} " <>
          "schedule_ref=#{inspect(entry.schedule.ref)} " <>
          "cron=#{inspect(cron)} cap=#{limit} selected=#{limit} observed=#{length(selected)}"
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

  defp next_cursor_due(_policy, previous_due_at, _latest_due, []), do: previous_due_at
  defp next_cursor_due(:skip, _previous_due_at, latest_due, _submitted), do: latest_due

  defp next_cursor_due(policy, _previous_due_at, _latest_due, submitted)
       when policy in [:one, :all],
       do: List.last(submitted)

  defp reconcile_in_flight(%State{in_flight_run_id: nil} = state), do: state

  defp reconcile_in_flight(%State{in_flight_run_id: run_id} = state) do
    case Storage.get_run(run_id) do
      {:ok, %{status: status}} when status in [:pending, :running] -> state
      {:ok, _terminal} -> %{state | in_flight_run_id: nil}
      {:error, :not_found} -> %{state | in_flight_run_id: nil}
      {:error, _reason} -> state
    end
  end

  defp occurrence_key(entry, %DateTime{} = due_at) do
    [
      "schedule",
      Atom.to_string(entry.module),
      to_string(entry.id),
      entry.schedule_fingerprint,
      DateTime.to_iso8601(due_at)
    ]
    |> Enum.join(":")
  end

  defp scheduled_run_id(occurrence_key) do
    digest = :crypto.hash(:sha256, occurrence_key) |> Base.encode16(case: :lower)
    "run_schedule_" <> digest
  end

  defp persisted_occurrence_key(%{trigger: trigger}) when is_map(trigger) do
    occurrence = Map.get(trigger, :occurrence) || Map.get(trigger, "occurrence") || %{}
    Map.get(occurrence, :occurrence_key) || Map.get(occurrence, "occurrence_key")
  end

  defp persisted_occurrence_key(_run), do: nil

  defp latest_occurrence(due, []), do: due
  defp latest_occurrence(_due, rest), do: List.last(rest)

  defp effective_enabled?(entry, %State{} = state),
    do: entry.schedule.active == true and state.activation_state == :enabled

  defp record_scheduler_error(%State{} = state, phase, reason, %DateTime{} = now) do
    %{state | last_scheduler_error: SchedulerError.new(phase, reason, now), updated_at: now}
  end

  defp configured_max_missed_all_occurrences do
    scheduler_option(
      :max_missed_all_occurrences,
      @default_max_missed_all_occurrences
    )
  end

  defp configured_submission_budget,
    do: scheduler_option(:submission_budget, @default_submission_budget)

  defp scheduler_option(key, default) do
    case Application.get_env(:favn_orchestrator, :scheduler, []) do
      opts when is_list(opts) -> positive_integer(Keyword.get(opts, key), default)
      _other -> default
    end
  end

  defp positive_integer(value, _default) when is_integer(value) and value > 0, do: value
  defp positive_integer(_value, default), do: default

  defp safe_diagnostic(reason) do
    reason
    |> Redaction.redact_operational_bounded()
    |> inspect(limit: 20, printable_limit: 2_000)
  end
end
