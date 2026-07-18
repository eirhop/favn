defmodule FavnOrchestrator.Operator.Schedules do
  @moduledoc """
  Provides operator schedule queries, activation commands, and occurrence previews.

  Scheduler runtime state and manifest fallback reconciliation stay in the
  orchestrator control plane. The public `FavnOrchestrator` module delegates
  operator-facing schedule operations here.
  """

  alias Favn.Window.Policy
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Queries.PageSchedules
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.ScheduleListEntry
  alias FavnOrchestrator.ScheduleOccurrencePreview
  alias FavnOrchestrator.Scheduler.Cron
  alias FavnOrchestrator.SchedulerEntry

  @list_filter_keys [
    :search,
    :activation_state,
    :runtime_state,
    :pipeline_module,
    :window,
    :limit,
    :offset
  ]

  @type summary :: %{
          total: non_neg_integer(),
          enabled: non_neg_integer(),
          pending_activation: non_neg_integer(),
          disabled: non_neg_integer(),
          needs_review: non_neg_integer(),
          running: non_neg_integer(),
          queued: non_neg_integer()
        }

  @doc "Returns a bounded schedule page from the active PostgreSQL deployment."
  @spec page_entries(WorkspaceContext.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def page_entries(%WorkspaceContext{} = context, filters) when is_list(filters) do
    with :ok <- validate_options(filters, @list_filter_keys, :invalid_schedule_list_filters),
         {:ok, runtime} <- ManifestStore.get_runtime_state(context),
         {:ok, page} <-
           Persistence.stores().scheduler.page_schedules(%PageSchedules{
             workspace_context: context,
             limit: min(Keyword.get(filters, :limit, 100), 500)
           }) do
      entries =
        page.items
        |> Enum.map(fn schedule ->
          entry = persisted_scheduler_entry(schedule, runtime)
          ScheduleListEntry.from_scheduler_entry(persisted_entry_id(schedule), entry)
        end)
        |> filter_schedule_list_entries(filters)

      {:ok, %{page | items: entries}}
    end
  end

  @doc "Returns one exact active-deployment schedule under a workspace authority."
  @spec get_entry(WorkspaceContext.t(), String.t()) ::
          {:ok, SchedulerEntry.t()} | {:error, term()}
  def get_entry(%WorkspaceContext{} = context, schedule_id) when is_binary(schedule_id) do
    with {:ok, pipeline_target_id, persisted_schedule_id} <- parse_entry_id(schedule_id),
         {:ok, runtime} <- ManifestStore.get_runtime_state(context),
         {:ok, page} <-
           Persistence.stores().scheduler.page_schedules(%PageSchedules{
             workspace_context: context,
             pipeline_target_id: pipeline_target_id,
             schedule_id: persisted_schedule_id,
             limit: 1
           }),
         [schedule] <- page.items do
      {:ok, persisted_scheduler_entry(schedule, runtime)}
    else
      [] -> {:error, :schedule_not_found}
      {:error, _reason} = error -> error
      _invalid -> {:error, :schedule_not_found}
    end
  end

  @doc "Previews upcoming occurrences for one active workspace schedule."
  @spec preview_occurrences(WorkspaceContext.t(), String.t(), keyword()) ::
          {:ok, [ScheduleOccurrencePreview.t()]} | {:error, term()}
  def preview_occurrences(%WorkspaceContext{} = context, schedule_id, opts)
      when is_binary(schedule_id) and is_list(opts) do
    with :ok <-
           validate_options(opts, [:limit, :now], :invalid_schedule_occurrence_preview_options),
         {:ok, limit} <- normalize_occurrence_preview_limit(opts),
         {:ok, now} <- normalize_occurrence_preview_now(opts),
         {:ok, %SchedulerEntry{} = entry} <- get_entry(context, schedule_id),
         :ok <- validate_preview_schedule(entry) do
      occurrences =
        entry.cron
        |> preview_due_ats(entry.timezone, now, limit)
        |> Enum.map(&schedule_occurrence_preview(schedule_id, entry, &1))

      {:ok, occurrences}
    end
  end

  @doc "Returns the stable control-plane id for a schedule entry."
  @spec entry_id(SchedulerEntry.t()) :: String.t()
  def entry_id(%SchedulerEntry{} = entry) do
    pipeline_module = entry.pipeline_module || Favn.Unknown
    schedule_name = entry.schedule_id || :unknown

    "schedule:" <> Atom.to_string(pipeline_module) <> ":" <> Atom.to_string(schedule_name)
  end

  defp persisted_scheduler_entry(schedule, runtime) do
    definition = schedule.definition || %{}
    cursor = schedule.cursor || %{}
    in_flight_run_id = value(cursor, :in_flight_run_id)
    queued_due_at = datetime_value(cursor, :queued_due_at)

    %SchedulerEntry{
      pipeline_module: existing_module(value(definition, :pipeline_module)),
      schedule_id: existing_atom(value(definition, :schedule_name) || schedule.schedule_id),
      cron: value(definition, :cron),
      timezone: value(definition, :timezone),
      overlap: existing_atom(value(definition, :overlap)),
      missed: existing_atom(value(definition, :missed)),
      active: true,
      activation_state: :enabled,
      effective_enabled?: true,
      runtime_state: persisted_runtime_state(in_flight_run_id, queued_due_at),
      window: persisted_window(value(definition, :window)),
      schedule_fingerprint: schedule.schedule_fingerprint,
      manifest_version_id: runtime.manifest_version_id,
      manifest_content_hash: nil,
      last_evaluated_at: datetime_value(cursor, :last_evaluated_at),
      last_due_at: datetime_value(cursor, :last_due_at),
      next_due_at: schedule.next_due_at,
      last_submitted_due_at: datetime_value(cursor, :last_submitted_due_at),
      in_flight_run_id: in_flight_run_id,
      queued_due_at: queued_due_at,
      last_scheduler_error: value(cursor, :last_scheduler_error),
      updated_at: schedule.updated_at
    }
  end

  defp persisted_entry_id(schedule) do
    target = Base.url_encode64(schedule.pipeline_target_id, padding: false)
    name = Base.url_encode64(schedule.schedule_id, padding: false)
    "schedule-v2:" <> target <> ":" <> name
  end

  defp parse_entry_id("schedule-v2:" <> encoded) do
    with [target, name] <- String.split(encoded, ":", parts: 2),
         {:ok, target} <- Base.url_decode64(target, padding: false),
         {:ok, name} <- Base.url_decode64(name, padding: false),
         true <- target != "" and name != "" do
      {:ok, target, name}
    else
      _invalid -> {:error, :schedule_not_found}
    end
  end

  defp parse_entry_id(_schedule_id), do: {:error, :schedule_not_found}

  defp existing_module(value) when is_binary(value) do
    module = String.to_existing_atom(value)
    if String.starts_with?(value, "Elixir."), do: module, else: nil
  rescue
    ArgumentError -> nil
  end

  defp existing_module(_value), do: nil

  defp existing_atom(value) when is_binary(value) do
    String.to_existing_atom(value)
  rescue
    ArgumentError -> nil
  end

  defp existing_atom(value) when is_atom(value), do: value
  defp existing_atom(_value), do: nil

  defp persisted_runtime_state(run_id, _queued_due_at) when is_binary(run_id), do: :running
  defp persisted_runtime_state(_run_id, %DateTime{}), do: :queued
  defp persisted_runtime_state(_run_id, _queued_due_at), do: :idle

  defp persisted_window(nil), do: nil

  defp persisted_window(value) when is_map(value) do
    case Policy.from_value(value) do
      {:ok, policy} -> policy
      {:error, _reason} -> nil
    end
  end

  defp persisted_window(_value), do: nil

  defp datetime_value(map, key) do
    case value(map, key) do
      %DateTime{} = datetime -> datetime
      encoded when is_binary(encoded) -> parse_datetime(encoded)
      _value -> nil
    end
  end

  defp parse_datetime(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> datetime
      _error -> nil
    end
  end

  defp value(map, key) when is_map(map),
    do: Map.get(map, key) || Map.get(map, Atom.to_string(key))

  @doc "Summarizes operator schedule-list entries."
  @spec summary([ScheduleListEntry.t()]) :: summary()
  def summary(entries) when is_list(entries) do
    %{
      total: length(entries),
      enabled: Enum.count(entries, &(&1.activation_state == :enabled)),
      pending_activation: Enum.count(entries, &(&1.activation_state == :pending_activation)),
      disabled: Enum.count(entries, &(&1.activation_state == :disabled)),
      needs_review: Enum.count(entries, &(&1.activation_state == :needs_review)),
      running: Enum.count(entries, &(&1.runtime_state == :running)),
      queued: Enum.count(entries, &(&1.runtime_state == :queued))
    }
  end

  defp normalize_occurrence_preview_limit(opts) do
    case Keyword.get(opts, :limit, 10) do
      value when is_integer(value) and value > 0 -> {:ok, min(value, 100)}
      value -> {:error, {:invalid_schedule_occurrence_preview_limit, value}}
    end
  end

  defp normalize_occurrence_preview_now(opts) do
    case Keyword.get(opts, :now, DateTime.utc_now()) do
      %DateTime{} = now -> {:ok, now}
      value -> {:error, {:invalid_schedule_occurrence_preview_now, value}}
    end
  end

  defp validate_preview_schedule(%SchedulerEntry{cron: cron, timezone: timezone})
       when is_binary(cron) and is_binary(timezone),
       do: :ok

  defp validate_preview_schedule(%SchedulerEntry{}), do: {:error, :schedule_preview_unavailable}

  defp preview_due_ats(cron, timezone, now, limit) do
    do_preview_due_ats(cron, timezone, now, limit, [])
  end

  defp do_preview_due_ats(_cron, _timezone, _after_at, 0, acc), do: Enum.reverse(acc)

  defp do_preview_due_ats(cron, timezone, after_at, remaining, acc) do
    case Cron.next_due(cron, timezone, after_at) do
      %DateTime{} = due_at ->
        do_preview_due_ats(cron, timezone, DateTime.add(due_at, 1, :second), remaining - 1, [
          due_at | acc
        ])

      nil ->
        Enum.reverse(acc)
    end
  end

  defp schedule_occurrence_preview(schedule_id, entry, due_at) do
    {window, window_error} = occurrence_window(entry.window, due_at, entry.timezone)

    %ScheduleOccurrencePreview{
      schedule_entry_id: schedule_id,
      due_at: due_at,
      timezone: entry.timezone,
      window: window,
      status: occurrence_status(entry, due_at, window_error),
      notes: occurrence_notes(entry, window_error)
    }
  end

  defp occurrence_window(nil, _due_at, _timezone), do: {nil, nil}

  defp occurrence_window(%Policy{} = policy, due_at, timezone) do
    with {:ok, policy} <- Policy.validate(policy),
         {:ok, window} <- Policy.resolve_scheduled(policy, due_at, timezone) do
      {window_to_map(window), nil}
    else
      {:error, reason} -> {nil, reason}
    end
  end

  defp occurrence_window(_window, _due_at, _timezone), do: {nil, :invalid_scheduler_window}

  defp window_to_map(window) when is_map(window) do
    %{
      kind: Map.get(window, :kind),
      start_at: Map.get(window, :start_at),
      end_at: Map.get(window, :end_at),
      timezone: Map.get(window, :timezone),
      key: Map.get(window, :key),
      anchor_key: Map.get(window, :anchor_key)
    }
  end

  defp occurrence_status(_entry, _due_at, window_error) when not is_nil(window_error),
    do: :blocked

  defp occurrence_status(%SchedulerEntry{effective_enabled?: false}, _due_at, _window_error),
    do: :disabled

  defp occurrence_status(
         %SchedulerEntry{queued_due_at: %DateTime{} = queued_due_at},
         due_at,
         _window_error
       ) do
    if DateTime.compare(queued_due_at, due_at) == :eq, do: :queued, else: :upcoming
  end

  defp occurrence_status(
         %SchedulerEntry{
           in_flight_run_id: run_id,
           last_submitted_due_at: %DateTime{} = submitted_at
         },
         due_at,
         _window_error
       )
       when is_binary(run_id) do
    if DateTime.compare(submitted_at, due_at) == :eq, do: :running, else: :upcoming
  end

  defp occurrence_status(_entry, _due_at, _window_error), do: :upcoming

  defp occurrence_notes(entry, window_error) do
    []
    |> maybe_note(entry.effective_enabled? == false, "Will not submit until enabled")
    |> maybe_note(entry.activation_state == :needs_review, "Schedule change needs review")
    |> maybe_note(
      not is_nil(window_error),
      "Window preview unavailable: #{inspect(window_error)}"
    )
    |> Enum.reverse()
  end

  defp maybe_note(notes, true, note), do: [note | notes]
  defp maybe_note(notes, false, _note), do: notes

  defp filter_schedule_list_entries(entries, filters) do
    Enum.filter(entries, fn entry ->
      schedule_matches_search?(entry, Keyword.get(filters, :search)) and
        schedule_matches_atom?(entry.activation_state, Keyword.get(filters, :activation_state)) and
        schedule_matches_atom?(entry.runtime_state, Keyword.get(filters, :runtime_state)) and
        schedule_matches_pipeline?(entry, Keyword.get(filters, :pipeline_module)) and
        schedule_matches_window?(entry, Keyword.get(filters, :window))
    end)
  end

  defp schedule_matches_search?(_entry, value) when value in [nil, "", :all], do: true

  defp schedule_matches_search?(entry, value) do
    search = value |> to_string() |> String.downcase() |> String.trim()

    [entry.id, inspect(entry.pipeline_module), inspect(entry.schedule_id)]
    |> Enum.join(" ")
    |> String.downcase()
    |> String.contains?(search)
  end

  defp schedule_matches_atom?(_actual, value) when value in [nil, "", :all, "all"], do: true
  defp schedule_matches_atom?(actual, value) when is_atom(value), do: actual == value
  defp schedule_matches_atom?(actual, value), do: to_string(actual) == to_string(value)

  defp schedule_matches_pipeline?(_entry, value) when value in [nil, "", :all, "all"], do: true

  defp schedule_matches_pipeline?(entry, value),
    do: inspect(entry.pipeline_module) == to_string(value)

  defp schedule_matches_window?(_entry, value) when value in [nil, "", :all, "all"], do: true
  defp schedule_matches_window?(%{window: nil}, value), do: to_string(value) == "none"

  defp schedule_matches_window?(%{window: %{kind: kind}}, value),
    do: to_string(kind) == to_string(value)

  defp schedule_matches_window?(%{window: window}, value), do: inspect(window) == to_string(value)

  defp validate_options(opts, allowed_keys, error) do
    with true <- Keyword.keyword?(opts),
         [] <- Keyword.keys(opts) -- allowed_keys do
      :ok
    else
      false -> {:error, error}
      unsupported_keys -> {:error, {error, Enum.uniq(unsupported_keys)}}
    end
  end
end
