defmodule FavnOrchestrator.Operator.Schedules do
  @moduledoc """
  Provides operator schedule queries, activation commands, and occurrence previews.

  Scheduler runtime state and manifest fallback reconciliation stay in the
  orchestrator control plane. The public `FavnOrchestrator` module delegates
  operator-facing schedule operations here.
  """

  alias Favn.Manifest.Index
  alias Favn.Scheduler.State
  alias Favn.Window.Policy
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.Page
  alias FavnOrchestrator.ScheduleListEntry
  alias FavnOrchestrator.ScheduleOccurrencePreview
  alias FavnOrchestrator.Scheduler.Cron
  alias FavnOrchestrator.Scheduler.ManifestEntries
  alias FavnOrchestrator.Scheduler.Runtime, as: SchedulerRuntime
  alias FavnOrchestrator.SchedulerEntry
  alias FavnOrchestrator.Storage

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

  @doc "Lists schedule inspection entries, falling back to the active manifest."
  @spec list_entries() :: {:ok, [SchedulerEntry.t()]} | {:error, term()}
  def list_entries do
    case scheduled_entries() do
      entries when is_list(entries) -> {:ok, sort_entries(entries)}
      {:error, :scheduler_not_running} -> list_schedule_entries_from_active_manifest()
      {:error, _reason} = error -> error
    end
  end

  @doc "Returns a bounded, filtered page of operator schedule entries."
  @spec page_entries(keyword()) :: {:ok, Page.t(ScheduleListEntry.t())} | {:error, term()}
  def page_entries(filters \\ []) when is_list(filters) do
    with :ok <- validate_options(filters, @list_filter_keys, :invalid_schedule_list_filters),
         {:ok, page_opts} <- Page.normalize_opts(filters),
         {:ok, entries} <- list_entries() do
      limit = Keyword.fetch!(page_opts, :limit)
      offset = Keyword.fetch!(page_opts, :offset)

      items =
        entries
        |> Enum.map(&ScheduleListEntry.from_scheduler_entry(entry_id(&1), &1))
        |> filter_schedule_list_entries(filters)
        |> Enum.slice(offset, limit + 1)

      {:ok, Page.from_fetched(items, page_opts)}
    end
  end

  @doc "Returns one schedule inspection entry by its control-plane id."
  @spec get_entry(String.t()) :: {:ok, SchedulerEntry.t()} | {:error, term()}
  def get_entry(schedule_id) when is_binary(schedule_id) do
    with {:ok, entries} <- list_entries() do
      case Enum.find(entries, &(entry_id(&1) == schedule_id)) do
        nil -> {:error, :schedule_not_found}
        %SchedulerEntry{} = entry -> {:ok, entry}
      end
    end
  end

  @doc "Enables a schedule for future submissions."
  @spec enable(String.t()) :: {:ok, SchedulerEntry.t()} | {:error, term()}
  def enable(schedule_id) when is_binary(schedule_id),
    do: update_schedule_activation(schedule_id, :enabled)

  @doc "Disables a schedule for future submissions."
  @spec disable(String.t()) :: {:ok, SchedulerEntry.t()} | {:error, term()}
  def disable(schedule_id) when is_binary(schedule_id),
    do: update_schedule_activation(schedule_id, :disabled)

  @doc "Previews upcoming occurrences for a schedule."
  @spec preview_occurrences(String.t(), keyword()) ::
          {:ok, [ScheduleOccurrencePreview.t()]} | {:error, term()}
  def preview_occurrences(schedule_id, opts \\ [])
      when is_binary(schedule_id) and is_list(opts) do
    with :ok <-
           validate_options(opts, [:limit, :now], :invalid_schedule_occurrence_preview_options),
         {:ok, limit} <- normalize_occurrence_preview_limit(opts),
         {:ok, now} <- normalize_occurrence_preview_now(opts),
         {:ok, %SchedulerEntry{} = entry} <- get_entry(schedule_id),
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

  defp list_schedule_entries_from_active_manifest do
    with {:ok, manifest_version_id} <- ManifestStore.get_active_manifest(),
         {:ok, version} <- ManifestStore.get_manifest(manifest_version_id),
         {:ok, index} <- Index.build_from_version(version),
         {:ok, runtime_entries} <- ManifestEntries.discover(version, index) do
      now = DateTime.utc_now()

      result =
        runtime_entries
        |> Map.values()
        |> Enum.reduce_while({:ok, []}, fn runtime_entry, {:ok, acc} ->
          case load_schedule_entry_state(runtime_entry, now) do
            {:ok, state} ->
              runtime_entry =
                Map.put(runtime_entry, :next_due_at, next_schedule_due_at(runtime_entry, now))

              {:cont, {:ok, [SchedulerEntry.from_runtime(runtime_entry, state) | acc]}}

            {:error, reason} ->
              {:halt, {:error, reason}}
          end
        end)

      case result do
        {:ok, entries} ->
          {:ok, sort_entries(entries)}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp load_schedule_entry_state(entry, now) do
    key = {entry.module, entry.schedule.name}

    case Storage.get_scheduler_state(key) do
      {:ok, nil} ->
        state = %State{
          pipeline_module: entry.module,
          schedule_id: entry.schedule.name,
          schedule_fingerprint: entry.schedule_fingerprint,
          activation_state: :pending_activation,
          updated_at: now,
          version: 1
        }

        with :ok <- Storage.put_scheduler_state(key, state) do
          {:ok, state}
        end

      {:ok, %State{} = state} ->
        reconcile_schedule_entry_state(key, state, entry, now)

      {:ok, state} when is_map(state) ->
        reconcile_schedule_entry_state(key, struct(State, state), entry, now)

      {:error, _reason} = error ->
        error
    end
  end

  defp reconcile_schedule_entry_state(key, state, entry, now) do
    next =
      cond do
        is_nil(state.activation_state) ->
          %{
            state
            | activation_state: initial_schedule_activation_state(state, entry),
              version: next_scheduler_version(state)
          }

        state.schedule_fingerprint != entry.schedule_fingerprint ->
          %State{
            pipeline_module: entry.module,
            schedule_id: entry.schedule.name,
            schedule_fingerprint: entry.schedule_fingerprint,
            activation_state: :needs_review,
            updated_at: now,
            version: next_scheduler_version(state)
          }

        true ->
          %{state | schedule_id: entry.schedule.name}
      end

    if next != state do
      persisted = %{next | version: next.version || next_scheduler_version(state)}

      with :ok <- Storage.put_scheduler_state(key, persisted) do
        {:ok, persisted}
      end
    else
      {:ok, next}
    end
  end

  defp initial_schedule_activation_state(state, entry) do
    has_runtime_cursor? =
      not is_nil(state.last_due_at) or not is_nil(state.last_submitted_due_at) or
        not is_nil(state.in_flight_run_id) or not is_nil(state.queued_due_at)

    cond do
      entry.schedule.active != true -> :disabled
      has_runtime_cursor? -> :enabled
      true -> :pending_activation
    end
  end

  defp next_scheduler_version(%State{version: version})
       when is_integer(version) and version > 0,
       do: version + 1

  defp next_scheduler_version(_state), do: 1

  defp update_schedule_activation(schedule_id, activation_state)
       when activation_state in [:enabled, :disabled] do
    with {:ok, %SchedulerEntry{} = entry} <- get_entry(schedule_id),
         :ok <- validate_activation_target(entry),
         {:ok, state} <- current_schedule_state(entry),
         :ok <- persist_schedule_activation(entry, state, activation_state),
         :ok <- reload_scheduler_if_running() do
      get_entry(schedule_id)
    end
  end

  defp validate_activation_target(%SchedulerEntry{active: true}), do: :ok
  defp validate_activation_target(%SchedulerEntry{}), do: {:error, :schedule_inactive_in_manifest}

  defp current_schedule_state(%SchedulerEntry{} = entry) do
    key = {entry.pipeline_module, entry.schedule_id}

    case Storage.get_scheduler_state(key) do
      {:ok, %State{} = state} -> {:ok, state}
      {:ok, state} when is_map(state) -> {:ok, struct(State, state)}
      {:ok, nil} -> {:ok, scheduler_state_from_entry(entry)}
      {:error, _reason} = error -> error
    end
  end

  defp scheduler_state_from_entry(%SchedulerEntry{} = entry) do
    %State{
      pipeline_module: entry.pipeline_module,
      schedule_id: entry.schedule_id,
      schedule_fingerprint: entry.schedule_fingerprint,
      activation_state: entry.activation_state,
      last_evaluated_at: entry.last_evaluated_at,
      last_due_at: entry.last_due_at,
      last_submitted_due_at: entry.last_submitted_due_at,
      in_flight_run_id: entry.in_flight_run_id,
      queued_due_at: entry.queued_due_at,
      last_scheduler_error: entry.last_scheduler_error,
      updated_at: entry.updated_at,
      version: 1
    }
  end

  defp persist_schedule_activation(entry, state, :enabled) do
    now = DateTime.utc_now()

    next = %{
      state
      | activation_state: :enabled,
        last_due_at: latest_schedule_due_at(entry, now) || state.last_due_at,
        queued_due_at: nil,
        last_scheduler_error: nil,
        updated_at: now,
        version: next_scheduler_version(state)
    }

    Storage.put_scheduler_state({entry.pipeline_module, entry.schedule_id}, next)
  end

  defp persist_schedule_activation(entry, state, :disabled) do
    now = DateTime.utc_now()

    next = %{
      state
      | activation_state: :disabled,
        queued_due_at: nil,
        updated_at: now,
        version: next_scheduler_version(state)
    }

    Storage.put_scheduler_state({entry.pipeline_module, entry.schedule_id}, next)
  end

  defp latest_schedule_due_at(%SchedulerEntry{cron: cron, timezone: timezone}, now)
       when is_binary(cron) and is_binary(timezone),
       do: Cron.latest_due(cron, timezone, now)

  defp latest_schedule_due_at(_entry, _now), do: nil

  defp reload_scheduler_if_running do
    case reload_scheduler() do
      :ok -> :ok
      {:error, :scheduler_not_running} -> :ok
      {:error, _reason} = error -> error
    end
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

  defp next_schedule_due_at(%{schedule: %{cron: cron, timezone: timezone}}, now)
       when is_binary(cron) and is_binary(timezone),
       do: Cron.next_due(cron, timezone, now)

  defp next_schedule_due_at(_entry, _now), do: nil

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

  defp sort_entries(entries),
    do: Enum.sort_by(entries, &{inspect(&1.pipeline_module), inspect(&1.schedule_id)})

  defp validate_options(opts, allowed_keys, error) do
    with true <- Keyword.keyword?(opts),
         [] <- Keyword.keys(opts) -- allowed_keys do
      :ok
    else
      false -> {:error, error}
      unsupported_keys -> {:error, {error, Enum.uniq(unsupported_keys)}}
    end
  end

  defp scheduled_entries, do: SchedulerRuntime.inspect_entries()
  defp reload_scheduler, do: SchedulerRuntime.reload()
end
