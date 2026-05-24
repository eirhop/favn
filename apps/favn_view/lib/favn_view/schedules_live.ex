defmodule FavnView.SchedulesLive do
  @moduledoc false

  use FavnView, :live_view

  alias FavnView.ScheduleRoute
  alias FavnView.Components.SchedulesPage

  @default_filters %{
    "search" => "",
    "activation_state" => "all",
    "runtime_state" => "all",
    "pipeline" => "all",
    "window" => "all"
  }

  @valid_modes ~w(list)

  @impl true
  def mount(_params, _session, socket) do
    {entries, error} = load_entries(@default_filters)

    socket =
      assign(socket,
        schedules: entries,
        all_schedules: entries,
        filters: @default_filters,
        filter_options: filter_options(entries),
        summary: summary(entries),
        active_mode: :list,
        loading: false,
        error: error,
        nav_items: SchedulesPage.nav_items(:schedules)
      )

    {:ok, socket}
  end

  @impl true
  def handle_event("filter_schedules", %{"filters" => params}, socket) do
    filters = normalize_filters(socket.assigns.filters, params)
    {entries, error} = load_entries(filters)

    {:noreply,
     assign(socket,
       schedules: entries,
       filters: filters,
       summary: summary(entries),
       error: error
     )}
  end

  def handle_event("clear_filters", _params, socket) do
    {entries, error} = load_entries(@default_filters)

    {:noreply,
     assign(socket,
       schedules: entries,
       all_schedules: entries,
       filters: @default_filters,
       filter_options: filter_options(entries),
       summary: summary(entries),
       error: error
     )}
  end

  def handle_event("set_mode", %{"mode" => mode}, socket) when mode in @valid_modes do
    {:noreply, assign(socket, :active_mode, String.to_existing_atom(mode))}
  end

  def handle_event("set_mode", _params, socket), do: {:noreply, socket}

  @impl true
  def render(assigns) do
    ~H"""
    <SchedulesPage.schedules_page
      schedules={@schedules}
      all_schedules={@all_schedules}
      filters={@filters}
      filter_options={@filter_options}
      summary={@summary}
      active_mode={@active_mode}
      loading={@loading}
      error={@error}
      nav_items={@nav_items}
    />
    """
  end

  defp load_entries(filters) do
    case page_schedule_list_entries(orchestrator_filters(filters)) do
      {:ok, %{items: entries}} -> {Enum.map(entries, &schedule_from_public/1), nil}
      {:error, reason} -> {[], inspect(reason)}
    end
  end

  defp page_schedule_list_entries(opts) do
    Application.get_env(
      :favn_view,
      :page_schedule_list_entries_fun,
      &FavnOrchestrator.page_schedule_list_entries/1
    ).(opts)
  end

  defp orchestrator_filters(filters) do
    []
    |> Keyword.put(:limit, 500)
    |> maybe_put_filter(:search, filters["search"])
    |> maybe_put_atom_filter(:activation_state, filters["activation_state"])
    |> maybe_put_atom_filter(:runtime_state, filters["runtime_state"])
    |> maybe_put_filter(:pipeline_module, filters["pipeline"])
    |> maybe_put_filter(:window, filters["window"])
  end

  defp maybe_put_filter(opts, _key, value) when value in [nil, "", "all"], do: opts
  defp maybe_put_filter(opts, key, value), do: Keyword.put(opts, key, value)

  defp maybe_put_atom_filter(opts, _key, value) when value in [nil, "", "all"], do: opts

  defp maybe_put_atom_filter(opts, key, value) do
    case known_filter_atom(value) do
      nil -> opts
      atom -> Keyword.put(opts, key, atom)
    end
  end

  defp known_filter_atom("pending_activation"), do: :pending_activation
  defp known_filter_atom("enabled"), do: :enabled
  defp known_filter_atom("disabled"), do: :disabled
  defp known_filter_atom("needs_review"), do: :needs_review
  defp known_filter_atom("retired"), do: :retired
  defp known_filter_atom("inactive"), do: :inactive
  defp known_filter_atom("idle"), do: :idle
  defp known_filter_atom("running"), do: :running
  defp known_filter_atom("queued"), do: :queued
  defp known_filter_atom(_value), do: nil

  defp normalize_filters(previous, params) do
    Map.merge(previous, %{
      "search" => Map.get(params, "search", ""),
      "activation_state" => Map.get(params, "activation_state", "all"),
      "runtime_state" => Map.get(params, "runtime_state", "all"),
      "pipeline" => Map.get(params, "pipeline", "all"),
      "window" => Map.get(params, "window", "all")
    })
  end

  defp schedule_from_public(entry) do
    %{
      id: entry.id,
      route_id: ScheduleRoute.to_param(entry.id),
      schedule_id: entry.schedule_id,
      schedule_label: schedule_label(entry.schedule_id),
      pipeline_module: entry.pipeline_module,
      pipeline_label: module_label(entry.pipeline_module),
      cron: entry.cron || "-",
      timezone: entry.timezone || "-",
      window_kind: window_kind(entry.window),
      window_label: window_label(entry.window),
      overlap: entry.overlap,
      missed: entry.missed,
      activation_state: entry.activation_state,
      activation_label: humanize(entry.activation_state),
      runtime_state: entry.runtime_state,
      runtime_label: humanize(entry.runtime_state),
      next_due_at: entry.next_due_at,
      next_due_label: timestamp_label(entry.next_due_at),
      last_submitted_label: timestamp_label(entry.last_submitted_due_at),
      in_flight_run_id: entry.in_flight_run_id,
      current_run_label: short_id(entry.in_flight_run_id),
      last_scheduler_error: scheduler_error_from_public(entry.last_scheduler_error),
      updated_label: timestamp_label(entry.updated_at),
      manifest_active?: entry.manifest_active?,
      effective_enabled?: entry.effective_enabled?
    }
  end

  defp filter_options(entries) do
    %{
      pipelines: pipeline_option_values(entries),
      windows: option_values(entries, :window_kind, &window_option_label/1)
    }
  end

  defp pipeline_option_values(entries) do
    entries
    |> Enum.map(&Map.get(&1, :pipeline_module))
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
    |> Enum.sort_by(&module_label/1)
    |> Enum.map(fn module -> {module_label(module), module_label(module)} end)
  end

  defp option_values(entries, field, label_fun) do
    entries
    |> Enum.map(&Map.get(&1, field))
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
    |> Enum.sort_by(&to_string/1)
    |> Enum.map(&{label_fun.(&1), to_string(&1)})
  end

  defp summary(entries) do
    %{
      total: length(entries),
      enabled: Enum.count(entries, &(&1.activation_state == :enabled)),
      pending_activation: Enum.count(entries, &(&1.activation_state == :pending_activation)),
      disabled: Enum.count(entries, &(&1.activation_state == :disabled)),
      running: Enum.count(entries, &(&1.runtime_state == :running)),
      queued: Enum.count(entries, &(&1.runtime_state == :queued))
    }
  end

  defp schedule_label(nil), do: "default"
  defp schedule_label(value), do: to_string(value)

  defp module_label(module) when is_atom(module) do
    module
    |> Atom.to_string()
    |> String.replace_prefix("Elixir.", "")
  end

  defp module_label(value), do: to_string(value)

  defp window_kind(nil), do: :none
  defp window_kind(%{kind: kind}) when is_atom(kind), do: kind
  defp window_kind(_window), do: :unknown

  defp window_label(nil), do: "No window"

  defp window_label(%{kind: kind, timezone: timezone}) when not is_nil(timezone),
    do: "#{humanize(kind)} #{timezone}"

  defp window_label(%{kind: kind}), do: humanize(kind)
  defp window_label(_window), do: "Window"

  defp window_option_label(:none), do: "No window"
  defp window_option_label(value), do: humanize(value)

  defp timestamp_label(%DateTime{} = datetime), do: Calendar.strftime(datetime, "%b %-d %H:%M")
  defp timestamp_label(_value), do: "-"

  defp short_id(nil), do: nil
  defp short_id(id) when is_binary(id) and byte_size(id) > 12, do: String.slice(id, 0, 12)
  defp short_id(id), do: id

  defp scheduler_error_from_public(nil), do: nil

  defp scheduler_error_from_public(error) do
    %{
      phase_label: humanize(Map.get(error, :phase, :scheduler)),
      message: Map.get(error, :message, "Scheduler error")
    }
  end

  defp humanize(value) do
    value
    |> to_string()
    |> String.replace("_", " ")
    |> String.capitalize()
  end
end
