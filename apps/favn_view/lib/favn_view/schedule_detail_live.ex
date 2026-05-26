defmodule FavnView.ScheduleDetailLive do
  @moduledoc false

  use FavnView, :live_view

  alias FavnView.Components.ScheduleDetailPage
  alias FavnView.OperatorErrorLabels
  alias FavnView.ScheduleRoute

  @impl true
  def mount(%{"schedule_id" => route_id}, _session, socket) do
    schedule_id = ScheduleRoute.from_param(route_id)
    {schedule, error, occurrence_preview, occurrence_error} = load_schedule(schedule_id)

    socket =
      assign(socket,
        schedule: schedule,
        schedule_id: schedule_id,
        route_id: route_id,
        occurrence_preview: occurrence_preview,
        occurrence_error: occurrence_error,
        active_view: :overview,
        loading: false,
        error: error,
        nav_items: ScheduleDetailPage.nav_items(:schedules)
      )

    {:ok, socket}
  end

  @impl true
  def handle_event("set_detail_view", %{"mode" => "overview"}, socket) do
    {:noreply, assign(socket, :active_view, :overview)}
  end

  def handle_event("set_detail_view", %{"mode" => "occurrences"}, socket) do
    {:noreply, assign(socket, :active_view, :occurrences)}
  end

  def handle_event("set_schedule_activation", %{"action" => action}, socket)
      when action in ["enable", "disable"] do
    schedule_id = socket.assigns.schedule_id

    result =
      case action do
        "enable" -> enable_schedule(schedule_id)
        "disable" -> disable_schedule(schedule_id)
      end

    case result do
      {:ok, _entry} ->
        {schedule, error, occurrence_preview, occurrence_error} = load_schedule(schedule_id)

        {:noreply,
         assign(socket,
           schedule: schedule,
           occurrence_preview: occurrence_preview,
           occurrence_error: occurrence_error,
           error: error
         )}

      {:error, reason} ->
        {:noreply,
         assign(socket, :occurrence_error, OperatorErrorLabels.schedule_activation(reason))}
    end
  end

  def handle_event("set_detail_view", _params, socket), do: {:noreply, socket}
  def handle_event("set_schedule_activation", _params, socket), do: {:noreply, socket}

  @impl true
  def render(assigns) do
    ~H"""
    <ScheduleDetailPage.schedule_detail_page
      schedule={@schedule}
      occurrence_preview={@occurrence_preview}
      occurrence_error={@occurrence_error}
      active_view={@active_view}
      loading={@loading}
      error={@error}
      nav_items={@nav_items}
    />
    """
  end

  defp load_schedule(schedule_id) do
    case get_schedule_entry(schedule_id) do
      {:ok, entry} ->
        {preview, preview_error} = load_occurrence_preview(schedule_id)
        {schedule_from_public(schedule_id, entry), nil, preview, preview_error}

      {:error, reason} ->
        {nil, OperatorErrorLabels.load(reason), [], nil}
    end
  end

  defp get_schedule_entry(schedule_id) do
    Application.get_env(
      :favn_view,
      :get_schedule_entry_fun,
      &FavnOrchestrator.get_schedule_entry/1
    ).(schedule_id)
  end

  defp load_occurrence_preview(schedule_id) do
    case preview_schedule_occurrences(schedule_id, limit: 10) do
      {:ok, occurrences} -> {Enum.map(occurrences, &occurrence_from_public/1), nil}
      {:error, reason} -> {[], OperatorErrorLabels.schedule_occurrences(reason)}
    end
  end

  defp preview_schedule_occurrences(schedule_id, opts) do
    Application.get_env(
      :favn_view,
      :preview_schedule_occurrences_fun,
      &FavnOrchestrator.preview_schedule_occurrences/2
    ).(schedule_id, opts)
  end

  defp enable_schedule(schedule_id) do
    Application.get_env(:favn_view, :enable_schedule_fun, &FavnOrchestrator.enable_schedule/1).(
      schedule_id
    )
  end

  defp disable_schedule(schedule_id) do
    Application.get_env(:favn_view, :disable_schedule_fun, &FavnOrchestrator.disable_schedule/1).(
      schedule_id
    )
  end

  defp schedule_from_public(id, entry) do
    %{
      id: id,
      schedule_id: entry.schedule_id,
      schedule_label: schedule_label(entry.schedule_id),
      pipeline_module: entry.pipeline_module,
      pipeline_label: module_label(entry.pipeline_module),
      cron: entry.cron || "-",
      timezone: entry.timezone || "-",
      window_label: window_label(entry.window),
      overlap: entry.overlap,
      missed: entry.missed,
      manifest_active?: entry.active,
      activation_state: entry.activation_state,
      activation_label: humanize(entry.activation_state),
      activation_tone: activation_tone(entry.activation_state),
      runtime_state: entry.runtime_state,
      runtime_label: humanize(entry.runtime_state),
      effective_enabled?: entry.effective_enabled?,
      next_due_label: timestamp_label(entry.next_due_at),
      last_evaluated_label: timestamp_label(entry.last_evaluated_at),
      last_due_label: timestamp_label(entry.last_due_at),
      last_submitted_label: timestamp_label(entry.last_submitted_due_at),
      queued_due_label: timestamp_label(entry.queued_due_at),
      updated_label: timestamp_label(entry.updated_at),
      in_flight_run_id: entry.in_flight_run_id,
      current_run_label: short_id(entry.in_flight_run_id),
      last_scheduler_error: scheduler_error_from_public(entry.last_scheduler_error),
      manifest_version_id: entry.manifest_version_id,
      manifest_content_hash: entry.manifest_content_hash,
      schedule_fingerprint: entry.schedule_fingerprint
    }
  end

  defp occurrence_from_public(occurrence) do
    %{
      due_at: occurrence.due_at,
      due_label: timestamp_label(occurrence.due_at),
      timezone: occurrence.timezone,
      window_label: occurrence_window_label(occurrence.window),
      status: occurrence.status,
      status_label: humanize(occurrence.status),
      notes: occurrence.notes || []
    }
  end

  defp scheduler_error_from_public(nil), do: nil

  defp scheduler_error_from_public(error) do
    %{
      occurred_label: timestamp_label(Map.get(error, :occurred_at)),
      phase_label: humanize(Map.get(error, :phase, :scheduler)),
      code_label: humanize(Map.get(error, :code, :scheduler_error)),
      message: Map.get(error, :message, "Scheduler error")
    }
  end

  defp occurrence_window_label(nil), do: "-"

  defp occurrence_window_label(%{start_at: %DateTime{} = start_at, end_at: %DateTime{} = end_at}) do
    "#{timestamp_label(start_at)} -> #{timestamp_label(end_at)}"
  end

  defp occurrence_window_label(%{key: key}) when not is_nil(key), do: inspect(key)
  defp occurrence_window_label(_window), do: "Window"

  defp schedule_label(nil), do: "default"
  defp schedule_label(value), do: to_string(value)

  defp module_label(module) when is_atom(module) do
    module
    |> Atom.to_string()
    |> String.replace_prefix("Elixir.", "")
  end

  defp module_label(value), do: to_string(value)

  defp window_label(nil), do: "No window"

  defp window_label(%{kind: kind, timezone: timezone}) when not is_nil(timezone),
    do: "#{humanize(kind)} #{timezone}"

  defp window_label(%{kind: kind}), do: humanize(kind)
  defp window_label(_window), do: "Window"

  defp activation_tone(:enabled), do: :success
  defp activation_tone(:pending_activation), do: :warning
  defp activation_tone(:needs_review), do: :warning
  defp activation_tone(:disabled), do: :error
  defp activation_tone(_state), do: :neutral

  defp timestamp_label(%DateTime{} = datetime), do: Calendar.strftime(datetime, "%b %-d %H:%M")
  defp timestamp_label(_value), do: "-"

  defp short_id(nil), do: nil
  defp short_id(id) when is_binary(id) and byte_size(id) > 12, do: String.slice(id, 0, 12)
  defp short_id(id), do: id

  defp humanize(value) do
    value
    |> to_string()
    |> String.replace("_", " ")
    |> String.capitalize()
  end
end
