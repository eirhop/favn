defmodule FavnView.ScheduleDetailLive do
  @moduledoc false

  use FavnView, :live_view

  alias FavnView.Orchestrator
  alias FavnView.CommandAttempt
  alias FavnView.Components.ScheduleDetailPage
  alias FavnView.OperatorErrorLabels
  alias FavnView.ScheduleRoute

  @activation_operation "schedule_activation"

  @impl true
  def mount(%{"schedule_id" => route_id}, _session, socket) do
    schedule_id = ScheduleRoute.from_param(route_id)
    operator_context = socket.assigns.current_scope.operator_context

    {schedule, error, occurrence_preview, occurrence_error} =
      load_schedule(operator_context, schedule_id, socket.assigns.current_scope)

    socket =
      assign(socket,
        schedule: schedule,
        schedule_id: schedule_id,
        route_id: route_id,
        occurrence_preview: occurrence_preview,
        occurrence_error: occurrence_error,
        activation_error: nil,
        active_view: :overview,
        activation_attempt: nil,
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

  def handle_event(
        "set_schedule_activation",
        %{"action" => action} = params,
        socket
      )
      when action in ["enable", "disable"] do
    schedule_id = socket.assigns.schedule_id

    attempt =
      CommandAttempt.next(
        socket.assigns.activation_attempt,
        @activation_operation,
        {schedule_id, action},
        params
      )

    result =
      case action do
        "enable" -> enable_schedule(operator_context(socket), schedule_id, attempt.key)
        "disable" -> disable_schedule(operator_context(socket), schedule_id, attempt.key)
      end

    case result do
      {:ok, _receipt} ->
        {schedule, error, occurrence_preview, occurrence_error} =
          load_schedule(operator_context(socket), schedule_id, socket.assigns.current_scope)

        {:noreply,
         assign(socket,
           schedule: schedule,
           occurrence_preview: occurrence_preview,
           occurrence_error: occurrence_error,
           activation_attempt: nil,
           activation_error: nil,
           error: error
         )
         |> CommandAttempt.acknowledge(attempt)}

      {:error, reason} ->
        failure = OperatorErrorLabels.failure(:schedule_activation, reason)
        {socket, retained_attempt} = CommandAttempt.settle_failure(socket, attempt, reason)

        {:noreply,
         assign(socket,
           activation_attempt: retained_attempt,
           activation_error: failure.label
         )}
    end
  end

  @impl true
  def render(assigns) do
    ~H"""
    <ScheduleDetailPage.schedule_detail_page
      schedule={@schedule}
      occurrence_preview={@occurrence_preview}
      occurrence_error={@occurrence_error}
      activation_error={@activation_error}
      active_view={@active_view}
      loading={@loading}
      error={@error}
      nav_items={@nav_items}
      current_scope={@current_scope}
      operator_workspaces={@operator_workspaces}
    />
    """
  end

  defp load_schedule(operator_context, schedule_id, timezone) do
    case get_schedule_entry(operator_context, schedule_id) do
      {:ok, entry} ->
        {preview, preview_error} =
          load_occurrence_preview(operator_context, schedule_id, timezone)

        {schedule_from_public(schedule_id, entry, timezone), nil, preview, preview_error}

      {:error, reason} ->
        {nil, OperatorErrorLabels.load(reason), [], nil}
    end
  end

  defp get_schedule_entry(operator_context, schedule_id) do
    facade(:get_schedule_entry_fun, &Orchestrator.get_schedule_entry/2).(
      operator_context,
      schedule_id
    )
  end

  defp load_occurrence_preview(operator_context, schedule_id, timezone) do
    case preview_schedule_occurrences(operator_context, schedule_id, limit: 10) do
      {:ok, occurrences} -> {Enum.map(occurrences, &occurrence_from_public(&1, timezone)), nil}
      {:error, reason} -> {[], OperatorErrorLabels.schedule_occurrences(reason)}
    end
  end

  defp preview_schedule_occurrences(operator_context, schedule_id, opts) do
    facade(
      :preview_schedule_occurrences_fun,
      &Orchestrator.preview_schedule_occurrences/3
    ).(
      operator_context,
      schedule_id,
      opts
    )
  end

  defp enable_schedule(operator_context, schedule_id, command_id) do
    facade(:enable_schedule_fun, &Orchestrator.enable_schedule/3).(
      operator_context,
      schedule_id,
      command_id: command_id
    )
  end

  defp disable_schedule(operator_context, schedule_id, command_id) do
    facade(:disable_schedule_fun, &Orchestrator.disable_schedule/3).(
      operator_context,
      schedule_id,
      command_id: command_id
    )
  end

  # A test may substitute any facade call, at the facade's own arity.
  defp facade(key, default), do: Application.get_env(:favn_view, key, default)

  defp operator_context(socket), do: socket.assigns.current_scope.operator_context

  defp schedule_from_public(id, entry, timezone) do
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
      next_due_label: timestamp_label(entry.next_due_at, timezone),
      last_evaluated_label: timestamp_label(entry.last_evaluated_at, timezone),
      last_due_label: timestamp_label(entry.last_due_at, timezone),
      last_submitted_label: timestamp_label(entry.last_submitted_due_at, timezone),
      queued_due_label: timestamp_label(entry.queued_due_at, timezone),
      updated_label: timestamp_label(entry.updated_at, timezone),
      in_flight_run_id: entry.in_flight_run_id,
      current_run_label: short_id(entry.in_flight_run_id),
      last_scheduler_error: scheduler_error_from_public(entry.last_scheduler_error, timezone),
      manifest_version_id: entry.manifest_version_id,
      manifest_content_hash: entry.manifest_content_hash,
      schedule_fingerprint: entry.schedule_fingerprint
    }
  end

  defp occurrence_from_public(occurrence, timezone) do
    %{
      due_at: occurrence.due_at,
      due_label: timestamp_label(occurrence.due_at, timezone),
      timezone: occurrence.timezone,
      window_label: occurrence_window_label(occurrence.window, timezone),
      status: occurrence.status,
      status_label: humanize(occurrence.status),
      notes: occurrence.notes || []
    }
  end

  defp scheduler_error_from_public(nil, _timezone), do: nil

  defp scheduler_error_from_public(error, timezone) do
    %{
      occurred_label: timestamp_label(Map.get(error, :occurred_at), timezone),
      phase_label: humanize(Map.get(error, :phase, :scheduler)),
      code_label: humanize(Map.get(error, :code, :scheduler_error)),
      message: Map.get(error, :message, "Scheduler error")
    }
  end

  defp occurrence_window_label(nil, _timezone), do: "-"

  defp occurrence_window_label(
         %{start_at: %DateTime{} = start_at, end_at: %DateTime{} = end_at},
         timezone
       ) do
    "#{timestamp_label(start_at, timezone)} -> #{timestamp_label(end_at, timezone)}"
  end

  defp occurrence_window_label(%{key: key}, _timezone) when not is_nil(key), do: inspect(key)
  defp occurrence_window_label(_window, _timezone), do: "Window"

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

  defp timestamp_label(%DateTime{} = datetime, timezone),
    do: FavnView.Time.format(datetime, "%b %-d %H:%M", timezone)

  defp timestamp_label(_value, _timezone), do: "-"

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
