defmodule FavnView.RunDetailLive do
  @moduledoc false

  use FavnView, :live_view

  alias FavnView.AssetRoute
  alias FavnView.Components.AssetCataloguePage
  alias FavnView.Components.RunDetailPage
  alias FavnView.LogsViewModel

  @refresh_interval_ms 1_500
  @active_statuses [:pending, :running]
  @valid_modes ~w(overview events outputs context debug)

  @impl true
  def mount(%{"run_id" => run_id}, _session, socket) do
    run = load_run(run_id)

    socket =
      assign(socket,
        run_id: run_id,
        run: run,
        run_event_sequence: latest_event_sequence(run),
        run_events_live?: false,
        active_mode: :overview,
        nav_items: AssetCataloguePage.nav_items(:runs)
      )
      |> maybe_subscribe_run()
      |> maybe_schedule_refresh()

    {:ok, socket}
  end

  @impl true
  def handle_info(:refresh_run, socket) do
    run = load_run(socket.assigns.run_id, socket.assigns.run[:back_asset_href])

    {:noreply,
     socket
     |> assign(:run, run)
     |> assign(:run_event_sequence, latest_event_sequence(run, socket.assigns.run_event_sequence))
     |> maybe_schedule_refresh()}
  end

  def handle_info(
        {:favn_run_event, %{run_id: run_id} = event},
        %{assigns: %{run_id: run_id}} = socket
      ) do
    socket =
      if fresh_run_event?(event, socket.assigns.run_event_sequence) do
        reload_run_from_event(socket, Map.get(event, :sequence))
      else
        socket
      end

    {:noreply, socket}
  end

  def handle_info({:favn_run_event, _event}, socket), do: {:noreply, socket}

  @impl true
  def handle_event("set_mode", %{"mode" => mode}, socket) when mode in @valid_modes do
    {:noreply, assign(socket, :active_mode, String.to_existing_atom(mode))}
  end

  def handle_event("set_mode", _params, socket), do: {:noreply, socket}

  @impl true
  def render(assigns) do
    ~H"""
    <RunDetailPage.run_detail_page
      run={@run}
      run_id={@run_id}
      nav_items={@nav_items}
      active_mode={@active_mode}
    />
    """
  end

  @impl true
  def terminate(_reason, socket) do
    if socket.assigns[:run_events_live?] do
      FavnOrchestrator.unsubscribe_run(socket.assigns.run_id)
    end

    :ok
  end

  defp load_run(run_id, existing_back_asset_href \\ nil) do
    case FavnOrchestrator.get_run_detail(run_id) do
      {:ok, detail} -> detail_from_public(detail, existing_back_asset_href)
      {:error, reason} -> %{id: run_id, found?: false, error: error_label(reason)}
    end
  end

  defp detail_from_public(%{summary: summary} = detail, existing_back_asset_href) do
    target = target_label(summary)
    window = window_label(summary.window)
    status = summary.status
    steps = Enum.map(Map.get(detail, :steps, []), &step_from_public/1)
    events = Enum.map(Map.get(detail, :events, []), &event_from_public/1)

    backfill_failures =
      Enum.map(Map.get(detail, :backfill_failures, []), &backfill_failure_from_public/1)

    failure_summary = failure_summary(status, steps, events, backfill_failures)

    %{
      found?: true,
      id: summary.id,
      raw_status: status,
      active?: active_status?(status),
      short_id: short_id(summary.id),
      title: short_id(summary.id),
      subtitle: subtitle([target, window]),
      status: LogsViewModel.status_label(status),
      status_tone: LogsViewModel.status_tone(status),
      target: target || "No target",
      trigger: label(summary.kind),
      window: window,
      started_at: LogsViewModel.timestamp_label(summary.started_at),
      finished_at: LogsViewModel.timestamp_label(summary.finished_at),
      duration: LogsViewModel.duration_ms_label(summary.duration_ms),
      manifest_version_id: summary.manifest_version_id || "Unknown",
      asset_results: steps,
      events: events,
      latest_event_summary: latest_event_summary(events),
      current_activity: current_activity(status, steps, events),
      failure_summary: failure_summary,
      backfill_failures: backfill_failures,
      asset_empty_message: asset_empty_message(status, failure_summary),
      outputs: outputs(steps),
      context: context_items(summary, target, window),
      back_asset_href: existing_back_asset_href || back_asset_href(summary.asset_ref),
      raw_run: inspect(detail, pretty: true, limit: 50, printable_limit: 2_000),
      raw_events: inspect(detail.events, pretty: true, limit: 50, printable_limit: 2_000)
    }
  end

  defp maybe_schedule_refresh(%{assigns: %{run: %{active?: true}}} = socket) do
    if connected?(socket), do: Process.send_after(self(), :refresh_run, @refresh_interval_ms)
    socket
  end

  defp maybe_schedule_refresh(socket), do: socket

  defp maybe_subscribe_run(socket) do
    if connected?(socket) do
      case FavnOrchestrator.subscribe_run(socket.assigns.run_id) do
        :ok ->
          socket
          |> assign(:run_events_live?, true)
          |> replay_run_event_gap()

        {:error, _reason} ->
          socket
      end
    else
      socket
    end
  end

  defp replay_run_event_gap(socket) do
    after_sequence = socket.assigns.run_event_sequence || 0

    case FavnOrchestrator.list_run_stream_events(socket.assigns.run_id,
           after_sequence: after_sequence,
           limit: 200
         ) do
      {:ok, []} ->
        socket

      {:ok, events} ->
        latest_sequence =
          events |> Enum.map(&Map.get(&1, :sequence)) |> Enum.max(fn -> after_sequence end)

        reload_run_from_event(socket, latest_sequence)

      {:error, _reason} ->
        socket
    end
  end

  defp reload_run_from_event(socket, event_sequence) do
    run = load_run(socket.assigns.run_id, socket.assigns.run[:back_asset_href])

    socket
    |> assign(:run, run)
    |> assign(:run_event_sequence, latest_event_sequence(run, event_sequence))
    |> maybe_schedule_refresh()
  end

  defp fresh_run_event?(%{sequence: sequence}, latest_sequence) when is_integer(sequence) do
    is_nil(latest_sequence) or sequence > latest_sequence
  end

  defp fresh_run_event?(_event, _latest_sequence), do: true

  defp step_from_public(step) do
    %{
      id: step.id,
      asset_ref: step.asset_ref,
      display_name: LogsViewModel.display_name(step.asset_ref) || step.asset_ref,
      secondary: step_secondary(step),
      status: step_status_label(step.status, Map.get(step, :failure_role)),
      raw_status: step.status,
      status_tone: step_status_tone(step.status, Map.get(step, :failure_role)),
      duration: LogsViewModel.duration_ms_label(step.duration_ms),
      started_at: LogsViewModel.timestamp_label(step.started_at),
      attempt: step.attempt,
      error: error_summary(step.error),
      explanation: step.explanation,
      failure_role: Map.get(step, :failure_role),
      root_failure_asset_ref: Map.get(step, :root_failure_asset_ref),
      output: step.output,
      inspectable?: true
    }
  end

  defp event_from_public(event) do
    %{
      sequence: Map.get(event, :sequence),
      raw_status: Map.get(event, :status),
      timestamp: LogsViewModel.timestamp_label(Map.get(event, :occurred_at)),
      event_type: label(Map.get(event, :event_type)),
      raw_event_type: Map.get(event, :event_type),
      status: LogsViewModel.status_label(Map.get(event, :status)),
      status_tone: LogsViewModel.status_tone(Map.get(event, :status)),
      asset: LogsViewModel.ref_label(Map.get(event, :asset_ref)),
      summary: event_summary(event)
    }
  end

  defp backfill_failure_from_public(failure) do
    window = Map.get(failure, :window) || %{}
    child_run_id = Map.get(failure, :child_run_id)

    %{
      child_run_id: child_run_id,
      child_run_href: child_run_id && "/runs/#{child_run_id}",
      status: LogsViewModel.status_label(Map.get(failure, :status)),
      status_tone: LogsViewModel.status_tone(Map.get(failure, :status)),
      asset_ref: Map.get(failure, :asset_ref),
      error: error_summary(Map.get(failure, :error)),
      window: window_label(window) || "Unknown window",
      attempt_count: Map.get(failure, :attempt_count),
      duration: LogsViewModel.duration_ms_label(Map.get(failure, :duration_ms))
    }
  end

  defp step_secondary(step) do
    [window_label(step.window), step.stage && "Stage #{step.stage}"]
    |> Enum.reject(&is_nil/1)
    |> Enum.join(" · ")
    |> case do
      "" -> nil
      value -> value
    end
  end

  defp target_label(%{target_refs: refs}) when is_list(refs) and refs != [] do
    case refs do
      [single_ref] ->
        LogsViewModel.ref_label(single_ref)

      refs ->
        "#{length(refs)} selected assets"
    end
  end

  defp target_label(%{asset_ref: ref}), do: LogsViewModel.ref_label(ref)

  defp window_label(%{label: label}) when is_binary(label), do: label
  defp window_label(%{"label" => label}) when is_binary(label), do: label
  defp window_label(%{key: key}) when is_binary(key), do: key
  defp window_label(%{"key" => key}) when is_binary(key), do: key
  defp window_label(_window), do: nil

  defp context_items(summary, target, window) do
    [
      %{label: "Run ID", value: summary.id},
      %{label: "Manifest version", value: summary.manifest_version_id || "Unknown"},
      %{label: "Target", value: target || "No target"},
      %{label: "Trigger", value: label(summary.kind)},
      %{label: "Window", value: window || "No window metadata"},
      %{label: "Submit kind", value: label(summary.submit_kind)}
    ]
  end

  defp back_asset_href(nil), do: nil

  defp back_asset_href(ref) do
    ref_string = LogsViewModel.ref_label(ref)

    with {:ok, entries} <- FavnOrchestrator.active_asset_catalogue(),
         entry when not is_nil(entry) <-
           Enum.find(entries, fn entry ->
             LogsViewModel.ref_label(Map.get(entry, :asset_ref)) == ref_string
           end),
         target_id when is_binary(target_id) <- Map.get(entry, :target_id) do
      "/assets/#{AssetRoute.to_param(target_id)}"
    else
      _other -> nil
    end
  end

  defp failure_summary(status, _steps, _events, [failure | _rest])
       when status in [:partial, :error, :timed_out] do
    %{
      count: 1,
      total: 1,
      asset: failure.asset_ref,
      error: failure.error
    }
  end

  defp failure_summary(status, steps, events, _backfill_failures)
       when status in [:partial, :error, :timed_out] do
    failed = Enum.filter(steps, &(&1.status_tone == :error))
    first = List.first(failed)
    latest_error = Enum.find(Enum.reverse(events), &(&1.status_tone == :error))

    %{
      count: length(failed),
      total: length(steps),
      asset: first && first.asset_ref,
      error: (first && first.error) || (latest_error && latest_error.summary)
    }
  end

  defp failure_summary(_status, _steps, _events, _backfill_failures), do: nil

  defp current_activity(status, steps, events) when status in [:pending, :running] do
    running = Enum.find(steps, &(&1.status == "Running"))
    latest = List.last(events)
    latest_asset = latest && meaningful_activity(latest.asset)
    latest_summary = latest && meaningful_activity(latest.summary)

    cond do
      running -> "Currently executing #{running.asset_ref}"
      latest_asset -> "Latest event: #{latest_asset}"
      latest_summary -> "Latest event: #{latest_summary}"
      latest -> nil
      true -> "Waiting for first execution event..."
    end
  end

  defp current_activity(_status, _steps, _events), do: nil

  defp asset_empty_message(status, _failure) when status in [:pending, :running],
    do: "Run accepted. Waiting for asset execution results..."

  defp asset_empty_message(:ok, _failure),
    do: "Run completed, but no asset results were persisted."

  defp asset_empty_message(status, %{error: error})
       when status in [:error, :timed_out] and is_binary(error),
       do: "Run failed before asset results were persisted. Latest error: #{error}"

  defp asset_empty_message(status, _failure) when status in [:error, :timed_out],
    do: "Run failed before asset results were persisted."

  defp asset_empty_message(_status, _failure), do: "No asset results persisted for this run yet."

  defp outputs(steps),
    do:
      steps
      |> Enum.filter(& &1.output)
      |> Enum.map(&%{asset: &1.asset_ref, output: inspect(&1.output, pretty: true)})

  defp latest_event_summary([]), do: nil
  defp latest_event_summary(events), do: events |> List.last() |> Map.get(:summary)
  defp latest_event_sequence(run, fallback \\ nil)

  defp latest_event_sequence(%{events: events}, fallback) when is_list(events) do
    events
    |> Enum.map(&Map.get(&1, :sequence))
    |> Enum.reject(&is_nil/1)
    |> Enum.max(fn -> fallback end)
  end

  defp latest_event_sequence(_run, fallback), do: fallback

  defp meaningful_activity(value) when is_binary(value) do
    value = String.trim(value)

    if value in ["", "nil", "Asset nil", "Unknown"] do
      nil
    else
      value
    end
  end

  defp meaningful_activity(_value), do: nil

  defp active_status?(status), do: status in @active_statuses
  defp subtitle(parts), do: parts |> Enum.reject(&is_nil/1) |> Enum.join(" · ")
  defp short_id(id) when is_binary(id) and byte_size(id) > 18, do: String.slice(id, 0, 18)
  defp short_id(id) when is_binary(id), do: id
  defp short_id(_id), do: "unknown"
  defp step_status_label(_status, :cascade), do: "Cascade failed"
  defp step_status_label(status, _role) when status in [:pending, "pending"], do: "Waiting"
  defp step_status_label(status, _role) when status in [:ok, "ok"], do: "Ran"
  defp step_status_label(status, _role), do: LogsViewModel.status_label(status)

  defp step_status_tone(_status, :cascade), do: :warning
  defp step_status_tone(status, _role), do: LogsViewModel.status_tone(status)

  defp event_summary(event),
    do:
      Map.get(event.data || %{}, :message) || Map.get(event.data || %{}, "message") ||
        if(Map.get(event, :asset_ref),
          do: "Asset #{LogsViewModel.ref_label(Map.get(event, :asset_ref))}",
          else: LogsViewModel.status_label(Map.get(event, :status))
        )

  defp label(nil), do: "Unknown"
  defp label(value), do: value |> to_string() |> String.replace("_", " ") |> String.capitalize()
  defp error_summary(nil), do: nil
  defp error_summary(%{message: message}) when is_binary(message), do: message
  defp error_summary(%{"message" => message}) when is_binary(message), do: message
  defp error_summary(%{reason: reason}), do: error_summary(reason)
  defp error_summary(%{"reason" => reason}), do: error_summary(reason)
  defp error_summary(reason) when is_binary(reason), do: reason
  defp error_summary(reason) when is_atom(reason), do: label(reason)
  defp error_summary(reason), do: inspect(reason, limit: 5, printable_limit: 200)
  defp error_label(:not_found), do: "Run not found"
  defp error_label(_reason), do: "Run could not be loaded"
end
