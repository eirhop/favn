defmodule FavnView.RunDetailLive do
  @moduledoc false

  use FavnView, :live_view

  alias FavnView.AssetRoute
  alias FavnView.Components.AssetCataloguePage
  alias FavnView.Components.RunDetailPage
  alias FavnView.LogsViewModel

  @refresh_interval_ms 1_500
  @active_statuses [:pending, :running]
  @valid_modes ~w(overview timeline failures windows events)
  @timeline_zoom_levels ~w(5m 15m 30m 1h 6h full)

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
        timeline_state: default_timeline_state(run),
        selected_child_run_id: nil,
        selected_attempt_id: nil,
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
        %{assigns: %{run: %{subscribed_run_id: run_id}}} = socket
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
    {:noreply, patch_run_state(socket, active_mode: String.to_existing_atom(mode))}
  end

  def handle_event("timeline_zoom", %{"zoom" => zoom}, socket)
      when zoom in @timeline_zoom_levels do
    {:noreply,
     patch_run_state(socket,
       active_mode: :timeline,
       timeline_state: %{
         socket.assigns.timeline_state
         | zoom: zoom,
           mode: :manual,
           live_follow?: false
       }
     )}
  end

  def handle_event("timeline_fit", _params, socket) do
    {:noreply,
     patch_run_state(socket,
       active_mode: :timeline,
       timeline_state: %{socket.assigns.timeline_state | mode: :fit, live_follow?: false}
     )}
  end

  def handle_event("timeline_focus", _params, socket) do
    zoom = socket.assigns.timeline_state.zoom

    {:noreply,
     patch_run_state(socket,
       active_mode: :timeline,
       timeline_state: %{
         socket.assigns.timeline_state
         | zoom: if(zoom == "full", do: "30m", else: zoom),
           mode: :manual,
           live_follow?: false
       }
     )}
  end

  def handle_event("timeline_jump_now", _params, socket) do
    if socket.assigns.run[:active?] do
      {:noreply,
       patch_run_state(socket,
         active_mode: :timeline,
         timeline_state: %{socket.assigns.timeline_state | mode: :live, live_follow?: true}
       )}
    else
      {:noreply, socket}
    end
  end

  def handle_event("timeline_pause_live", _params, socket) do
    {:noreply,
     patch_run_state(socket,
       active_mode: :timeline,
       timeline_state: %{socket.assigns.timeline_state | live_follow?: false}
     )}
  end

  def handle_event("timeline_filter", %{"timeline" => filters}, socket) do
    timeline_state =
      socket.assigns.timeline_state
      |> Map.merge(%{
        search: Map.get(filters, "search", ""),
        status: Map.get(filters, "status", "all"),
        window: Map.get(filters, "window", "all"),
        failed_only?: Map.has_key?(filters, "failed_only"),
        running_only?: Map.has_key?(filters, "running_only")
      })

    {:noreply, patch_run_state(socket, active_mode: :timeline, timeline_state: timeline_state)}
  end

  def handle_event("select_attempt", %{"attempt-id" => attempt_id}, socket) do
    {:noreply, assign(socket, :selected_attempt_id, attempt_id)}
  end

  def handle_event("close_attempt", _params, socket) do
    {:noreply, assign(socket, :selected_attempt_id, nil)}
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
      timeline_state={@timeline_state}
      timeline_hook?={true}
      selected_child_run_id={@selected_child_run_id}
      selected_attempt_id={@selected_attempt_id}
    />
    """
  end

  @impl true
  def handle_params(params, _uri, socket) do
    active_mode = active_mode_from_params(params, socket.assigns.active_mode)
    timeline_state = timeline_state_from_params(params, socket.assigns.run)

    selected_child_run_id =
      selected_child_run_id_from_params(params, socket.assigns.run, socket.assigns.run_id)

    active_mode =
      if selected_child_run_id && active_mode == :overview, do: :windows, else: active_mode

    {:noreply,
     assign(socket,
       active_mode: active_mode,
       timeline_state: timeline_state,
       selected_child_run_id: selected_child_run_id
     )}
  end

  @impl true
  def terminate(_reason, socket) do
    if socket.assigns[:run_events_live?] do
      FavnOrchestrator.unsubscribe_run(socket.assigns.run.subscribed_run_id)
    end

    :ok
  end

  defp load_run(run_id, existing_back_asset_href \\ nil) do
    group_id = execution_group_id(run_id)

    case FavnOrchestrator.get_execution_group_detail(group_id) do
      {:ok, detail} -> detail_from_execution_group(detail, run_id, existing_back_asset_href)
      {:error, reason} -> %{id: run_id, found?: false, error: error_label(reason)}
    end
  end

  defp execution_group_id(run_id) do
    case FavnOrchestrator.get_run_detail(run_id) do
      {:ok, %{summary: %{root_run_id: root_run_id}}} when is_binary(root_run_id) ->
        root_run_id

      {:ok, %{summary: %{parent_run_id: parent_run_id}}} when is_binary(parent_run_id) ->
        parent_run_id

      {:ok, %{summary: %{id: id}}} ->
        id

      {:error, _reason} ->
        run_id
    end
  end

  defp detail_from_execution_group(
         %{summary: summary, root_run: root_run} = detail,
         _run_id,
         existing_back_asset_href
       ) do
    root_detail = root_detail(summary.id)
    attempts = Enum.map(Map.get(detail, :asset_attempts, []), &attempt_from_public/1)
    legacy_asset_results = Enum.map(Map.get(root_detail, :steps, []), &legacy_step_from_public/1)
    windows = Enum.map(Map.get(detail, :windows, []), &window_from_public/1)
    events = Map.get(detail, :events, root_detail.events)
    child_runs = child_runs_from_public(Map.get(detail, :child_runs, []), attempts, windows)
    timeline = Enum.map(Map.get(detail, :timeline, []), &timeline_from_public(&1, attempts))
    matrix = matrix(attempts, windows)
    failures = Enum.filter(attempts, &(&1.status_tone == :error))
    target = target_label(summary.target_assets)
    status = group_status(summary)

    %{
      found?: true,
      id: summary.id,
      subscribed_run_id: root_run.id,
      raw_status: status,
      active?: active_group?(summary),
      short_id: short_id(summary.id),
      title: group_title(summary),
      subtitle: subtitle([target, window_range_label(windows)]),
      status: LogsViewModel.status_label(status),
      status_tone: LogsViewModel.status_tone(status),
      target: target || "No target",
      trigger: label(summary.trigger_type),
      window: window_range_label(windows),
      started_at: LogsViewModel.timestamp_label(summary.started_at),
      finished_at: LogsViewModel.timestamp_label(summary.finished_at),
      duration: LogsViewModel.duration_ms_label(summary.duration_ms),
      elapsed_duration: duration_or_elapsed(summary),
      manifest_version_id: root_run.manifest_version_id || "Unknown",
      total_windows: summary.total_windows,
      completed_windows: summary.completed_windows,
      failed_windows: summary.failed_windows,
      total_asset_attempts: summary.total_asset_attempts,
      completed_asset_attempts: summary.completed_asset_attempts,
      succeeded_asset_attempts:
        max(summary.completed_asset_attempts - summary.failed_asset_attempts, 0),
      failed_asset_attempts: summary.failed_asset_attempts,
      running_asset_attempts: summary.running_asset_attempts,
      queued_asset_attempts: summary.queued_asset_attempts,
      progress_label:
        progress_label(summary.completed_asset_attempts, summary.total_asset_attempts),
      matrix: matrix,
      assets: matrix.assets,
      windows: matrix.windows,
      attempts: attempts,
      legacy_asset_results: legacy_asset_results,
      legacy_asset_text: legacy_asset_text(legacy_asset_results),
      failures: failures,
      child_runs: child_runs,
      timeline: timeline,
      events: Enum.map(events, &event_from_public/1),
      latest_event_summary: events |> Enum.map(&event_from_public/1) |> latest_event_summary(),
      waiting_activity?: root_detail.events == [] and active_group?(summary),
      current_activity: current_activity(attempts),
      selected_attempt: nil,
      context: context_items(summary, root_run, target, windows),
      back_asset_href:
        existing_back_asset_href || back_asset_href(List.first(summary.target_assets)),
      raw_run: inspect(detail, pretty: true, limit: 50, printable_limit: 2_000),
      raw_events: inspect(events, pretty: true, limit: 50, printable_limit: 2_000),
      root_event_sequence: latest_sequence(root_detail.events, nil)
    }
  end

  defp root_detail(run_id) do
    case FavnOrchestrator.get_run_detail(run_id) do
      {:ok, detail} -> detail
      {:error, _reason} -> %{events: []}
    end
  end

  defp maybe_schedule_refresh(%{assigns: %{run: %{active?: true}}} = socket) do
    if connected?(socket), do: Process.send_after(self(), :refresh_run, @refresh_interval_ms)
    socket
  end

  defp maybe_schedule_refresh(socket), do: socket

  defp maybe_subscribe_run(%{assigns: %{run: %{subscribed_run_id: run_id}}} = socket) do
    if connected?(socket) do
      case FavnOrchestrator.subscribe_run(run_id) do
        :ok -> socket |> assign(:run_events_live?, true) |> replay_run_event_gap()
        {:error, _reason} -> socket
      end
    else
      socket
    end
  end

  defp maybe_subscribe_run(socket), do: socket

  defp replay_run_event_gap(socket) do
    after_sequence = socket.assigns.run_event_sequence || 0

    case FavnOrchestrator.list_run_stream_events(socket.assigns.run.subscribed_run_id,
           after_sequence: after_sequence,
           limit: 200
         ) do
      {:ok, []} -> socket
      {:ok, events} -> reload_run_from_event(socket, latest_sequence(events, after_sequence))
      {:error, _reason} -> socket
    end
  end

  defp reload_run_from_event(socket, event_sequence) do
    run = load_run(socket.assigns.run_id, socket.assigns.run[:back_asset_href])

    socket
    |> assign(:run, run)
    |> assign(:run_event_sequence, latest_event_sequence(run, event_sequence))
    |> maybe_schedule_refresh()
  end

  defp patch_run_state(socket, updates) do
    active_mode = Keyword.get(updates, :active_mode, socket.assigns.active_mode)
    timeline_state = Keyword.get(updates, :timeline_state, socket.assigns.timeline_state)

    push_patch(socket,
      to: ~p"/runs/#{socket.assigns.run_id}?#{run_query_params(active_mode, timeline_state)}"
    )
  end

  defp run_query_params(:overview, _timeline_state), do: %{}

  defp run_query_params(active_mode, timeline_state) do
    Map.put(timeline_query_params(timeline_state), "view", Atom.to_string(active_mode))
  end

  defp timeline_query_params(timeline_state) do
    %{}
    |> maybe_put_param(
      "timeline_zoom",
      timeline_state.zoom,
      timeline_state.zoom != "30m" or timeline_state.mode == :manual
    )
    |> maybe_put_param(
      "timeline_mode",
      Atom.to_string(timeline_state.mode),
      timeline_state.mode != :live
    )
    |> maybe_put_param("timeline_follow", "0", timeline_state.live_follow? == false)
    |> maybe_put_param("timeline_search", timeline_state.search, timeline_state.search != "")
    |> maybe_put_param("timeline_status", timeline_state.status, timeline_state.status != "all")
    |> maybe_put_param("timeline_window", timeline_state.window, timeline_state.window != "all")
    |> maybe_put_param("timeline_failed", "1", timeline_state.failed_only?)
    |> maybe_put_param("timeline_running", "1", timeline_state.running_only?)
  end

  defp maybe_put_param(params, key, value, true), do: Map.put(params, key, value)
  defp maybe_put_param(params, _key, _value, false), do: params

  defp active_mode_from_params(%{"view" => mode}, _current) when mode in @valid_modes,
    do: String.to_existing_atom(mode)

  defp active_mode_from_params(_params, _current), do: :overview

  defp timeline_state_from_params(params, run) do
    default = default_timeline_state(run)
    mode = timeline_mode(Map.get(params, "timeline_mode"), default.mode)
    zoom = timeline_zoom(Map.get(params, "timeline_zoom"), default.zoom)

    %{
      default
      | mode: mode,
        zoom: zoom,
        live_follow?: Map.get(params, "timeline_follow") != "0" and mode == :live,
        search: Map.get(params, "timeline_search", default.search),
        status: Map.get(params, "timeline_status", default.status),
        window: Map.get(params, "timeline_window", default.window),
        failed_only?: Map.get(params, "timeline_failed") == "1",
        running_only?: Map.get(params, "timeline_running") == "1"
    }
  end

  defp default_timeline_state(%{active?: true}) do
    live_timeline_state()
  end

  defp default_timeline_state(%{running_asset_attempts: running}) when running > 0 do
    live_timeline_state()
  end

  defp default_timeline_state(%{raw_status: status}) when status in @active_statuses do
    live_timeline_state()
  end

  defp default_timeline_state(_run) do
    %{
      mode: :fit,
      zoom: "full",
      live_follow?: false,
      search: "",
      status: "all",
      window: "all",
      failed_only?: false,
      running_only?: false
    }
  end

  defp live_timeline_state do
    %{
      mode: :live,
      zoom: "30m",
      live_follow?: true,
      search: "",
      status: "all",
      window: "all",
      failed_only?: false,
      running_only?: false
    }
  end

  defp timeline_mode("fit", _default), do: :fit
  defp timeline_mode("manual", _default), do: :manual
  defp timeline_mode("live", _default), do: :live
  defp timeline_mode(_mode, default), do: default

  defp timeline_zoom(zoom, _default) when zoom in @timeline_zoom_levels, do: zoom
  defp timeline_zoom(_zoom, default), do: default

  defp selected_child_run_id_from_params(params, run, requested_run_id) do
    child_ids = MapSet.new(Enum.map(run[:child_runs] || [], & &1.id))

    cond do
      MapSet.member?(child_ids, Map.get(params, "child_run_id")) ->
        Map.get(params, "child_run_id")

      MapSet.member?(child_ids, requested_run_id) ->
        requested_run_id

      true ->
        nil
    end
  end

  defp attempt_from_public(attempt) do
    %{
      id: attempt.id,
      root_execution_group_id: attempt.root_execution_group_id,
      child_run_id: attempt.child_run_id,
      run_id: attempt.run_id,
      asset_key: attempt.asset_key,
      asset_ref: attempt.asset_ref,
      short_asset_name: LogsViewModel.display_name(attempt.asset_ref) || attempt.asset_ref,
      stage: attempt.stage,
      stage_label: stage_label(attempt.stage),
      attempt_number: attempt.attempt_number,
      started_at_raw: attempt.started_at,
      finished_at_raw: attempt.finished_at,
      duration_ms: attempt.duration_ms,
      started_at: LogsViewModel.timestamp_label(attempt.started_at),
      finished_at: LogsViewModel.timestamp_label(attempt.finished_at),
      duration: LogsViewModel.duration_ms_label(attempt.duration_ms),
      status: status_label(attempt.status),
      raw_status: attempt.status,
      status_tone: status_tone(attempt.status),
      error_summary: attempt.error_summary,
      window: window_from_public(attempt.window),
      window_id: window_identity(attempt.window),
      window_label: window_label(attempt.window) || "No window",
      logs_href: ~p"/runs/#{attempt.run_id}/assets/#{attempt.id}/logs"
    }
  end

  defp window_from_public(nil), do: nil

  defp window_from_public(window) do
    %{
      id: window_identity(window),
      key: Map.get(window, :key),
      label: window_label(window) || "No window",
      start_at: Map.get(window, :start_at),
      end_at: Map.get(window, :end_at),
      range_label: range_label(Map.get(window, :start_at), Map.get(window, :end_at)),
      status: status_label(Map.get(window, :status)),
      raw_status: Map.get(window, :status),
      status_tone: status_tone(Map.get(window, :status)),
      child_run_id: Map.get(window, :child_run_id),
      attempt_count: Map.get(window, :attempt_count),
      started_at: LogsViewModel.timestamp_label(Map.get(window, :started_at)),
      finished_at: LogsViewModel.timestamp_label(Map.get(window, :finished_at)),
      duration: LogsViewModel.duration_ms_label(Map.get(window, :duration_ms))
    }
  end

  defp child_runs_from_public(child_runs, attempts, windows) do
    Enum.map(child_runs, fn child ->
      child_attempts = Enum.filter(attempts, &(&1.run_id == child.id))

      window =
        Enum.find(windows, &(&1.child_run_id == child.id)) || window_from_public(child.window)

      completed = Enum.count(child_attempts, &terminal_status?(&1.raw_status))
      total = length(child_attempts)

      %{
        id: child.id,
        window: window,
        window_label: (window && window.label) || "No window",
        status: status_label(child.status),
        raw_status: child.status,
        status_tone: status_tone(child.status),
        progress: progress_label(completed, total),
        started_at: LogsViewModel.timestamp_label(child.started_at),
        finished_at: LogsViewModel.timestamp_label(child.finished_at),
        duration: LogsViewModel.duration_ms_label(child.duration_ms),
        succeeded_count: Enum.count(child_attempts, &(&1.raw_status == :ok)),
        failed_count: Enum.count(child_attempts, &failed_status?(&1.raw_status)),
        running_count: Enum.count(child_attempts, &running_status?(&1.raw_status)),
        queued_count: Enum.count(child_attempts, &queued_status?(&1.raw_status)),
        attempts: child_attempts
      }
    end)
  end

  defp timeline_from_public(entry, attempts) do
    attempt = Enum.find(attempts, &(&1.id == entry.attempt_id))

    %{
      attempt_id: entry.attempt_id,
      asset_key: entry.asset_key,
      asset_name: LogsViewModel.display_name(entry.asset_key) || entry.asset_key,
      child_run_id: entry.child_run_id,
      window: window_from_public(entry.window),
      window_label: window_label(entry.window) || "No window",
      status: status_label(entry.status),
      raw_status: entry.status,
      status_tone: status_tone(entry.status),
      started_at_raw: entry.started_at,
      finished_at_raw: entry.finished_at,
      started_at: LogsViewModel.timestamp_label(entry.started_at),
      finished_at: LogsViewModel.timestamp_label(entry.finished_at),
      duration: if(attempt, do: attempt.duration, else: "-")
    }
  end

  defp matrix(attempts, []),
    do: matrix(attempts, [%{id: "none", label: "No window", range_label: nil}])

  defp matrix(attempts, windows) do
    assets =
      attempts
      |> Enum.uniq_by(& &1.asset_key)
      |> Enum.map(fn attempt ->
        %{key: attempt.asset_key, name: attempt.short_asset_name, stage: attempt.stage_label}
      end)
      |> Enum.sort_by(& &1.name)

    attempts_by_window_id = Map.new(attempts, &{{&1.asset_key, &1.window_id || "none"}, &1})
    attempts_by_child_run_id = Map.new(attempts, &{{&1.asset_key, &1.child_run_id}, &1})

    rows =
      Enum.map(assets, fn asset ->
        cells =
          Enum.map(windows, fn window ->
            Map.get(attempts_by_window_id, {asset.key, window.id}) ||
              Map.get(attempts_by_child_run_id, {asset.key, Map.get(window, :child_run_id)}) ||
              pending_cell(asset, window)
          end)

        Map.put(asset, :cells, cells)
      end)

    %{assets: assets, windows: windows, rows: rows}
  end

  defp pending_cell(asset, window) do
    %{
      id: nil,
      asset_key: asset.key,
      asset_ref: asset.key,
      short_asset_name: asset.name,
      stage_label: asset.stage,
      window: window,
      window_id: window.id,
      window_label: window.label,
      status: "Queued",
      raw_status: :pending,
      status_tone: :neutral,
      started_at_raw: nil,
      finished_at_raw: nil,
      duration_ms: nil,
      duration: "-",
      started_at: "-",
      finished_at: "-",
      child_run_id: Map.get(window, :child_run_id),
      run_id: Map.get(window, :child_run_id),
      attempt_number: nil,
      error_summary: nil,
      logs_href: nil
    }
  end

  defp current_activity(attempts) do
    case Enum.find(attempts, &running_status?(&1.raw_status)) do
      nil ->
        nil

      attempt ->
        %{
          asset: attempt.short_asset_name,
          window: attempt.window_label,
          started_at: attempt.started_at,
          duration: attempt.duration,
          attempt: attempt
        }
    end
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

  defp legacy_step_from_public(step) do
    %{
      id: step.id,
      asset_ref: step.asset_ref,
      display_name: LogsViewModel.display_name(step.asset_ref) || step.asset_ref,
      status: legacy_step_status_label(step.status, Map.get(step, :failure_role)),
      stage: stage_label(step.stage),
      window: window_label(step.window) || "No window",
      error: error_summary(step.error),
      explanation: step.explanation,
      logs_href: ~p"/runs/#{Map.get(step, :run_id, step.id)}/assets/#{step.id}/logs"
    }
  end

  defp legacy_asset_text(steps) do
    steps
    |> Enum.map(fn step ->
      [
        step.display_name,
        step.asset_ref,
        step.status,
        step.stage,
        step.window,
        step.error,
        step.explanation
      ]
      |> Enum.reject(&is_nil/1)
      |> Enum.join(" ")
    end)
    |> Enum.join(" ")
  end

  defp group_title(%{trigger_type: :backfill}), do: "Backfill run"
  defp group_title(_summary), do: "Run"

  defp group_status(summary) do
    cond do
      summary.failed_asset_attempts > 0 or summary.failed_windows > 0 -> :error
      summary.running_asset_attempts > 0 or summary.root_status == :running -> :running
      summary.queued_asset_attempts > 0 or summary.root_status == :pending -> :pending
      summary.root_status == :ok -> :ok
      true -> summary.root_status
    end
  end

  defp active_group?(summary), do: group_status(summary) in @active_statuses

  defp target_label([single]), do: LogsViewModel.ref_label(single)

  defp target_label(targets) when is_list(targets) and targets != [],
    do: "#{length(targets)} selected assets"

  defp target_label(_targets), do: nil

  defp window_range_label([]), do: nil
  defp window_range_label(windows), do: Enum.map(windows, & &1.label) |> Enum.join(" -> ")

  defp context_items(summary, root_run, target, windows) do
    [
      %{label: "Backfill run", value: summary.id},
      %{label: "Manifest version", value: root_run.manifest_version_id || "Unknown"},
      %{label: "Target", value: target || "No target"},
      %{label: "Trigger", value: label(summary.trigger_type)},
      %{label: "Window range", value: window_range_label(windows) || "No window metadata"}
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

  defp duration_or_elapsed(%{duration_ms: duration_ms}) when is_integer(duration_ms),
    do: LogsViewModel.duration_ms_label(duration_ms)

  defp duration_or_elapsed(%{started_at: %DateTime{} = started_at}),
    do:
      LogsViewModel.duration_ms_label(DateTime.diff(DateTime.utc_now(), started_at, :millisecond))

  defp duration_or_elapsed(_summary), do: "-"

  defp progress_label(_done, 0), do: "0 / 0"
  defp progress_label(done, total), do: "#{done} / #{total}"
  defp stage_label(nil), do: nil
  defp stage_label(stage), do: "Stage #{stage}"
  defp subtitle(parts), do: parts |> Enum.reject(&is_nil/1) |> Enum.join(" · ")
  defp short_id(id) when is_binary(id) and byte_size(id) > 18, do: String.slice(id, 0, 18)
  defp short_id(id) when is_binary(id), do: id
  defp short_id(_id), do: "unknown"
  defp latest_event_summary([]), do: nil
  defp latest_event_summary(events), do: events |> List.last() |> Map.get(:summary)

  defp latest_event_sequence(run, fallback \\ nil)

  defp latest_event_sequence(%{root_event_sequence: sequence}, fallback)
       when is_integer(sequence),
       do: max(sequence, fallback || 0)

  defp latest_event_sequence(%{events: events}, fallback) when is_list(events) do
    events
    |> Enum.map(&Map.get(&1, :sequence))
    |> Enum.reject(&is_nil/1)
    |> Enum.max(fn -> fallback end)
  end

  defp latest_event_sequence(_run, fallback), do: fallback

  defp latest_sequence(events, fallback),
    do: events |> Enum.map(&Map.get(&1, :sequence)) |> Enum.max(fn -> fallback end)

  defp fresh_run_event?(%{sequence: sequence}, latest_sequence) when is_integer(sequence),
    do: is_nil(latest_sequence) or sequence > latest_sequence

  defp fresh_run_event?(_event, _latest_sequence), do: true

  defp window_identity(nil), do: "none"

  defp window_identity(window),
    do:
      Enum.find(
        [Map.get(window, :key), datetime_iso(Map.get(window, :start_at)), window_label(window)],
        &is_binary/1
      ) || "none"

  defp window_label(%{label: label}) when is_binary(label), do: label
  defp window_label(%{"label" => label}) when is_binary(label), do: label
  defp window_label(%{key: key}) when is_binary(key), do: key
  defp window_label(%{"key" => key}) when is_binary(key), do: key
  defp window_label(_window), do: nil
  defp datetime_iso(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)
  defp datetime_iso(_datetime), do: nil

  defp range_label(%DateTime{} = start_at, %DateTime{} = end_at),
    do: "#{Calendar.strftime(start_at, "%b %-d")} - #{Calendar.strftime(end_at, "%b %-d")}"

  defp range_label(_start_at, _end_at), do: nil

  defp terminal_status?(status),
    do:
      status in [
        :ok,
        :error,
        :partial,
        :cancelled,
        :timed_out,
        :skipped,
        :skipped_fresh,
        :blocked
      ]

  defp failed_status?(status), do: status in [:error, :failed, :timed_out, :blocked]
  defp running_status?(status), do: status in [:running, :retrying]
  defp queued_status?(status), do: status in [:pending, :queued]
  defp status_label(:ok), do: "Succeeded"
  defp status_label(:error), do: "Failed"
  defp status_label(:failed), do: "Failed"
  defp status_label(:pending), do: "Queued"
  defp status_label(:queued), do: "Queued"
  defp status_label(:running), do: "Running"
  defp status_label(:retrying), do: "Retrying"
  defp status_label(:skipped), do: "Skipped"
  defp status_label(:skipped_fresh), do: "Skipped fresh"
  defp status_label(:blocked), do: "Blocked"
  defp status_label(:partial), do: "Partial"
  defp status_label(nil), do: "Pending"
  defp status_label(status), do: LogsViewModel.status_label(status)
  defp status_tone(status) when status in [:ok], do: :success
  defp status_tone(status) when status in [:error, :failed, :timed_out, :blocked], do: :error
  defp status_tone(status) when status in [:running, :retrying], do: :info
  defp status_tone(status) when status in [:pending, :queued], do: :warning
  defp status_tone(_status), do: :neutral
  defp label(nil), do: "Unknown"
  defp label(:step_started), do: "Step submitted"
  defp label("step_started"), do: "Step submitted"
  defp label(value), do: value |> to_string() |> String.replace("_", " ") |> String.capitalize()

  defp legacy_step_status_label(_status, :cascade), do: "Cascade failed"
  defp legacy_step_status_label(status, _role) when status in [:pending, "pending"], do: "Waiting"
  defp legacy_step_status_label(status, _role) when status in [:ok, "ok"], do: "Succeeded"
  defp legacy_step_status_label(status, _role), do: status_label(status)

  defp error_summary(nil), do: nil
  defp error_summary(%{message: message}) when is_binary(message), do: message
  defp error_summary(%{"message" => message}) when is_binary(message), do: message
  defp error_summary(%{reason: reason}), do: error_summary(reason)
  defp error_summary(%{"reason" => reason}), do: error_summary(reason)
  defp error_summary(reason) when is_binary(reason), do: reason
  defp error_summary(reason) when is_atom(reason), do: label(reason)
  defp error_summary(reason), do: inspect(reason, limit: 5, printable_limit: 200)

  defp event_summary(event),
    do:
      Map.get(event.data || %{}, :message) || Map.get(event.data || %{}, "message") ||
        if(Map.get(event, :asset_ref),
          do: "Asset #{LogsViewModel.ref_label(Map.get(event, :asset_ref))}",
          else: LogsViewModel.status_label(Map.get(event, :status))
        )

  defp error_label(:not_found), do: "Run not found"
  defp error_label(_reason), do: "Run could not be loaded"
end
