defmodule FavnView.RunDetailLive do
  @moduledoc false

  use FavnView, :live_view

  alias FavnView.AssetRoute
  alias FavnView.Auth.Scope
  alias FavnView.Components.AssetCataloguePage
  alias FavnView.Components.RunDetailPage
  alias FavnView.LiveRefresh
  alias FavnView.LogsViewModel
  alias FavnView.OperatorErrorLabels
  alias FavnView.RunEventRefresh

  @refresh_interval_ms 1_500
  @coalesce_refresh_ms 100
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
        active_mode: :overview,
        timeline_state: default_timeline_state(run),
        selected_child_run_id: nil,
        selected_attempt_id: nil,
        nav_items: AssetCataloguePage.nav_items(:runs)
      )
      |> RunEventRefresh.init([:refresh_timer_ref, :fallback_poll_ref])
      |> sync_run_event_refresh()
      |> maybe_schedule_fallback_poll()

    {:ok, socket}
  end

  @impl true
  def handle_info({:refresh_run, token}, socket) do
    case LiveRefresh.take(socket, :refresh_timer_ref, token) do
      {:ok, socket} ->
        {:noreply, refresh_run(socket)}

      {:stale, socket} ->
        {:noreply, socket}
    end
  end

  def handle_info({:poll_run, token}, socket) do
    case LiveRefresh.take(socket, :fallback_poll_ref, token) do
      {:ok, socket} ->
        {:noreply, socket |> refresh_run() |> maybe_schedule_fallback_poll()}

      {:stale, socket} ->
        {:noreply, socket}
    end
  end

  def handle_info(:refresh_run, socket) do
    {:noreply, refresh_run(socket)}
  end

  def handle_info(:poll_run, socket) do
    {:noreply, socket |> refresh_run() |> maybe_schedule_fallback_poll()}
  end

  def handle_info({:favn_run_event, event}, socket) do
    {:noreply, RunEventRefresh.handle_event(socket, event, run_event_refresh_opts())}
  end

  defp refresh_run(socket) do
    run = load_run(socket.assigns.run_id, socket.assigns.run[:back_asset_href])

    socket
    |> assign(:run, run)
    |> RunEventRefresh.mark_refreshed(run_event_sequences(run))
    |> sync_run_event_refresh()
    |> maybe_schedule_fallback_poll()
  end

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

  def handle_event("cancel_run", _params, socket) do
    case socket.assigns.run do
      %{cancellable?: true, cancel_run_id: run_id} when is_binary(run_id) ->
        case FavnOrchestrator.cancel_operator_run(actor_context(socket), run_id) do
          :ok ->
            {:noreply,
             socket
             |> put_flash(:info, "Run cancellation requested")
             |> refresh_run()}

          {:error, reason} ->
            {:noreply, put_flash(socket, :error, cancel_error_label(reason))}
        end

      _run ->
        {:noreply, socket}
    end
  end

  def handle_event("retry_remaining", _params, socket) do
    case socket.assigns.run do
      %{retry_remaining?: true} ->
        case FavnOrchestrator.retry_operator_run_remaining(
               actor_context(socket),
               socket.assigns.run_id
             ) do
          {:ok, %{run_ids: run_ids, asset_count: asset_count}} ->
            {:noreply,
             socket
             |> put_flash(:info, retry_remaining_submitted_label(run_ids, asset_count))
             |> refresh_run()}

          {:partial, %{run_ids: run_ids, reason: reason}} ->
            {:noreply,
             socket
             |> put_flash(:error, retry_remaining_partial_label(run_ids, reason))
             |> refresh_run()}

          {:error, reason} ->
            {:noreply, put_flash(socket, :error, retry_remaining_error_label(reason))}
        end

      _run ->
        {:noreply, socket}
    end
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
    RunEventRefresh.unsubscribe_all(socket, &unsubscribe_run/1)

    :ok
  end

  defp load_run(run_id, existing_back_asset_href \\ nil) do
    case get_operator_run_detail(run_id, include: [:events], event_limit: 200) do
      {:ok, detail} -> detail_from_execution_group(detail, run_id, existing_back_asset_href)
      {:error, reason} -> %{id: run_id, found?: false, error: error_label(reason)}
    end
  end

  defp detail_from_execution_group(
         %{summary: summary, root_run: root_run} = detail,
         run_id,
         existing_back_asset_href
       ) do
    attempts = Enum.map(Map.get(detail, :asset_attempts, []), &attempt_from_public/1)
    legacy_asset_results = Enum.map(Map.get(detail, :steps, []), &legacy_step_from_public/1)
    windows = Enum.map(Map.get(detail, :windows, []), &window_from_public/1)
    events = Map.get(detail, :events, [])
    child_runs = child_runs_from_public(Map.get(detail, :child_runs, []), attempts, windows)
    cancel_target = cancel_target(summary, root_run, child_runs, run_id)
    timeline = Enum.map(Map.get(detail, :timeline, []), &timeline_from_public(&1, attempts))
    matrix = matrix(attempts, windows)
    failures = Enum.filter(attempts, &(&1.status_tone == :error))

    backfill_failures =
      Enum.map(Map.get(detail, :backfill_failures, []), &backfill_failure_from_public/1)

    target = target_label(summary.target_assets)
    status = group_status(summary)

    %{
      found?: true,
      id: summary.id,
      subscribed_run_id: root_run.id,
      subscribed_run_ids: subscribed_run_ids(root_run, child_runs),
      raw_status: status,
      active?: active_group?(summary),
      cancellable?: !is_nil(cancel_target),
      cancel_run_id: cancel_target && cancel_target.id,
      cancel_label: cancel_target && cancel_target.label,
      retry_remaining?: retry_remaining?(summary),
      retry_remaining_label: retry_remaining_label(summary),
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
      backfill_failures: backfill_failures,
      backfill_failure_count: Map.get(detail, :backfill_failure_count, length(backfill_failures)),
      child_runs: child_runs,
      timeline: timeline,
      events: Enum.map(events, &event_from_public/1),
      latest_event_summary: latest_event_summary(detail, events),
      waiting_activity?: events == [] and active_group?(summary),
      current_activity: current_activity(attempts),
      selected_attempt: nil,
      context: context_items(summary, root_run, target, windows),
      back_asset_href:
        existing_back_asset_href || back_asset_href(List.first(summary.target_assets)),
      raw_run: inspect(detail, pretty: true, limit: 50, printable_limit: 2_000),
      raw_events: inspect(events, pretty: true, limit: 50, printable_limit: 2_000),
      root_event_sequence: Map.get(detail, :root_event_sequence),
      run_event_sequences: run_event_sequences_from_public(root_run, child_runs, events, detail)
    }
  end

  defp maybe_schedule_fallback_poll(
         %{assigns: %{run_events_live?: false, run: %{active?: true}}} = socket
       ) do
    if connected?(socket) do
      LiveRefresh.schedule_once(socket, :fallback_poll_ref, :poll_run, @refresh_interval_ms)
    else
      socket
    end
  end

  defp maybe_schedule_fallback_poll(%{assigns: %{run: %{active?: true}}} = socket) do
    if connected?(socket) and needs_discovery_poll?(socket.assigns.run) do
      LiveRefresh.schedule_once(socket, :fallback_poll_ref, :poll_run, @refresh_interval_ms)
    else
      socket
    end
  end

  defp maybe_schedule_fallback_poll(socket), do: socket

  defp sync_run_event_refresh(%{assigns: %{run: run}} = socket) do
    RunEventRefresh.sync_subscriptions(
      socket,
      Map.get(run, :subscribed_run_ids, []),
      run_event_sequences(run),
      run_event_refresh_opts()
    )
  end

  defp sync_run_event_refresh(socket), do: socket

  defp needs_discovery_poll?(%{total_windows: total, child_runs: child_runs})
       when is_integer(total) and is_list(child_runs),
       do: total > length(child_runs)

  defp needs_discovery_poll?(_run), do: false

  defp actor_context(socket) do
    %Scope{} = scope = socket.assigns.current_scope

    case FavnOrchestrator.operator_context(scope.actor, scope.session, source: :live_view) do
      {:ok, context} -> context
      {:error, _reason} -> %{}
    end
  end

  defp get_operator_run_detail(run_id, opts) do
    Application.get_env(
      :favn_view,
      :operator_run_detail_fun,
      &FavnOrchestrator.get_operator_run_detail/2
    ).(run_id, opts)
  end

  defp subscribe_run(run_id) do
    Application.get_env(:favn_view, :run_subscribe_fun, &FavnOrchestrator.subscribe_run/1).(
      run_id
    )
  end

  defp unsubscribe_run(run_id), do: FavnOrchestrator.unsubscribe_run(run_id)

  defp list_run_stream_events(run_id, opts) do
    Application.get_env(
      :favn_view,
      :run_stream_events_fun,
      &FavnOrchestrator.list_run_stream_events/2
    ).(run_id, opts)
  end

  defp subscribed_run_ids(root_run, child_runs) do
    [root_run.id | Enum.map(child_runs, & &1.id)]
    |> Enum.filter(&(is_binary(&1) and &1 != ""))
    |> Enum.uniq()
  end

  defp run_event_sequences(%{run_event_sequences: sequences}) when is_map(sequences),
    do: sequences

  defp run_event_sequences(_run), do: %{}

  defp run_event_sequences_from_public(root_run, child_runs, events, detail) do
    event_sequences =
      events
      |> Enum.reduce(%{}, fn event, acc ->
        run_id = Map.get(event, :run_id)
        sequence = Map.get(event, :sequence)

        if is_binary(run_id) and is_integer(sequence) do
          Map.update(acc, run_id, sequence, &max(&1, sequence))
        else
          acc
        end
      end)

    child_sequences =
      child_runs
      |> Enum.reduce(%{}, fn child, acc ->
        if is_integer(Map.get(child, :event_seq)) do
          Map.put(acc, child.id, child.event_seq)
        else
          acc
        end
      end)

    event_sequences
    |> Map.merge(child_sequences, fn _run_id, left, right -> max(left, right) end)
    |> maybe_put_sequence(root_run.id, Map.get(detail, :root_event_sequence))
  end

  defp maybe_put_sequence(sequences, run_id, sequence)
       when is_binary(run_id) and is_integer(sequence),
       do: Map.update(sequences, run_id, sequence, &max(&1, sequence))

  defp maybe_put_sequence(sequences, _run_id, _sequence), do: sequences

  defp run_event_refresh_opts do
    [
      subscribe_fun: &subscribe_run/1,
      unsubscribe_fun: &unsubscribe_run/1,
      list_events_fun: &list_run_stream_events/2,
      refresh_key: :refresh_timer_ref,
      refresh_message: :refresh_run,
      coalesce_ms: @coalesce_refresh_ms
    ]
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
      execution_pool: Map.get(attempt, :execution_pool),
      queue_reason: Map.get(attempt, :queue_reason),
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
      output_metadata: Map.get(attempt, :output_metadata),
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

  defp backfill_failure_from_public(failure) do
    window = window_from_public(Map.get(failure, :window))
    status = Map.get(failure, :status)
    asset_ref = Map.get(failure, :asset_ref)
    child_run_id = Map.get(failure, :child_run_id)

    %{
      id: child_run_id || "backfill-window-#{window_identity(window)}",
      child_run_id: child_run_id,
      asset_ref: asset_ref,
      short_asset_name:
        LogsViewModel.display_name(asset_ref) || LogsViewModel.ref_label(asset_ref) ||
          "Window run",
      window: window,
      window_id: window_identity(window),
      window_label: window_label(window) || "No window",
      status: status_label(status),
      raw_status: status,
      status_tone: status_tone(status),
      error_summary:
        error_summary(Map.get(failure, :error)) || OperatorErrorLabels.run_failure_detail(nil),
      attempt_count: Map.get(failure, :attempt_count),
      started_at: LogsViewModel.timestamp_label(Map.get(failure, :started_at)),
      finished_at: LogsViewModel.timestamp_label(Map.get(failure, :finished_at)),
      duration: LogsViewModel.duration_ms_label(Map.get(failure, :duration_ms))
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

  defp retry_remaining?(%{status: status, failed_asset_attempts: failed})
       when status in [:error, :partial] and failed > 0,
       do: true

  defp retry_remaining?(_summary), do: false

  defp retry_remaining_label(%{failed_asset_attempts: 1}), do: "Retry 1 remaining asset"

  defp retry_remaining_label(%{failed_asset_attempts: count}),
    do: "Retry #{count} remaining assets"

  defp active_group?(summary), do: Map.get(summary, :active?, false)

  defp cancel_target(summary, root_run, child_runs, run_id) do
    cond do
      active_child = active_child_run(child_runs, run_id) ->
        %{id: active_child.id, label: "Cancel window run"}

      active_group?(summary) and Map.get(root_run, :submit_kind) != :backfill_pipeline ->
        %{id: root_run.id, label: "Cancel run"}

      true ->
        nil
    end
  end

  defp active_child_run(child_runs, run_id) do
    Enum.find(child_runs, fn child ->
      child.id == run_id and child.raw_status in @active_statuses
    end)
  end

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

  defp cancel_error_label(reason), do: OperatorErrorLabels.run_cancel(reason)

  defp retry_remaining_submitted_label(run_ids, asset_count) do
    run_label = if(length(run_ids) == 1, do: "1 retry run", else: "#{length(run_ids)} retry runs")
    asset_label = if(asset_count == 1, do: "1 asset", else: "#{asset_count} assets")
    "Submitted #{run_label} for #{asset_label}"
  end

  defp retry_remaining_partial_label(run_ids, _reason) do
    run_label =
      if(length(run_ids) == 1, do: "1 retry run was", else: "#{length(run_ids)} retry runs were")

    "Retry submission partially succeeded: #{run_label} submitted before a later retry failed"
  end

  defp retry_remaining_error_label(:no_remaining_work), do: "No remaining assets to retry"
  defp retry_remaining_error_label({:run_not_retryable, _status}), do: "Run is not retryable"
  defp retry_remaining_error_label(_reason), do: "Remaining assets could not be retried"

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

  defp latest_event_summary(%{latest_event: latest_event}, _events) when not is_nil(latest_event),
    do: latest_event |> event_from_public() |> Map.get(:summary)

  defp latest_event_summary(_detail, events),
    do: events |> Enum.map(&event_from_public/1) |> latest_event_summary()

  defp latest_event_summary([]), do: nil
  defp latest_event_summary(events), do: events |> List.last() |> Map.get(:summary)

  defp legacy_step_status_label(_status, :cascade), do: "Cascade failed"
  defp legacy_step_status_label(status, _role) when status in [:pending, "pending"], do: "Waiting"
  defp legacy_step_status_label(status, _role) when status in [:ok, "ok"], do: "Succeeded"
  defp legacy_step_status_label(status, _role), do: status_label(status)

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
  defp status_label(:skipped_fresh), do: "Skipped"
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

  defp error_summary(nil), do: nil
  defp error_summary(%{message: message}) when is_binary(message), do: message
  defp error_summary(%{"message" => message}) when is_binary(message), do: message

  defp error_summary(%{reason: reason}), do: error_summary(reason)
  defp error_summary(%{"reason" => reason}), do: error_summary(reason)

  defp error_summary(reason) when is_binary(reason),
    do: OperatorErrorLabels.run_failure_detail(reason)

  defp error_summary(reason) when is_atom(reason), do: label(reason)
  defp error_summary(reason), do: OperatorErrorLabels.run_failure_detail(reason)

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
