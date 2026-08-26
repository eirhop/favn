defmodule FavnView.RunDetailLive do
  @moduledoc false

  use FavnView, :live_view

  alias FavnView.AssetRoute
  alias FavnView.Auth.Scope
  alias FavnView.CommandAttempt
  alias FavnView.Components.AssetCataloguePage
  alias FavnView.Components.RunDetailPage
  alias FavnView.LiveRefresh
  alias FavnView.LogsViewModel
  alias FavnView.OperatorErrorLabels
  alias FavnView.Orchestrator
  alias FavnView.RunComparison
  alias FavnView.RunEventRefresh
  alias FavnView.RunTimeline
  alias FavnView.RunWindowRail
  alias FavnView.WindowFailures
  alias FavnView.WindowLabel

  @fallback_refresh_ms 5_000
  @coalesce_refresh_ms 1_000
  @compare_read_timeout_ms 5_000
  @window_failure_limit 500
  @valid_modes ~w(flow events)
  @valid_flow_views ~w(chart table)
  @flow_outcomes ~w(succeeded failed running waiting)
  @flow_sorts ~w(start name)
  @flow_alignments ~w(window wall_clock)
  @step_keys ~w(ArrowLeft ArrowRight)
  @compare_limit RunWindowRail.compare_limit()
  @backfill_submit_kinds [:backfill_asset, :backfill_pipeline]
  @backfill_group_fields [
    :active?,
    :raw_status,
    :status,
    :status_tone,
    :title,
    :started_at,
    :finished_at,
    :elapsed_duration,
    :total_windows,
    :completed_windows,
    :failed_windows,
    :running_windows,
    :queued_windows,
    :cancelled_windows,
    :total_asset_attempts,
    :completed_asset_attempts,
    :succeeded_asset_attempts,
    :skipped_asset_attempts,
    :failed_asset_attempts,
    :running_asset_attempts,
    :queued_asset_attempts,
    :planned_asset_attempts,
    :group_loaded?
  ]

  @impl true
  def mount(%{"run_id" => run_id} = params, _session, socket) do
    active_mode = active_mode_from_params(params)

    socket =
      socket
      |> assign(
        run_id: run_id,
        run: loading_run(run_id),
        active_mode: active_mode,
        flow_view: :chart,
        flow_filter: [],
        flow_sort: :start,
        flow_alignment: :window,
        expanded_bands: [],
        compare?: false,
        compare_run_ids: [],
        compare_windows: %{},
        compare_limit_reached?: false,
        compare_error: nil,
        # A backfill parent opens its earliest window once, on arrival. Set here
        # rather than derived, so navigating deliberately back to the parent
        # later in the same session shows the parent instead of bouncing.
        window_opened?: false,
        cancel_attempt: nil,
        retry_attempt: nil,
        nav_items: AssetCataloguePage.nav_items(:runs)
      )
      |> reset_windows()
      |> RunEventRefresh.init([:refresh_timer_ref, :fallback_poll_ref])

    socket =
      if connected?(socket) do
        socket
        |> sync_run_subscription()
        |> refresh_run()
      else
        socket
      end

    {:ok, socket}
  end

  @impl true
  def handle_params(%{"run_id" => run_id} = params, _uri, socket) do
    active_mode = active_mode_from_params(params)

    cond do
      not connected?(socket) ->
        {:noreply, assign(socket, active_mode: active_mode, run_id: run_id)}

      run_id != socket.assigns.run_id ->
        {:noreply, select_run(socket, run_id, active_mode)}

      active_mode != socket.assigns.active_mode ->
        {:noreply, socket |> assign(:active_mode, active_mode) |> refresh_run()}

      true ->
        {:noreply, socket |> assign(:active_mode, active_mode) |> open_first_window()}
    end
  end

  # A run switch resets the page. The previous run's data must never render
  # under the new run's URL, including when the new run's first read fails, so
  # the keep-last-good behaviour starts over rather than carrying across.
  defp select_run(socket, run_id, active_mode) do
    socket
    |> assign(
      run_id: run_id,
      run: loading_run(run_id),
      active_mode: active_mode,
      expanded_bands: [],
      compare_run_ids: if(socket.assigns.compare?, do: [run_id], else: []),
      compare_windows: %{},
      compare_limit_reached?: false,
      compare_error: nil,
      cancel_attempt: nil,
      retry_attempt: nil
    )
    |> reset_windows(keep_loaded: true)
    |> sync_run_subscription()
    |> refresh_run()
  end

  @impl true
  def handle_info({:refresh_run, token}, socket) do
    case LiveRefresh.take(socket, :refresh_timer_ref, token) do
      {:ok, socket} -> {:noreply, refresh_run(socket)}
      {:stale, socket} -> {:noreply, socket}
    end
  end

  def handle_info({:poll_run, token}, socket) do
    case LiveRefresh.take(socket, :fallback_poll_ref, token) do
      {:ok, %{assigns: %{refresh_timer_ref: refresh_ref}} = socket}
      when not is_nil(refresh_ref) ->
        {:noreply, socket}

      {:ok, socket} ->
        {:noreply, refresh_run(socket)}

      {:stale, socket} ->
        {:noreply, socket}
    end
  end

  def handle_info({:favn_run_event, event}, socket) do
    socket = RunEventRefresh.handle_event(socket, event, run_event_refresh_opts(socket))

    socket =
      if socket.assigns.refresh_timer_ref do
        assign(socket, :fallback_poll_ref, nil)
      else
        socket
      end

    {:noreply, socket}
  end

  # The run page deliberately does not subscribe to the global persistence topic.
  def handle_info(:favn_persistence_published, socket), do: {:noreply, socket}

  @impl true
  def handle_event("set_mode", %{"mode" => mode}, socket) when mode in @valid_modes do
    {:noreply, push_patch(socket, to: ~p"/runs/#{socket.assigns.run_id}?view=#{mode}")}
  end

  # Chart or table is a reading preference, not a different run: it changes no
  # URL and issues no read, and it survives a run switch the way a zoom level
  # would.
  def handle_event("set_flow_view", %{"view" => view}, socket) when view in @valid_flow_views do
    {:noreply, assign(socket, :flow_view, String.to_existing_atom(view))}
  end

  def handle_event("set_flow_view", _params, socket), do: {:noreply, socket}

  def handle_event("toggle_flow_filter", %{"outcome" => outcome}, socket)
      when outcome in @flow_outcomes do
    outcome = String.to_existing_atom(outcome)
    filter = socket.assigns.flow_filter

    filter = if outcome in filter, do: List.delete(filter, outcome), else: [outcome | filter]

    {:noreply, socket |> assign(:flow_filter, filter) |> assign_chart()}
  end

  def handle_event("toggle_flow_filter", _params, socket), do: {:noreply, socket}

  def handle_event("set_flow_sort", %{"sort" => sort}, socket) when sort in @flow_sorts do
    {:noreply, socket |> assign(:flow_sort, String.to_existing_atom(sort)) |> assign_chart()}
  end

  def handle_event("set_flow_sort", _params, socket), do: {:noreply, socket}

  # Alignment is view state over rows the page already holds. Asking for wall
  # clock across windows too far apart is not refused here; the comparison falls
  # back and the control says why.
  def handle_event("set_flow_alignment", %{"alignment" => alignment}, socket)
      when alignment in @flow_alignments do
    {:noreply,
     socket
     |> assign(:flow_alignment, String.to_existing_atom(alignment))
     |> assign_comparison()}
  end

  def handle_event("set_flow_alignment", _params, socket), do: {:noreply, socket}

  # A dense chart collapses its stages. Expanding one leaves the others
  # collapsed, so a wide run opens the stage in question rather than everything.
  def handle_event("toggle_flow_band", %{"band" => band}, socket) when is_binary(band) do
    expanded = socket.assigns.expanded_bands

    expanded = if band in expanded, do: List.delete(expanded, band), else: [band | expanded]

    {:noreply, socket |> assign(:expanded_bands, expanded) |> assign_chart()}
  end

  def handle_event("toggle_flow_band", _params, socket), do: {:noreply, socket}

  def handle_event("select_window", %{"run_id" => run_id}, socket) do
    {:noreply, patch_to_window(socket, run_id)}
  end

  # Entering compare mode seeds the comparison with the open run and leaving it
  # empties the selection, so the page returns to exactly its single-window
  # behaviour with nothing left subscribed or loaded on its behalf.
  def handle_event("toggle_compare", _params, socket) do
    compare? = not socket.assigns.compare?

    {:noreply,
     socket
     |> assign(
       compare?: compare?,
       compare_run_ids: if(compare?, do: [socket.assigns.run_id], else: []),
       compare_limit_reached?: false,
       compare_error: nil
     )
     |> build_rail()
     |> update_comparison()}
  end

  # The open run anchors its own comparison: dropping it would leave the page
  # drawing windows it is not on. Every other window toggles, up to the limit,
  # which refuses rather than silently discarding the click.
  def handle_event("toggle_compare_window", %{"run_id" => run_id}, socket)
      when is_binary(run_id) do
    selected = socket.assigns.compare_run_ids

    cond do
      not socket.assigns.compare? ->
        {:noreply, socket}

      run_id == socket.assigns.run_id ->
        {:noreply, socket}

      run_id in selected ->
        {:noreply, select_compare(socket, List.delete(selected, run_id))}

      length(selected) >= @compare_limit ->
        {:noreply, assign(socket, :compare_limit_reached?, true)}

      known_window?(socket, run_id) ->
        {:noreply, select_compare(socket, [run_id | selected])}

      true ->
        {:noreply, put_flash(socket, :error, "That window run is not available")}
    end
  end

  def handle_event("toggle_compare_window", _params, socket), do: {:noreply, socket}

  def handle_event("open_window_bucket", %{"bucket" => bucket}, socket) do
    {:noreply, socket |> assign(:open_bucket, bucket) |> build_rail()}
  end

  def handle_event("step_window", %{"key" => key}, socket) when key in @step_keys do
    {:noreply, patch_to_window(socket, stepped_run_id(socket, key))}
  end

  def handle_event("step_window", _params, socket), do: {:noreply, socket}

  def handle_event("cancel_run", params, socket) do
    case socket.assigns.run do
      %{cancellable?: true, cancel_run_id: run_id} when is_binary(run_id) ->
        attempt = CommandAttempt.next(socket.assigns.cancel_attempt, "run_cancel", run_id, params)
        socket = assign(socket, :cancel_attempt, attempt)

        case Orchestrator.cancel_operator_run(actor_context(socket), run_id,
               idempotency_key: attempt.key
             ) do
          :ok ->
            {:noreply,
             socket
             |> CommandAttempt.acknowledge(attempt)
             |> assign(:cancel_attempt, nil)
             |> put_flash(:info, "Run cancellation requested")
             |> refresh_run()}

          {:error, reason} ->
            {socket, attempt} = CommandAttempt.settle_failure(socket, attempt, reason)

            {:noreply,
             socket
             |> assign(:cancel_attempt, attempt)
             |> put_flash(:error, OperatorErrorLabels.run_cancel(reason))}
        end

      _run ->
        {:noreply, put_flash(socket, :error, "This run can no longer be cancelled")}
    end
  end

  def handle_event("retry_remaining", params, socket) do
    case socket.assigns.run do
      %{retry_remaining?: true} ->
        attempt =
          CommandAttempt.next(
            socket.assigns.retry_attempt,
            "run_retry_remaining",
            socket.assigns.run_id,
            params
          )

        socket = assign(socket, :retry_attempt, attempt)

        case Orchestrator.retry_operator_run_remaining(
               actor_context(socket),
               socket.assigns.run_id,
               idempotency_key: attempt.key
             ) do
          {:ok, %{run_ids: run_ids, asset_count: asset_count}} ->
            {:noreply,
             socket
             |> CommandAttempt.acknowledge(attempt)
             |> assign(:retry_attempt, nil)
             |> put_flash(:info, retry_success_label(run_ids, asset_count))
             |> refresh_run()}

          {:partial, %{run_ids: run_ids}} ->
            {:noreply,
             socket
             |> CommandAttempt.acknowledge(attempt)
             |> assign(:retry_attempt, nil)
             |> put_flash(
               :error,
               "Submitted #{length(run_ids)} retry runs before a later retry failed"
             )
             |> refresh_run()}

          {:error, reason} ->
            {socket, attempt} = CommandAttempt.settle_failure(socket, attempt, reason)

            {:noreply,
             socket
             |> assign(:retry_attempt, attempt)
             |> put_flash(:error, retry_error_label(reason))}
        end

      _run ->
        {:noreply, socket}
    end
  end

  def handle_event("set_mode", _params, socket), do: {:noreply, socket}

  @impl true
  def render(assigns) do
    ~H"""
    <RunDetailPage.run_detail_page
      run={@run}
      run_id={@run_id}
      nav_items={@nav_items}
      current_scope={@current_scope}
      operator_workspaces={@operator_workspaces}
      active_mode={@active_mode}
      flow_view={@flow_view}
      flow_filter={@flow_filter}
      flow_sort={@flow_sort}
      rail={@rail}
      compare?={@compare?}
      compare_limit_reached?={@compare_limit_reached?}
      compare_error={@compare_error}
      windows_error={@windows_error}
      window_failures={@window_failures}
      window_failures_overflow?={@window_failures_overflow?}
      window_failures_error={@window_failures_error}
      flash={@flash}
    />
    """
  end

  @impl true
  def terminate(_reason, socket) do
    operator_context = operator_context(socket)
    RunEventRefresh.unsubscribe_all(socket, &unsubscribe_run(operator_context, &1))
    _ = Orchestrator.unsubscribe_run_wakeups(operator_context)
    :ok
  end

  defp refresh_run(socket) do
    # The pending sequences name the compared windows that changed. They are
    # read here, before the cycle marks itself done: marking clears the whole
    # map, so a later read would no longer know what it was supposed to fetch.
    pending = pending_sequences(socket)

    {refresh_result, loaded_run} = load_run(socket, socket.assigns.active_mode)
    run = preserve_visible_run(loaded_run, socket.assigns.run)

    socket
    |> assign(:run, run)
    |> assign_chart()
    |> sync_run_subscription()
    |> refresh_comparison(pending)
    |> assign_comparison()
    |> record_refresh(refresh_result, loaded_run)
    |> refresh_windows()
    |> schedule_fallback()
  end

  # An operator changing the selection loads what the selection now needs
  # without re-reading the open run, which the page has already loaded.
  #
  # The subscription is taken before the read, as it is at mount: an event
  # emitted between a window's read and its subscription would otherwise be lost,
  # and nothing replays on subscribe.
  defp update_comparison(socket) do
    socket
    |> sync_run_subscription()
    |> refresh_comparison(pending_sequences(socket))
    |> assign_comparison()
    |> schedule_fallback()
  end

  defp pending_sequences(socket),
    do: Map.get(socket.assigns, :pending_run_event_sequences, %{})

  # The chart is a function of the loaded rows and the reading controls, so it is
  # rebuilt where either changes and never inside the template.
  defp assign_chart(socket) do
    case socket.assigns.run do
      %{assets: assets} when is_list(assets) ->
        chart =
          assets
          |> filter_assets(socket.assigns.flow_filter)
          |> RunTimeline.build(
            expanded: socket.assigns.expanded_bands,
            sort: socket.assigns.flow_sort
          )

        assign(socket, :run, Map.put(socket.assigns.run, :chart, chart))

      _other ->
        socket
    end
  end

  # The comparison is built from the windows the page holds, in the track order
  # the selection fixed. A comparison of the open window alone is not one, so
  # the page keeps drawing the single-run chart until a second window loads.
  defp assign_comparison(socket) do
    windows = socket.assigns[:compare_windows] || %{}

    comparison =
      if socket.assigns.compare? and map_size(windows) > 1 do
        RunComparison.build(Map.values(windows), alignment: socket.assigns.flow_alignment)
      end

    assign(socket, :run, Map.put(socket.assigns.run, :comparison, comparison))
  end

  # A filter narrows what the chart draws, never what the run did: the counts
  # above the chart come from the run's own totals and stay whole.
  defp filter_assets(assets, []), do: assets

  defp filter_assets(assets, outcomes),
    do: Enum.filter(assets, &(RunTimeline.outcome(Map.get(&1, :state)) in outcomes))

  # A comparison holds at most the selection limit, and the open run is never one
  # of the reads: the page has just loaded it. So a cycle issues at most one
  # fewer read than the limit, however many events arrived to provoke it.
  defp refresh_comparison(%{assigns: %{compare?: false}} = socket, _pending),
    do: assign(socket, :compare_windows, %{})

  defp refresh_comparison(socket, pending) do
    selection = compare_selection(socket)
    loaded = socket.assigns[:compare_windows] || %{}

    results =
      selection
      |> Enum.reject(&(&1 == socket.assigns.run_id))
      |> Enum.filter(&read_window?(&1, loaded, pending))
      |> read_windows(socket)

    windows =
      selection
      |> Enum.with_index(1)
      |> Map.new(fn {run_id, track} ->
        {run_id, compare_window(socket, run_id, track, Map.get(loaded, run_id), results)}
      end)

    socket |> assign(:compare_windows, windows) |> fall_back_when_lost(windows, loaded)
  end

  # The bound is applied here, where the reads are issued, and not only in the
  # rail: a selection that somehow grew past the limit still costs one cycle's
  # worth of reads.
  defp compare_selection(socket), do: Enum.take(socket.assigns.compare_run_ids, @compare_limit)

  # A window is re-read when its events moved past what the page holds, and
  # whenever it is not loaded — which is how a first selection loads and how a
  # window that failed last cycle gets another attempt.
  #
  # `retry?` covers the case the pending map cannot: a read that never answered
  # leaves the window loaded and looking current, while `mark_refreshed` folds
  # the event sequence it was supposed to fetch into the seen map and clears the
  # pending entry. Without the flag that window would never be read again.
  defp read_window?(run_id, loaded, pending) do
    case Map.get(loaded, run_id) do
      %{state: :loaded, retry?: false, event_sequence: sequence} ->
        Map.get(pending, run_id, 0) > (sequence || 0)

      _absent_unloaded_or_unserved ->
        true
    end
  end

  # A window the page holds no current answer for, so a cycle owes it a read.
  defp unserved?(%{state: :loaded, retry?: retry?}), do: retry?
  defp unserved?(_window), do: true

  # The bound a compare read gets before the cycle gives up on it. It is a seam
  # so a test can prove the unserved-read path rather than wait out the real one.
  defp compare_read_timeout_ms,
    do: Application.get_env(:favn_view, :compare_read_timeout_ms, @compare_read_timeout_ms)

  defp read_windows([], _socket), do: %{}

  defp read_windows(run_ids, socket) do
    context = operator_context(socket)

    run_ids
    |> Task.async_stream(&get_run_flow(context, &1),
      max_concurrency: @compare_limit,
      timeout: compare_read_timeout_ms(),
      on_timeout: :kill_task
    )
    |> Enum.zip(run_ids)
    |> Map.new(fn {outcome, run_id} -> {run_id, outcome} end)
  end

  # The open run's track is drawn from what the page already loaded rather than
  # from a second read of the same run.
  defp compare_window(%{assigns: %{run_id: run_id, run: run}}, run_id, track, _previous, _results) do
    %{
      run_id: run_id,
      track: track,
      state: :loaded,
      label: run[:window] || "This run",
      title: run[:window_title],
      status: run[:status],
      assets: Map.get(run, :assets) || [],
      event_sequence: run |> run_event_sequences() |> Map.get(run_id),
      reason: nil,
      retry?: false,
      selected?: true
    }
  end

  defp compare_window(socket, run_id, track, previous, results) do
    case Map.get(results, run_id) do
      {:ok, {:ok, %{kind: :run, detail: detail}}} ->
        loaded_window(socket, run_id, track, detail)

      {:ok, {:ok, %{kind: :submission}}} ->
        blank_window(run_id, track, :unavailable, :not_started)

      {:ok, {:error, reason}} ->
        blank_window(run_id, track, :unavailable, reason_class(reason))

      # A read that never answered leaves the last good result standing, exactly
      # as the single-window view does, and marks the window owed another read so
      # the last good result cannot become the permanent one.
      {:exit, _reason} ->
        unserved(previous || blank_window(run_id, track, :loading, nil), track)

      nil ->
        %{(previous || blank_window(run_id, track, :loading, nil)) | track: track}
    end
  end

  defp unserved(window, track), do: %{window | track: track, retry?: true}

  defp loaded_window(socket, run_id, track, %{header: header, assets: assets}) do
    %{
      run_id: run_id,
      track: track,
      state: :loaded,
      label:
        window_label(header.window_start_at, header.window_end_at, socket.assigns.current_scope) ||
          LogsViewModel.status_label(header.status),
      title:
        window_title(header.window_start_at, header.window_end_at, socket.assigns.current_scope),
      status: LogsViewModel.status_label(header.status),
      assets: Enum.map(assets, &Map.from_struct/1),
      event_sequence: header.event_sequence,
      reason: nil,
      retry?: false,
      selected?: false
    }
  end

  defp blank_window(run_id, track, state, reason) do
    %{
      run_id: run_id,
      track: track,
      state: state,
      label: nil,
      status: nil,
      assets: [],
      event_sequence: nil,
      reason: reason,
      retry?: false,
      selected?: false
    }
  end

  # A comparison that lost every window it was drawing is no longer a
  # comparison, so the page says so once and returns to the single-window view
  # rather than showing one track and a column of apologies. A window that fails
  # the moment it is added has lost nothing: it is marked unavailable in place,
  # where the operator can retry it or pick another.
  defp fall_back_when_lost(socket, windows, previous) do
    others = Enum.reject(Map.values(windows), & &1.selected?)
    lost? = Enum.any?(others, &match?(%{state: :loaded}, Map.get(previous, &1.run_id)))

    if lost? and Enum.all?(others, &(&1.state == :unavailable)) do
      socket
      |> assign(
        compare?: false,
        compare_run_ids: [],
        compare_windows: %{},
        compare_error: "No compared window could be read. Showing this window on its own."
      )
      |> build_rail()
    else
      socket
    end
  end

  defp compare_sequences(socket) do
    socket.assigns
    |> Map.get(:compare_windows, %{})
    |> Enum.flat_map(fn
      {run_id, %{event_sequence: sequence}} when is_integer(sequence) -> [{run_id, sequence}]
      _unloaded -> []
    end)
    |> Map.new()
  end

  defp reason_class(:not_found), do: :not_found
  defp reason_class(reason) when is_atom(reason), do: reason
  defp reason_class(_reason), do: :unavailable

  defp record_refresh(socket, :ok, run) do
    RunEventRefresh.mark_refreshed(
      socket,
      Map.merge(run_event_sequences(run), compare_sequences(socket))
    )
  end

  defp record_refresh(socket, :error, _run) do
    RunEventRefresh.retry_pending(socket, run_event_refresh_opts(socket))
  end

  defp preserve_visible_run(
         %{found?: false, submission?: false, initializing?: false, error: error},
         %{found?: true} = visible_run
       ) do
    Map.put(visible_run, :refresh_error, error)
  end

  defp preserve_visible_run(loaded_run, _previous_run), do: Map.delete(loaded_run, :refresh_error)

  defp load_run(socket, :events) do
    case get_run_events(operator_context(socket), socket.assigns.run_id) do
      {:ok, %{kind: :run, header: header, events: events}} ->
        run =
          run_from_header(
            header,
            socket,
            Enum.map(events, &event_row(&1, socket.assigns.current_scope))
          )

        {:ok, maybe_load_backfill_group(run, socket)}

      {:ok, %{kind: :submission, submission: submission}} ->
        {:ok, submission_from_public(submission, socket.assigns.current_scope)}

      {:error, reason} ->
        {:error, error_run(socket.assigns.run_id, reason)}
    end
  end

  defp load_run(socket, :flow) do
    case get_run_flow(operator_context(socket), socket.assigns.run_id) do
      {:ok, %{kind: :run, detail: flow}} ->
        run = flow_from_public(flow, socket)
        {:ok, maybe_load_backfill_group(run, socket)}

      {:ok, %{kind: :submission, submission: submission}} ->
        {:ok, submission_from_public(submission, socket.assigns.current_scope)}

      {:error, reason} ->
        {:error, error_run(socket.assigns.run_id, reason)}
    end
  end

  defp flow_from_public(%{header: header, assets: assets, overflow?: overflow?}, socket) do
    run = run_from_header(header, socket, [])

    scope = socket.assigns.current_scope

    # The row keeps its raw timing beside the label it renders. The table reads
    # the label; the chart measures the instants. Formatting in place would have
    # left the chart parsing its own page's strings back into times.
    assets =
      Enum.map(assets, fn asset ->
        asset
        |> Map.from_struct()
        |> Map.put(:started_label, timestamp_label(asset.started_at, scope))
        |> Map.put(:finished_label, timestamp_label(asset.finished_at, scope))
      end)

    %{run | assets: assets, asset_attempts_truncated?: overflow?}
  end

  defp run_from_header(header, socket, events) do
    counts = header.counts
    duration_ms = duration_ms(header.started_at, header.finished_at)

    window_label =
      window_label(header.window_start_at, header.window_end_at, socket.assigns.current_scope)

    total_windows = if header.window_start_at, do: 1, else: 0
    completed_windows = if total_windows == 1 and not header.active?, do: 1, else: 0
    failed_windows = if total_windows == 1 and counts.failed > 0, do: 1, else: 0

    %{
      id: header.run_id,
      root_run_id: header.root_run_id,
      parent_run_id: header.parent_run_id,
      submit_kind: header.submit_kind,
      backfill_parent?:
        header.submit_kind in [:backfill_asset, :backfill_pipeline] and
          header.run_id == header.root_run_id,
      found?: true,
      submission?: false,
      active?: header.active?,
      raw_status: header.status,
      status: LogsViewModel.status_label(header.status),
      status_tone: LogsViewModel.status_tone(header.status),
      cancellable?: header.cancellable?,
      cancel_run_id: header.run_id,
      cancel_label: "Cancel run",
      retry_remaining?: header.retry_remaining?,
      retry_remaining_label: retry_label(counts.failed),
      title: if(header.trigger_type == :backfill, do: "Backfill run", else: "Run"),
      subtitle: [header.target_label, window_label] |> Enum.reject(&is_nil/1) |> Enum.join(" · "),
      target: header.target_label || "No target",
      trigger: label(header.trigger_type),
      window: window_label,
      window_title:
        window_title(header.window_start_at, header.window_end_at, socket.assigns.current_scope),
      started_at: timestamp_label(header.started_at, socket.assigns.current_scope),
      finished_at: timestamp_label(header.finished_at, socket.assigns.current_scope),
      elapsed_duration: LogsViewModel.duration_ms_label(duration_ms),
      total_windows: total_windows,
      completed_windows: completed_windows,
      failed_windows: failed_windows,
      total_asset_attempts: counts.total,
      completed_asset_attempts: counts.completed,
      succeeded_asset_attempts: counts.succeeded,
      skipped_asset_attempts: counts.skipped,
      failed_asset_attempts: counts.failed,
      running_asset_attempts: counts.running,
      queued_asset_attempts: counts.queued,
      planned_asset_attempts: counts.planned,
      assets: [],
      chart: nil,
      comparison: nil,
      asset_attempts_truncated?: false,
      events: events,
      subscribed_run_ids: [header.run_id],
      run_event_sequences: %{header.run_id => header.event_sequence},
      back_asset_href: back_asset_href(header.target_id)
    }
  end

  defp maybe_load_backfill_group(%{backfill_parent?: true} = run, socket) do
    case get_execution_group_detail(operator_context(socket), run.root_run_id) do
      {:ok, %{overview: overview}} ->
        windows = overview.summary_totals.windows

        group =
          %{
            active?: overview.active?,
            raw_status: overview.status,
            status: LogsViewModel.status_label(overview.status),
            status_tone: LogsViewModel.status_tone(overview.status),
            title: "Backfill parent",
            total_windows: windows.total,
            completed_windows: windows.completed,
            failed_windows: windows.failed,
            running_windows: windows.running,
            queued_windows: windows.queued,
            cancelled_windows: windows.cancelled,
            total_asset_attempts: overview.total_asset_attempts,
            completed_asset_attempts: overview.completed_asset_attempts,
            succeeded_asset_attempts: overview.succeeded_asset_attempts,
            skipped_asset_attempts: overview.skipped_asset_attempts,
            failed_asset_attempts: overview.failed_asset_attempts,
            running_asset_attempts: overview.running_asset_attempts,
            queued_asset_attempts: overview.queued_asset_attempts,
            planned_asset_attempts: overview.planned_asset_attempts,
            group_loaded?: true,
            group_error: nil
          }
          |> Map.merge(group_timing(overview, socket.assigns.current_scope))

        Map.merge(run, group)

      {:error, _reason} ->
        preserve_backfill_group(run, socket.assigns.run)
    end
  end

  defp maybe_load_backfill_group(run, _socket), do: run

  defp preserve_backfill_group(run, %{backfill_parent?: true, group_loaded?: true} = previous) do
    run
    |> Map.merge(Map.take(previous, @backfill_group_fields))
    |> Map.put(:group_error, "Backfill progress could not be refreshed; showing the last update.")
  end

  defp preserve_backfill_group(run, _previous) do
    run
    |> Map.put(:active?, true)
    |> Map.put(:title, "Backfill parent")
    |> Map.put(:raw_status, :pending)
    |> Map.put(:status, "Planning")
    |> Map.put(:status_tone, :info)
    |> Map.put(:finished_at, nil)
    |> Map.put(:elapsed_duration, "-")
    |> Map.put(:group_error, "Backfill progress is not available yet; the page will retry.")
  end

  defp group_timing(overview, timezone) do
    case Map.get(overview, :started_at) do
      %DateTime{} = started_at ->
        finished_at = Map.get(overview, :finished_at)

        %{
          started_at: timestamp_label(started_at, timezone),
          finished_at: timestamp_label(finished_at, timezone),
          elapsed_duration: LogsViewModel.duration_label(started_at, finished_at)
        }

      _missing ->
        %{}
    end
  end

  defp submission_from_public(submission, timezone) do
    %{
      id: submission.run_id,
      found?: false,
      submission?: true,
      active?: submission.active?,
      raw_status: submission.status,
      status: submission.status_label,
      status_tone: submission.status_tone,
      target_kind: submission.target_kind,
      target_id: submission.target_id,
      attempt: submission.attempt,
      enqueued_at: timestamp_label(submission.enqueued_at, timezone),
      updated_at: timestamp_label(submission.updated_at, timezone),
      terminal_at: timestamp_label(submission.terminal_at, timezone),
      failure: submission.failure,
      subscribed_run_ids: [submission.run_id],
      run_event_sequences: %{}
    }
  end

  defp loading_run(run_id) do
    %{
      id: run_id,
      found?: false,
      submission?: false,
      initializing?: true,
      active?: false,
      status: "Loading",
      status_tone: :neutral,
      error: nil,
      subscribed_run_ids: [run_id],
      run_event_sequences: %{}
    }
  end

  defp error_run(run_id, reason) do
    %{
      id: run_id,
      found?: false,
      submission?: false,
      initializing?: false,
      active?: false,
      not_found?: reason == :not_found,
      error: if(reason == :not_found, do: "Run not found", else: "Run could not be loaded"),
      subscribed_run_ids: [run_id],
      run_event_sequences: %{}
    }
  end

  # A comparison subscribes to the windows it draws, so their tracks stay as live
  # as the open run's. Leaving compare mode narrows the wanted set back to the
  # open run, and the sync releases everything it no longer wants.
  defp sync_run_subscription(socket) do
    RunEventRefresh.sync_subscriptions(
      socket,
      [socket.assigns.run_id | compare_selection(socket)],
      run_event_sequences(socket.assigns.run),
      run_event_refresh_opts(socket)
    )
  end

  # In a sequential backfill the selected run and every loaded window run can be
  # terminal while the backfill is still producing more. Polling on the run's
  # own activity alone would stop across exactly those moments, so a non-terminal
  # backfill keeps the poll alive.
  #
  # A read this page still owes — a window list that failed, or a compared window
  # that is unavailable, loading, or was left unserved by a read that never
  # answered — keeps it alive too. Without that, a page whose run and backfill
  # are both terminal has no cycle left in which to try again.
  defp poll_worthy?(socket) do
    socket.assigns.run.active? or
      match?(%RunWindowRail{in_progress?: true}, socket.assigns.rail) or
      not is_nil(socket.assigns[:windows_error]) or
      not is_nil(socket.assigns[:window_failures_error]) or
      comparison_owes_a_read?(socket)
  end

  defp comparison_owes_a_read?(%{assigns: %{compare?: true} = assigns}),
    do: assigns |> Map.get(:compare_windows, %{}) |> Map.values() |> Enum.any?(&unserved?/1)

  defp comparison_owes_a_read?(_socket), do: false

  defp schedule_fallback(socket) do
    if connected?(socket) and
         (socket.assigns.active_mode == :flow or socket.assigns.run[:backfill_parent?]) and
         poll_worthy?(socket) and
         is_nil(socket.assigns.refresh_timer_ref) do
      LiveRefresh.schedule_once(
        socket,
        :fallback_poll_ref,
        :poll_run,
        @fallback_refresh_ms
      )
    else
      assign(socket, :fallback_poll_ref, nil)
    end
  end

  defp run_event_refresh_opts(socket) do
    context = operator_context(socket)

    [
      subscribe_fun: &subscribe_run(context, &1),
      unsubscribe_fun: &unsubscribe_run(context, &1),
      replay_on_subscribe?: false,
      refresh_key: :refresh_timer_ref,
      refresh_message: :refresh_run,
      coalesce_ms: @coalesce_refresh_ms
    ]
  end

  defp get_run_flow(context, run_id) do
    Application.get_env(:favn_view, :operator_run_flow_fun, &Orchestrator.get_operator_run_flow/2)
    |> then(fn fun -> if is_function(fun, 2), do: fun.(context, run_id), else: fun.(run_id) end)
  end

  defp get_run_events(context, run_id) do
    Application.get_env(
      :favn_view,
      :operator_run_events_fun,
      &Orchestrator.get_operator_run_events/2
    )
    |> then(fn fun -> if is_function(fun, 2), do: fun.(context, run_id), else: fun.(run_id) end)
  end

  defp list_run_windows(context, run_id) do
    Application.get_env(
      :favn_view,
      :operator_run_windows_fun,
      &Orchestrator.list_operator_run_windows/2
    )
    |> then(fn fun -> if is_function(fun, 2), do: fun.(context, run_id), else: fun.(run_id) end)
  end

  # Bounded to the failed windows alone. A backfill's ledger can hold thousands
  # of windows and this read exists to name reasons, not to list coverage.
  defp page_backfill_windows(context, backfill_id) do
    Application.get_env(
      :favn_view,
      :operator_backfill_windows_fun,
      &Orchestrator.page_operator_backfill_windows/3
    )
    |> then(fn fun ->
      fun.(context, backfill_id, status: :failed, limit: @window_failure_limit)
    end)
  end

  defp get_execution_group_detail(context, root_run_id) do
    Application.get_env(
      :favn_view,
      :operator_execution_group_fun,
      &Orchestrator.get_execution_group_detail/3
    )
    |> then(fn fun -> fun.(context, root_run_id, limit: 1) end)
  end

  defp reset_windows(socket, opts \\ []) do
    keep? = Keyword.get(opts, :keep_loaded, false)

    socket
    |> assign(
      windows: if(keep?, do: socket.assigns[:windows]),
      windows_overflow?: keep? and socket.assigns[:windows_overflow?] == true,
      windows_status: if(keep?, do: socket.assigns[:windows_status]),
      windows_read_at: if(keep?, do: socket.assigns[:windows_read_at]),
      windows_error: nil,
      backfill_id: if(keep?, do: socket.assigns[:backfill_id]),
      open_bucket: nil,
      rail: nil
    )
    |> reset_window_failures(keep?)
    |> build_rail()
  end

  defp reset_window_failures(socket, keep?) do
    assign(socket,
      window_failures: if(keep?, do: socket.assigns[:window_failures]),
      window_failures_overflow?: keep? and socket.assigns[:window_failures_overflow?] == true,
      window_failures_read_at: if(keep?, do: socket.assigns[:window_failures_read_at]),
      window_failures_error: nil
    )
  end

  # The rail is eager for backfill runs only. A scheduled or manual windowed run
  # outside a backfill has no sibling windows to navigate to, so it must not pay
  # for the read.
  defp backfill_run?(%{submit_kind: kind}) when kind in @backfill_submit_kinds, do: true

  defp backfill_run?(%{window: window, id: id, root_run_id: root})
       when not is_nil(window) and is_binary(root),
       do: root != id

  defp backfill_run?(_run), do: false

  defp refresh_windows(socket) do
    cond do
      not backfill_run?(socket.assigns.run) -> socket
      window_read_due?(socket) -> load_windows(socket)
      true -> socket
    end
  end

  # Event-driven refreshes can run far faster than the fallback interval. The
  # window list changes only when the backfill starts another window, so it is
  # read at most once per fallback interval however often the run refreshes.
  defp window_read_due?(%{assigns: %{windows_read_at: nil}}), do: true

  defp window_read_due?(%{assigns: %{windows_read_at: read_at}}),
    do: monotonic_ms() - read_at >= @fallback_refresh_ms

  defp load_windows(socket) do
    socket = assign(socket, :windows_read_at, monotonic_ms())

    case list_run_windows(operator_context(socket), socket.assigns.run_id) do
      {:ok, %{items: items, overflow?: overflow?} = result} ->
        socket
        |> assign(
          windows: items,
          windows_overflow?: overflow?,
          windows_status: Map.get(result, :backfill_status),
          windows_error: nil,
          backfill_id: Map.get(result, :backfill_id)
        )
        |> build_rail()
        |> refresh_window_failures()

      # A failed window read hides the rail and leaves the run page fully
      # functional; it is navigation, not run state. Compare mode is a mode of
      # the rail, so it closes with it: the toggle that leaves it lives there,
      # and an operator held inside a comparison with no way out is not a fully
      # functional page.
      {:error, _reason} ->
        socket
        |> assign(
          windows: nil,
          windows_error: "Window runs could not be loaded. The page will try again.",
          compare?: false,
          compare_run_ids: [],
          compare_windows: %{},
          compare_limit_reached?: false,
          rail: nil
        )
        |> assign_comparison()
        |> build_rail()
    end
  end

  defp build_rail(%{assigns: %{windows: windows}} = socket) when is_list(windows) do
    rail =
      RunWindowRail.build(windows, socket.assigns.run_id, socket.assigns.current_scope,
        truncated?: socket.assigns[:windows_overflow?] == true,
        backfill_status: socket.assigns[:windows_status],
        open_bucket: socket.assigns[:open_bucket],
        compare_run_ids: socket.assigns[:compare_run_ids] || []
      )

    socket
    |> assign(:rail, rail)
    |> assign_combined_window(rail)
  end

  defp build_rail(socket), do: assign(socket, :rail, nil)

  # A combined backfill has no rail to state its coverage, so the run header
  # carries the span instead. It is named once, here, rather than derived in the
  # markup from a rail the markup is not rendering.
  defp assign_combined_window(socket, %RunWindowRail{combined: %{} = combined}) do
    label =
      window_label(combined.start_at, combined.end_at, socket.assigns.current_scope) ||
        window_title(combined.start_at, combined.end_at, socket.assigns.current_scope)

    assign(
      socket,
      :run,
      Map.put(socket.assigns.run, :combined_window, %{
        label: label,
        window_count: combined.window_count
      })
    )
  end

  defp assign_combined_window(socket, _rail), do: socket

  # A window that fails before its child run exists leaves no run to open and no
  # run page to carry its diagnosis, so the reason it recorded is the only
  # account of the failure the product has. It lives on the backfill ledger,
  # which is a different read from the window list: the list returns navigable
  # window runs, and these windows never became one.
  #
  # The read piggybacks the window read rather than taking a timer of its own.
  # Both describe the same ledger, so a second cadence would only add a second
  # statement per interval and could show a count and a reason list that
  # disagree.
  defp refresh_window_failures(socket) do
    if window_failures_due?(socket) do
      load_window_failures(socket)
    else
      socket
    end
  end

  defp window_failures_due?(%{assigns: assigns}) do
    is_binary(assigns[:backfill_id]) and
      assigns.run[:backfill_parent?] == true and
      (assigns.run[:failed_windows] || 0) > 0
  end

  # A failed ledger read leaves the rest of the page alone, exactly as a failed
  # window read does. It states what could not be read rather than letting the
  # page imply that a backfill with failed windows recorded nothing.
  defp load_window_failures(socket) do
    socket = assign(socket, :window_failures_read_at, monotonic_ms())

    case page_backfill_windows(operator_context(socket), socket.assigns.backfill_id) do
      {:ok, %{items: items} = page} ->
        assign(socket,
          window_failures:
            WindowFailures.group(
              Enum.map(items, &Map.from_struct/1),
              socket.assigns.current_scope
            ),
          window_failures_overflow?: Map.get(page, :has_more?) == true,
          window_failures_error: nil
        )

      {:error, _reason} ->
        assign(socket,
          window_failures: nil,
          window_failures_error:
            "Why these windows failed could not be loaded. The page will try again."
        )
    end
  end

  # A backfill parent runs no asset work of its own — its page says so and offers
  # its windows — so landing on it and finding nothing drawn is a step the
  # operator always has to take. It opens its earliest window instead.
  #
  # Called from `handle_params`, never from the rail build: a live patch issued
  # while mounting raises, and the rail is built inside the mount's first read.
  # Only on arrival, too — a later refresh must not drag the page off a window
  # that was chosen, and neither must coming back to the parent deliberately.
  defp open_first_window(%{assigns: %{run: %{backfill_parent?: true}, rail: rail}} = socket)
       when not is_nil(rail) do
    case {socket.assigns[:window_opened?], rail.cells} do
      {true, _cells} ->
        socket

      {_first_arrival, []} ->
        socket

      {_first_arrival, [%{run_id: run_id} | _rest]} ->
        socket |> assign(:window_opened?, true) |> patch_to_window(run_id)
    end
  end

  defp open_first_window(socket), do: socket

  # Track order is the windows' own calendar order rather than the order the
  # operator clicked them, so a track position means the same thing in every
  # lane and adding a window never renumbers the ones already drawn.
  defp select_compare(socket, run_ids) do
    run_id = socket.assigns.run_id
    wanted = MapSet.new([run_id | run_ids])

    ordered =
      (socket.assigns.windows || [])
      |> Enum.sort_by(&{DateTime.to_unix(&1.window_start_at, :microsecond), &1.run_id})
      |> Enum.map(& &1.run_id)
      |> Enum.uniq()
      |> Enum.filter(&MapSet.member?(wanted, &1))

    # The window read caps at 1,000 rows, so the open run is not guaranteed to
    # be among the loaded choices. It anchors the comparison regardless.
    ordered = if run_id in ordered, do: ordered, else: [run_id | ordered]

    socket
    |> assign(
      compare_run_ids: Enum.take(ordered, @compare_limit),
      compare_limit_reached?: false
    )
    |> build_rail()
    |> update_comparison()
  end

  defp known_window?(socket, run_id),
    do: Enum.any?(socket.assigns.windows || [], &(&1.run_id == run_id))

  defp patch_to_window(socket, run_id) do
    cond do
      run_id == socket.assigns.run_id -> socket
      is_nil(run_id) -> socket
      known_window?(socket, run_id) -> push_patch(socket, to: window_path(socket, run_id))
      true -> put_flash(socket, :error, "That window run is not available")
    end
  end

  defp window_path(socket, run_id),
    do: ~p"/runs/#{run_id}?view=#{socket.assigns.active_mode}"

  # The ends of the rail are ends, not a loop. `Enum.at/2` counts a negative
  # index from the back, so stepping left off the first cell would otherwise
  # jump to the last one.
  defp stepped_run_id(%{assigns: %{rail: %RunWindowRail{cells: cells}, run_id: run_id}}, key) do
    step = if key == "ArrowLeft", do: -1, else: 1

    with index when is_integer(index) <- Enum.find_index(cells, &(&1.run_id == run_id)),
         target when target >= 0 <- index + step,
         %{run_id: stepped} <- Enum.at(cells, target) do
      stepped
    else
      _end_of_rail -> nil
    end
  end

  defp stepped_run_id(_socket, _key), do: nil

  defp monotonic_ms, do: System.monotonic_time(:millisecond)

  defp subscribe_run(context, run_id) do
    Application.get_env(:favn_view, :run_subscribe_fun, &Orchestrator.subscribe_run/2).(
      context,
      run_id
    )
  end

  # Selecting a window moves the run subscription exactly once. The seam exists
  # so a test can prove the move happened rather than inferring it.
  defp unsubscribe_run(context, run_id) do
    Application.get_env(:favn_view, :run_unsubscribe_fun, &Orchestrator.unsubscribe_run/2).(
      context,
      run_id
    )
  end

  defp event_row(event, timezone) do
    %{
      sequence: event.sequence,
      timestamp: timestamp_label(event.occurred_at, timezone),
      event_type: label(event.event_type),
      asset: event.asset_ref,
      summary: event.summary || event.asset_ref || LogsViewModel.status_label(event.status)
    }
  end

  # A window that covers one whole calendar period is named by that period. The
  # full bounds stay one hover away, because the compact form is a name and the
  # range is the definition.
  defp window_label(start_at, end_at, timezone),
    do: WindowLabel.compact(start_at, end_at, timezone)

  defp window_title(start_at, end_at, timezone),
    do: WindowLabel.full(start_at, end_at, timezone)

  defp back_asset_href(target_id) when is_binary(target_id),
    do: "/assets/#{AssetRoute.to_param(target_id)}"

  defp back_asset_href(_target_id), do: nil

  defp run_event_sequences(%{run_event_sequences: sequences}) when is_map(sequences),
    do: sequences

  defp run_event_sequences(_run), do: %{}

  defp active_mode_from_params(%{"view" => mode}) when mode in @valid_modes,
    do: String.to_existing_atom(mode)

  defp active_mode_from_params(_params), do: :flow

  defp timestamp_label(nil, _timezone), do: nil

  defp timestamp_label(%DateTime{} = value, timezone),
    do: FavnView.Time.format(value, "%b %-d, %Y %H:%M:%S %Z", timezone)

  defp duration_ms(%DateTime{} = started_at, %DateTime{} = finished_at),
    do: max(DateTime.diff(finished_at, started_at, :millisecond), 0)

  defp duration_ms(%DateTime{} = started_at, nil),
    do: max(DateTime.diff(DateTime.utc_now(), started_at, :millisecond), 0)

  defp duration_ms(_started_at, _finished_at), do: nil

  defp label(nil), do: "Unknown"
  defp label(:step_started), do: "Step submitted"
  defp label("step_started"), do: "Step submitted"
  defp label(:step_running), do: "Runner started"
  defp label("step_running"), do: "Runner started"
  defp label(value), do: value |> to_string() |> String.replace("_", " ") |> String.capitalize()

  defp retry_label(1), do: "Retry 1 remaining asset"
  defp retry_label(count), do: "Retry #{count} remaining assets"

  defp retry_success_label(run_ids, asset_count) do
    run_label = if length(run_ids) == 1, do: "1 retry run", else: "#{length(run_ids)} retry runs"
    asset_label = if asset_count == 1, do: "1 asset", else: "#{asset_count} assets"
    "Submitted #{run_label} for #{asset_label}"
  end

  defp retry_error_label(:no_remaining_work), do: "No remaining assets to retry"
  defp retry_error_label({:run_not_retryable, _status}), do: "Run is not retryable"
  defp retry_error_label(_reason), do: "Remaining assets could not be retried"

  defp actor_context(socket) do
    %Scope{} = scope = socket.assigns.current_scope
    scope.operator_context
  end

  defp operator_context(socket), do: actor_context(socket)
end
