defmodule FavnView.RunDetailLive do
  @moduledoc false

  use FavnView, :live_view

  alias FavnView.AssetRoute
  alias FavnView.Auth.Scope
  alias FavnView.Components.AssetCataloguePage
  alias FavnView.Components.RunDetailPage
  alias FavnView.CommandAttempt
  alias FavnView.LiveRefresh
  alias FavnView.LogsViewModel
  alias FavnView.OperatorErrorLabels
  alias FavnView.RunEventRefresh
  alias FavnView.RunFlow

  @refresh_interval_ms 1_500
  @initial_load_retry_count 10
  @coalesce_refresh_ms 100
  @active_statuses [:pending, :running]
  @valid_modes ~w(flow windows events)

  # A live run's clock has to move on its own. Run events are the only thing that
  # used to advance the axis, so a running bar sat still for however long the
  # runner was quiet and then jumped — which reads as the UI lagging rather than
  # as the asset taking a while. This re-projects the flow on a fixed tick with no
  # facade call, and the bars interpolate between ticks in CSS, so the timeline
  # advances smoothly whether or not anything is happening.
  @flow_tick_ms 1_000

  @impl true
  def mount(%{"run_id" => run_id}, _session, socket) do
    run =
      operator_context(socket)
      |> load_run(run_id, nil, :flow)
      |> mark_initializing()

    socket =
      assign(socket,
        run_id: run_id,
        run: run,
        active_mode: :flow,
        selected_child_run_id: nil,
        selected_attempt_id: nil,
        cancel_attempt: nil,
        retry_attempt: nil,
        detail_load_attempts_remaining: initial_load_attempts(run),
        nav_items: AssetCataloguePage.nav_items(:runs)
      )
      |> RunEventRefresh.init([:refresh_timer_ref, :fallback_poll_ref, :flow_tick_ref])
      |> assign_flow()
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

  def handle_info({:tick_flow, token}, socket) do
    case LiveRefresh.take(socket, :flow_tick_ref, token) do
      {:ok, socket} -> {:noreply, assign_flow(socket)}
      {:stale, socket} -> {:noreply, socket}
    end
  end

  def handle_info({:favn_run_event, event}, socket) do
    {:noreply, RunEventRefresh.handle_event(socket, event, run_event_refresh_opts(socket))}
  end

  def handle_info(:favn_persistence_published, socket) do
    {:noreply, RunEventRefresh.schedule_refresh(socket, run_event_refresh_opts(socket))}
  end

  defp refresh_run(socket) do
    run =
      load_run(
        operator_context(socket),
        socket.assigns.run_id,
        socket.assigns.run[:back_asset_href],
        socket.assigns.active_mode
      )

    attempts_remaining = next_load_attempts(socket.assigns.detail_load_attempts_remaining, run)
    run = Map.put(run, :initializing?, !run.found? and attempts_remaining > 0)

    socket
    |> assign(:run, run)
    |> assign(:detail_load_attempts_remaining, attempts_remaining)
    |> assign_flow()
    |> RunEventRefresh.mark_refreshed(run_event_sequences(run))
    |> sync_run_event_refresh()
    |> maybe_schedule_fallback_poll()
  end

  # The flow is geometry over the attempts and the clock. Recomputing it here
  # rather than in `render/1` keeps it out of the diff when nothing moved, and
  # lets the tick advance the axis without refetching the run. The header's
  # elapsed duration shares the tick so both clocks read the same now.
  defp assign_flow(%{assigns: %{run: %{found?: true} = run}} = socket) do
    run = Map.put(run, :elapsed_duration, duration_or_elapsed(run_timing(run)))

    socket
    |> assign(:run, run)
    |> assign(:flow, RunFlow.build(run.attempts, active?: run.active?))
    |> maybe_schedule_flow_tick()
  end

  defp assign_flow(socket) do
    socket
    |> assign(:flow, nil)
    |> maybe_schedule_flow_tick()
  end

  defp maybe_schedule_flow_tick(%{assigns: %{run: %{active?: true}}} = socket) do
    if connected?(socket) do
      LiveRefresh.schedule_once(socket, :flow_tick_ref, :tick_flow, @flow_tick_ms)
    else
      socket
    end
  end

  defp maybe_schedule_flow_tick(socket), do: socket

  @impl true
  def handle_event("set_mode", %{"mode" => mode}, socket) when mode in @valid_modes do
    {:noreply, patch_run_state(socket, active_mode: String.to_existing_atom(mode))}
  end

  def handle_event("cancel_run", params, socket) do
    case socket.assigns.run do
      %{cancellable?: true, cancel_run_id: run_id} when is_binary(run_id) ->
        attempt =
          CommandAttempt.next(socket.assigns.cancel_attempt, "run_cancel", run_id, params)

        socket = assign(socket, :cancel_attempt, attempt)

        case FavnOrchestrator.cancel_operator_run(
               actor_context(socket),
               run_id,
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
             |> put_flash(:error, cancel_error_label(reason))}
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

        case FavnOrchestrator.retry_operator_run_remaining(
               actor_context(socket),
               socket.assigns.run_id,
               idempotency_key: attempt.key
             ) do
          {:ok, %{run_ids: run_ids, asset_count: asset_count}} ->
            {:noreply,
             socket
             |> CommandAttempt.acknowledge(attempt)
             |> assign(:retry_attempt, nil)
             |> put_flash(:info, retry_remaining_submitted_label(run_ids, asset_count))
             |> refresh_run()}

          {:partial, %{run_ids: run_ids, reason: reason}} ->
            {:noreply,
             socket
             |> CommandAttempt.acknowledge(attempt)
             |> assign(:retry_attempt, nil)
             |> put_flash(:error, retry_remaining_partial_label(run_ids, reason))
             |> refresh_run()}

          {:error, reason} ->
            {socket, attempt} = CommandAttempt.settle_failure(socket, attempt, reason)

            {:noreply,
             socket
             |> assign(:retry_attempt, attempt)
             |> put_flash(:error, retry_remaining_error_label(reason))}
        end

      _run ->
        {:noreply, socket}
    end
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
      current_scope={@current_scope}
      operator_workspaces={@operator_workspaces}
      active_mode={@active_mode}
      flow={@flow}
      selected_child_run_id={@selected_child_run_id}
      selected_attempt_id={@selected_attempt_id}
      flash={@flash}
    />
    """
  end

  @impl true
  def handle_params(params, _uri, socket) do
    active_mode = active_mode_from_params(params, socket.assigns.active_mode)

    run =
      if active_mode == socket.assigns.active_mode do
        socket.assigns.run
      else
        load_run(
          operator_context(socket),
          socket.assigns.run_id,
          socket.assigns.run[:back_asset_href],
          active_mode
        )
      end

    selected_child_run_id =
      selected_child_run_id_from_params(params, run, socket.assigns.run_id)

    active_mode =
      if selected_child_run_id && active_mode == :flow, do: :windows, else: active_mode

    {:noreply,
     socket
     |> assign(
       run: run,
       active_mode: active_mode,
       selected_child_run_id: selected_child_run_id
     )
     |> assign_flow()}
  end

  @impl true
  def terminate(_reason, socket) do
    operator_context = operator_context(socket)
    RunEventRefresh.unsubscribe_all(socket, &unsubscribe_run(operator_context, &1))
    _ = FavnOrchestrator.unsubscribe_run_wakeups(operator_context)

    :ok
  end

  defp load_run(operator_context, run_id, existing_back_asset_href, active_mode) do
    opts = [view: detail_view(active_mode), limit: 200]
    opts = if active_mode == :events, do: Keyword.put(opts, :include, [:events]), else: opts

    case get_operator_run_activity(operator_context, run_id, opts) do
      {:ok, %{kind: :run, detail: detail}} ->
        detail_from_execution_group(
          detail,
          operator_context,
          run_id,
          existing_back_asset_href,
          active_mode
        )

      {:ok, %{kind: :submission, submission: submission}} ->
        submission_from_public(submission)

      {:error, reason} ->
        %{
          id: run_id,
          found?: false,
          not_found?: reason == :not_found,
          error: error_label(reason)
        }
    end
  end

  defp submission_from_public(submission) do
    %{
      id: submission.run_id,
      found?: false,
      submission?: true,
      initializing?: false,
      active?: submission.active?,
      raw_status: submission.status,
      status: submission.status_label,
      status_tone: submission.status_tone,
      target_kind: submission.target_kind,
      target_id: submission.target_id,
      attempt: submission.attempt,
      enqueued_at: timestamp_label(submission.enqueued_at),
      updated_at: timestamp_label(submission.updated_at),
      terminal_at: timestamp_label(submission.terminal_at),
      failure: submission.failure,
      subscribed_run_ids: []
    }
  end

  # The facade view values predate the flow, and `:overview`, `:timeline`, and
  # `:failures` all resolve to the same read there. The flow needs that read.
  defp detail_view(:flow), do: :overview
  defp detail_view(active_mode), do: active_mode

  defp detail_from_execution_group(
         %{summary: summary, root_run: root_run} = detail,
         operator_context,
         run_id,
         existing_back_asset_href,
         active_mode
       ) do
    attempts = Enum.map(Map.get(detail, :asset_attempts, []), &attempt_from_public/1)
    windows = Enum.map(Map.get(detail, :windows, []), &window_from_public/1)

    requested_windows =
      Enum.map(Map.get(detail, :requested_windows, []), &window_from_public/1)

    events = if active_mode == :events, do: Map.get(detail, :events, []), else: []
    child_runs = child_runs_from_public(Map.get(detail, :child_runs, []), attempts, windows)
    cancel_target = cancel_target(summary, root_run, child_runs, run_id)

    active? = active_group?(summary)
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
      active?: active?,
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
      duration_ms_raw: summary.duration_ms,
      started_at_raw: summary.started_at,
      elapsed_duration: duration_or_elapsed(summary),
      manifest_version_id: root_run.manifest_version_id || "Unknown",
      total_windows: summary.total_windows,
      completed_windows: summary.completed_windows,
      failed_windows: summary.failed_windows,
      requested_window_counts:
        Map.get(summary, :requested_window_counts, %{
          total: summary.total_windows,
          completed: summary.completed_windows,
          failed: summary.failed_windows
        }),
      effective_window_count: Map.get(summary, :effective_window_count, length(windows)),
      total_asset_attempts: summary.total_asset_attempts,
      completed_asset_attempts: summary.completed_asset_attempts,
      succeeded_asset_attempts:
        max(summary.completed_asset_attempts - summary.failed_asset_attempts, 0),
      failed_asset_attempts: summary.failed_asset_attempts,
      running_asset_attempts: summary.running_asset_attempts,
      queued_asset_attempts: summary.queued_asset_attempts,
      progress_label:
        get_in(summary, [:progress, :label]) ||
          progress_label(summary.completed_asset_attempts, summary.total_asset_attempts),
      windows: windows,
      requested_windows: requested_windows,
      requested_windows_truncated?: Map.get(detail, :requested_windows_truncated?, false),
      attempts: attempts,
      asset_attempts_truncated?: Map.get(detail, :asset_attempts_truncated?, false),
      failures: failures,
      backfill_failures: backfill_failures,
      backfill_failure_count: Map.get(detail, :backfill_failure_count, length(backfill_failures)),
      child_runs: child_runs,
      child_runs_truncated?: Map.get(detail, :child_run_details_truncated?, false),
      events: Enum.map(events, &event_from_public/1),
      latest_event_summary: latest_event_summary(detail, events),
      waiting_activity?: events == [] and active_group?(summary),
      current_activity: current_activity(attempts),
      selected_attempt: nil,
      context: context_items(summary, root_run, target, windows),
      back_asset_href:
        existing_back_asset_href ||
          back_asset_href(operator_context, List.first(summary.target_assets)),
      raw_run: nil,
      raw_events: nil,
      root_event_sequence: Map.get(detail, :root_event_sequence),
      run_event_sequences: run_event_sequences_from_public(root_run, child_runs, events, detail)
    }
  end

  defp maybe_schedule_fallback_poll(
         %{assigns: %{run: %{submission?: true, active?: true}}} = socket
       ) do
    if connected?(socket) do
      LiveRefresh.schedule_once(socket, :fallback_poll_ref, :poll_run, @refresh_interval_ms)
    else
      socket
    end
  end

  defp maybe_schedule_fallback_poll(
         %{
           assigns: %{
             run: %{found?: false},
             detail_load_attempts_remaining: attempts_remaining
           }
         } = socket
       )
       when attempts_remaining > 0 do
    if connected?(socket) do
      LiveRefresh.schedule_once(socket, :fallback_poll_ref, :poll_run, @refresh_interval_ms)
    else
      socket
    end
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

  defp mark_initializing(%{submission?: true} = run), do: Map.put(run, :initializing?, false)
  defp mark_initializing(%{found?: false} = run), do: Map.put(run, :initializing?, true)
  defp mark_initializing(run), do: Map.put(run, :initializing?, false)

  defp initial_load_attempts(%{submission?: true}), do: 0
  defp initial_load_attempts(%{found?: false}), do: @initial_load_retry_count
  defp initial_load_attempts(_run), do: 0

  defp next_load_attempts(_remaining, %{submission?: true}), do: 0
  defp next_load_attempts(_remaining, %{found?: true}), do: 0
  defp next_load_attempts(remaining, _run) when remaining > 0, do: remaining - 1
  defp next_load_attempts(_remaining, _run), do: 0

  defp sync_run_event_refresh(%{assigns: %{run: run}} = socket) do
    RunEventRefresh.sync_subscriptions(
      socket,
      Map.get(run, :subscribed_run_ids, []),
      run_event_sequences(run),
      run_event_refresh_opts(socket)
    )
  end

  defp sync_run_event_refresh(socket), do: socket

  defp needs_discovery_poll?(%{total_windows: total, child_runs: child_runs})
       when is_integer(total) and is_list(child_runs),
       do: total > length(child_runs)

  defp needs_discovery_poll?(_run), do: false

  defp actor_context(socket) do
    %Scope{} = scope = socket.assigns.current_scope
    scope.operator_context
  end

  defp operator_context(socket), do: actor_context(socket)

  defp get_operator_run_activity(operator_context, run_id, opts) do
    fun =
      Application.get_env(
        :favn_view,
        :operator_run_activity_fun,
        &FavnOrchestrator.get_operator_run_activity/3
      )

    if is_function(fun, 3), do: fun.(operator_context, run_id, opts), else: fun.(run_id, opts)
  end

  defp timestamp_label(nil), do: nil

  defp timestamp_label(%DateTime{} = value),
    do: Calendar.strftime(value, "%b %-d, %Y %H:%M:%S UTC")

  defp subscribe_run(operator_context, run_id) do
    Application.get_env(
      :favn_view,
      :run_subscribe_fun,
      &FavnOrchestrator.subscribe_run/2
    ).(operator_context, run_id)
  end

  defp unsubscribe_run(operator_context, run_id),
    do: FavnOrchestrator.unsubscribe_run(operator_context, run_id)

  defp list_run_stream_events(operator_context, run_id, opts) do
    fun =
      Application.get_env(
        :favn_view,
        :run_stream_events_fun,
        &FavnOrchestrator.list_run_stream_events/3
      )

    if is_function(fun, 3), do: fun.(operator_context, run_id, opts), else: fun.(run_id, opts)
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

  defp run_event_refresh_opts(socket) do
    operator_context = operator_context(socket)

    [
      subscribe_fun: &subscribe_run(operator_context, &1),
      unsubscribe_fun: &unsubscribe_run(operator_context, &1),
      list_events_fun: &list_run_stream_events(operator_context, &1, &2),
      refresh_key: :refresh_timer_ref,
      refresh_message: :refresh_run,
      coalesce_ms: @coalesce_refresh_ms
    ]
  end

  defp patch_run_state(socket, updates) do
    active_mode = Keyword.get(updates, :active_mode, socket.assigns.active_mode)

    push_patch(socket, to: ~p"/runs/#{socket.assigns.run_id}?#{run_query_params(active_mode)}")
  end

  defp run_query_params(:flow), do: %{}
  defp run_query_params(active_mode), do: %{"view" => Atom.to_string(active_mode)}

  defp active_mode_from_params(%{"view" => mode}, _current) when mode in @valid_modes,
    do: String.to_existing_atom(mode)

  defp active_mode_from_params(_params, _current), do: :flow

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
      asset_step_id: Map.get(attempt, :asset_step_id, attempt.id),
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
      logs_href:
        ~p"/runs/#{attempt.run_id}/assets/#{Map.get(attempt, :asset_step_id, attempt.id)}/logs"
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

  # Both lookups were a scan per child run, so a thirty-window backfill walked
  # every attempt thirty times on every live refresh. One grouping pass each.
  defp child_runs_from_public(child_runs, attempts, windows) do
    attempts_by_run_id = Enum.group_by(attempts, & &1.run_id)
    windows_by_child_run_id = Map.new(windows, &{&1.child_run_id, &1})

    Enum.map(child_runs, fn child ->
      child_attempts = Map.get(attempts_by_run_id, child.id, [])

      window =
        Map.get(windows_by_child_run_id, child.id) || window_from_public(child.window)

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

  defp back_asset_href(_operator_context, nil), do: nil

  defp back_asset_href(operator_context, ref) do
    ref_string = LogsViewModel.ref_label(ref)

    with {:ok, entries} <- FavnOrchestrator.active_asset_catalogue(operator_context),
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

  defp run_timing(run),
    do: %{duration_ms: Map.get(run, :duration_ms_raw), started_at: Map.get(run, :started_at_raw)}

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
  defp label(:step_running), do: "Runner started"
  defp label("step_running"), do: "Runner started"
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
