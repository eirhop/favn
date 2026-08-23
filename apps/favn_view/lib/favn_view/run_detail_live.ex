defmodule FavnView.RunDetailLive do
  @moduledoc false

  use FavnView, :live_view

  require Logger

  alias FavnView.Orchestrator
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
  alias FavnView.RunDetailTelemetry

  @fallback_initial_ms 5_000
  @fallback_max_ms 30_000
  @initial_load_retry_count 10
  @coalesce_refresh_ms 100
  @active_statuses [:pending, :running]
  @valid_modes ~w(flow windows events)
  @dialyzer {:no_unused, [retry_remaining_submitted_label: 2, retry_remaining_partial_label: 2]}
  @dialyzer {:no_match, [handle_event: 3, back_asset_href: 2, retry_remaining_error_label: 1]}

  # A live run's clock has to move on its own. Run events are the only thing that
  # used to advance the axis, so a running bar sat still for however long the
  # runner was quiet and then jumped — which reads as the UI lagging rather than
  # as the asset taking a while. This re-projects the flow on a fixed tick with no
  # facade call, and the bars interpolate between ticks in CSS, so the timeline
  # advances smoothly whether or not anything is happening.
  @flow_tick_ms 1_000
  @flow_page_size 200
  @flow_retained_limit 500

  @impl true
  def mount(%{"run_id" => run_id} = params, _session, socket) do
    connected_mount? = connected?(socket)

    back_asset_href =
      case params["back_asset_id"] do
        asset_id when is_binary(asset_id) and byte_size(asset_id) in 1..1_024 ->
          ~p"/assets/#{asset_id}"

        _other ->
          nil
      end

    socket =
      assign(socket,
        run_id: run_id,
        run: loading_run(run_id, back_asset_href),
        active_mode: :flow,
        selected_child_run_id: nil,
        selected_attempt_id: nil,
        flow_asset_prefix: nil,
        flow_task_generation: 0,
        flow_task_name: nil,
        flow_pending_watermark: nil,
        flow_reconcile_pending?: false,
        flow_reconcile_required?: false,
        flow_repair_generation: 0,
        summary_refresh_pending?: false,
        summary_repair_required?: false,
        summary_pending_watermark: nil,
        summary_acknowledged_watermark: 0,
        summary_task_watermark: nil,
        summary_task_repair_generation: nil,
        fallback_poll_delay_ms: @fallback_initial_ms,
        connected_mount?: connected_mount?,
        flow_filter_form: to_form(%{"asset_prefix" => ""}, as: :flow_filter),
        cancel_attempt: nil,
        retry_attempt: nil,
        detail_load_attempts_remaining: @initial_load_retry_count,
        nav_items: AssetCataloguePage.nav_items(:runs)
      )
      |> RunEventRefresh.init([:refresh_timer_ref, :fallback_poll_ref, :flow_tick_ref])

    socket =
      if connected?(socket) do
        _ = Orchestrator.subscribe_projection_listener()
        Orchestrator.with_read_deadline(3_000, fn -> connect_initial_flow(socket) end)
      else
        socket
      end

    socket =
      socket
      |> assign_flow()
      |> sync_run_event_refresh()
      |> maybe_schedule_fallback_poll()

    {:ok, socket}
  end

  defp connect_initial_flow(socket) do
    run_id = socket.assigns.run_id
    context = operator_context(socket)

    subscription =
      if Application.get_env(:favn_view, :operator_run_flow_fun) do
        :test_adapter
      else
        subscribe_run(context, run_id)
      end

    case subscription do
      result when result in [:ok, :test_adapter] ->
        run =
          context
          |> load_run(run_id, nil, :flow, socket.assigns.current_scope)
          |> mark_initializing()

        if result == :ok and not run[:found?] do
          _ = unsubscribe_run(context, run_id)
        end

        socket
        |> assign(:run, run)
        |> assign(:detail_load_attempts_remaining, initial_load_attempts(run))
        |> maybe_mark_initial_subscription(result, run)

      {:error, :not_found} ->
        # A reserved run identity has no run topic until admission. The
        # independently authorized Flow read may still return its durable
        # submission state, which is refreshed by the bounded fallback poll.
        run =
          context
          |> load_run(run_id, nil, :flow, socket.assigns.current_scope)
          |> mark_initializing()

        socket
        |> assign(:run, run)
        |> assign(:detail_load_attempts_remaining, initial_load_attempts(run))

      {:error, reason} ->
        run = %{
          id: run_id,
          found?: false,
          initializing?: false,
          active?: false,
          error: error_label(reason),
          subscribed_run_ids: []
        }

        socket
        |> assign(:run, run)
        |> assign(:detail_load_attempts_remaining, 0)
    end
  end

  defp maybe_mark_initial_subscription(socket, :ok, %{found?: true, id: run_id}) do
    socket
    |> assign(:run_event_subscriptions, MapSet.new([run_id]))
    |> assign(:run_events_live?, true)
  end

  defp maybe_mark_initial_subscription(socket, _result, _run), do: socket

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
        {:noreply,
         socket
         |> bump_fallback_backoff()
         |> refresh_run()
         |> maybe_schedule_fallback_poll()}

      {:stale, socket} ->
        {:noreply, socket}
    end
  end

  def handle_info(:refresh_run, socket) do
    {:noreply, refresh_run(socket)}
  end

  def handle_info(:poll_run, socket) do
    {:noreply,
     socket
     |> bump_fallback_backoff()
     |> refresh_run()
     |> maybe_schedule_fallback_poll()}
  end

  def handle_info(:favn_projection_listener_resumed, socket) do
    {:noreply, reconcile_active_screen(socket)}
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

  def handle_info(
        {:favn_run_projected, %{"change" => "membership", "repair_generation" => generation}},
        socket
      )
      when is_integer(generation) do
    if generation > socket.assigns.flow_repair_generation do
      socket =
        socket
        |> assign(:flow_repair_generation, generation)
        |> mark_projection_repair_required()

      {:noreply, reconcile_active_screen(socket)}
    else
      {:noreply, socket}
    end
  end

  def handle_info(
        {:favn_run_projected, %{"publication_id" => publication_id} = payload},
        socket
      ) do
    if socket.assigns.active_mode == :flow do
      handle_flow_projection(socket, publication_id, payload)
    else
      handle_summary_projection(socket, publication_id, payload)
    end
  end

  defp handle_flow_projection(socket, publication_id, payload) do
    acknowledged = socket.assigns.run[:flow_projection_cursor] || 0

    if is_integer(publication_id) and publication_id > acknowledged do
      socket =
        assign(
          socket,
          :flow_pending_watermark,
          max(publication_id, socket.assigns.flow_pending_watermark || 0)
        )

      if payload["change"] == "membership" do
        {:noreply,
         socket
         |> assign(:flow_reconcile_required?, true)
         |> reconcile_active_screen()}
      else
        if socket.assigns.flow_task_name do
          {:noreply, socket}
        else
          {:noreply, RunEventRefresh.schedule_refresh(socket, run_event_refresh_opts(socket))}
        end
      end
    else
      {:noreply, socket}
    end
  end

  defp handle_summary_projection(socket, publication_id, payload) do
    seen =
      [
        socket.assigns.summary_acknowledged_watermark,
        socket.assigns.run[:summary_projection_cursor] || 0,
        socket.assigns.summary_pending_watermark || 0,
        socket.assigns.summary_task_watermark || 0
      ]
      |> Enum.max()

    if is_integer(publication_id) and publication_id > seen do
      socket =
        socket
        |> assign(:summary_pending_watermark, publication_id)
        |> maybe_mark_summary_membership(payload)

      {:noreply, refresh_summary_mode(socket, socket.assigns.active_mode)}
    else
      {:noreply, socket}
    end
  end

  defp mark_projection_repair_required(%{assigns: %{active_mode: :flow}} = socket),
    do: assign(socket, :flow_reconcile_required?, true)

  defp mark_projection_repair_required(socket),
    do: assign(socket, :summary_repair_required?, true)

  defp maybe_mark_summary_membership(socket, %{"change" => "membership"}),
    do: assign(socket, :summary_repair_required?, true)

  defp maybe_mark_summary_membership(socket, _payload), do: socket

  defp reconcile_active_screen(%{assigns: %{active_mode: :flow, run: %{found?: true}}} = socket) do
    socket = assign(socket, :flow_reconcile_required?, true)

    if socket.assigns.flow_task_name do
      assign(socket, :flow_reconcile_pending?, true)
    else
      start_flow_reconciliation(socket)
    end
  end

  defp reconcile_active_screen(%{assigns: %{active_mode: mode, run: %{found?: true}}} = socket)
       when mode in [:windows, :events],
       do: refresh_summary_mode(socket, mode)

  defp reconcile_active_screen(socket), do: refresh_broad_mode(socket)

  defp start_flow_reconciliation(socket) do
    target = max(length(socket.assigns.run.attempts), 1)
    generation = socket.assigns.flow_task_generation + 1
    name = {:flow_reconcile, generation}
    context = operator_context(socket)
    run_id = socket.assigns.run_id
    prefix = socket.assigns.flow_asset_prefix
    lower_anchor = socket.assigns.run.flow_lower_anchor

    socket
    |> assign(:flow_task_generation, generation)
    |> assign(:flow_task_name, name)
    |> assign(:flow_reconcile_pending?, false)
    |> start_async(name, fn ->
      Orchestrator.with_read_deadline(6_000, fn ->
        reconcile_flow_pages(
          context,
          run_id,
          prefix,
          target,
          lower_anchor,
          [],
          []
        )
      end)
    end)
  end

  @impl true
  def handle_async({:flow_reconcile, generation}, {:ok, result}, socket) do
    if socket.assigns.flow_task_name == {:flow_reconcile, generation} do
      socket = assign(socket, :flow_task_name, nil)
      {:noreply, apply_flow_reconciliation(socket, result)}
    else
      {:noreply, socket}
    end
  end

  def handle_async({:flow_reconcile, generation}, {:exit, reason}, socket) do
    if socket.assigns.flow_task_name == {:flow_reconcile, generation} do
      {:noreply,
       socket
       |> assign(:flow_task_name, nil)
       |> put_flash(:error, error_label(reason))
       |> maybe_schedule_pending_flow_work()
       |> maybe_schedule_fallback_poll()}
    else
      {:noreply, socket}
    end
  end

  def handle_async({:flow_delta, generation}, {:ok, result}, socket) do
    if socket.assigns.flow_task_name == {:flow_delta, generation} do
      socket = assign(socket, :flow_task_name, nil)
      {:noreply, apply_flow_delta_result(socket, result)}
    else
      {:noreply, socket}
    end
  end

  def handle_async({:flow_delta, generation}, {:exit, reason}, socket) do
    if socket.assigns.flow_task_name == {:flow_delta, generation} do
      {:noreply,
       socket
       |> assign(:flow_task_name, nil)
       |> put_flash(:error, error_label(reason))
       |> maybe_schedule_pending_flow_work()}
    else
      {:noreply, socket}
    end
  end

  def handle_async({:page_mutation, generation}, {:ok, result}, socket) do
    if socket.assigns.flow_task_name == {:page_mutation, generation} do
      socket = assign(socket, :flow_task_name, nil)

      {:noreply,
       socket
       |> apply_page_mutation(result)
       |> finish_page_mutation(result)}
    else
      {:noreply, socket}
    end
  end

  def handle_async({:page_mutation, generation}, {:exit, reason}, socket) do
    if socket.assigns.flow_task_name == {:page_mutation, generation} do
      socket =
        socket
        |> assign(:flow_task_name, nil)
        |> put_flash(:error, error_label(reason))

      {:noreply, finish_page_mutation(socket, {:task_exit, reason})}
    else
      {:noreply, socket}
    end
  end

  defp apply_flow_reconciliation(socket, result) do
    case result do
      {:ok, first_page, steps, last_page, boundaries} ->
        attempts =
          Enum.map(
            steps,
            &attempt_from_flow_step(
              &1,
              first_page.header.root_run_id,
              socket.assigns.current_scope
            )
          )

        run =
          first_page
          |> flow_run_from_public(
            socket.assigns.run.back_asset_href,
            socket.assigns.current_scope
          )
          |> Map.put(:attempts, attempts)
          |> Map.put(:flow_next_cursor, last_page.next_cursor)
          |> Map.put(:flow_has_next?, last_page.has_next?)
          |> Map.put(:flow_previous_cursor, first_page.previous_cursor)
          |> Map.put(:flow_has_previous?, first_page.has_previous?)
          |> Map.put(:flow_lower_anchor, first_boundary_lower(boundaries))
          |> Map.put(:flow_boundaries, boundaries)
          |> Map.put(:flow_anchor_history, [])

        socket
        |> assign(:run, run)
        |> assign(:flow_reconcile_required?, false)
        |> assign_flow()
        |> maybe_schedule_pending_flow_work()

      {:error, reason} ->
        socket
        |> put_flash(:error, error_label(reason))
        |> maybe_schedule_pending_flow_work()
        |> maybe_schedule_fallback_poll()
    end
  end

  defp reconcile_flow_pages(context, run_id, prefix, target, cursor, attempts, pages) do
    remaining = target - length(attempts)

    if remaining <= 0 do
      {page, _cursor} = List.last(pages)
      {:ok, first_page(pages), attempts, page, flow_page_boundaries(pages)}
    else
      opts = [limit: min(@flow_page_size, remaining), asset_prefix: prefix]
      opts = if cursor, do: Keyword.put(opts, :after, cursor), else: opts

      case get_operator_run_flow(context, run_id, opts) do
        {:ok, %{kind: :flow, page: page}} ->
          continue_reconcile_flow(context, run_id, prefix, target, cursor, attempts, pages, page)

        {:ok, %FavnOrchestrator.OperatorRunFlow.Page{} = page} ->
          continue_reconcile_flow(context, run_id, prefix, target, cursor, attempts, pages, page)

        {:error, reason} ->
          {:error, reason}

        _submission ->
          {:error, :unavailable}
      end
    end
  end

  defp continue_reconcile_flow(context, run_id, prefix, target, cursor, attempts, pages, page) do
    pages = pages ++ [{page, cursor}]
    next_attempts = attempts ++ page.items

    if page.has_next? and length(next_attempts) < target do
      reconcile_flow_pages(
        context,
        run_id,
        prefix,
        target,
        page.next_cursor,
        next_attempts,
        pages
      )
    else
      {:ok, first_page(pages), next_attempts, page, flow_page_boundaries(pages)}
    end
  end

  defp first_page([{page, _cursor} | _]), do: page

  defp flow_page_boundaries(pages) do
    pages
    |> Enum.map(fn {page, lower} -> %{first: page.previous_cursor, lower: lower} end)
    |> Enum.reject(&is_nil(&1.first))
  end

  defp first_boundary_lower([boundary | _]), do: boundary.lower
  defp first_boundary_lower([]), do: nil

  defp refresh_run(socket) do
    cond do
      socket.assigns.active_mode == :flow and socket.assigns.run[:found?] and
          socket.assigns.flow_reconcile_required? ->
        reconcile_active_screen(socket)

      socket.assigns.active_mode == :flow and socket.assigns.run[:found?] and
          not legacy_flow_test_adapter?() ->
        refresh_flow_delta(socket)

      socket.assigns.active_mode in [:windows, :events] and socket.assigns.run[:found?] ->
        refresh_summary_mode(socket, socket.assigns.active_mode)

      true ->
        refresh_broad_mode(socket)
    end
  end

  defp refresh_broad_mode(socket) do
    run =
      load_run(
        operator_context(socket),
        socket.assigns.run_id,
        socket.assigns.run[:back_asset_href],
        socket.assigns.active_mode,
        socket.assigns.current_scope
      )

    attempts_remaining = next_load_attempts(socket.assigns.detail_load_attempts_remaining, run)
    run = Map.put(run, :initializing?, !run.found? and attempts_remaining > 0)
    run = preserve_selected_attempt(socket.assigns.run, run, socket.assigns.selected_attempt_id)

    socket
    |> assign(:run, run)
    |> assign(:detail_load_attempts_remaining, attempts_remaining)
    |> assign_flow()
    |> RunEventRefresh.mark_refreshed(run_event_sequences(run))
    |> sync_run_event_refresh()
    |> maybe_schedule_fallback_poll()
  end

  defp refresh_flow_delta(socket) do
    if socket.assigns.flow_task_name do
      socket
    else
      start_flow_delta(socket)
    end
  end

  defp refresh_summary_mode(%{assigns: %{flow_task_name: task}} = socket, _mode)
       when not is_nil(task),
       do: assign(socket, :summary_refresh_pending?, true)

  defp refresh_summary_mode(socket, mode) do
    repair_generation =
      if socket.assigns.summary_repair_required?,
        do: socket.assigns.flow_repair_generation,
        else: nil

    socket
    |> assign(:summary_refresh_pending?, false)
    |> assign(:summary_task_watermark, socket.assigns.summary_pending_watermark)
    |> assign(:summary_pending_watermark, nil)
    |> assign(:summary_task_repair_generation, repair_generation)
    |> start_page_mutation({:refresh_mode, mode})
  end

  defp start_flow_delta(socket) do
    run = socket.assigns.run
    ids = Enum.map(run.attempts, & &1.asset_step_id)
    acknowledged = run.flow_projection_cursor || 0
    through = socket.assigns.flow_pending_watermark || 9_223_372_036_854_775_807
    generation = socket.assigns.flow_task_generation + 1
    name = {:flow_delta, generation}
    context = operator_context(socket)
    run_id = socket.assigns.run_id
    prefix = socket.assigns.flow_asset_prefix
    deadline = System.monotonic_time(:millisecond) + 5_000

    socket
    |> assign(:flow_task_generation, generation)
    |> assign(:flow_task_name, name)
    |> assign(:flow_pending_watermark, nil)
    |> start_async(name, fn ->
      Orchestrator.with_read_deadline(5_000, fn ->
        drain_flow_delta(
          context,
          run_id,
          prefix,
          ids,
          acknowledged,
          through,
          nil,
          [],
          0,
          deadline
        )
      end)
    end)
  end

  defp apply_flow_delta_result(socket, result) do
    case result do
      {:ok, header, changed, through} ->
        run = socket.assigns.run
        changed_by_id = Map.new(changed, &{&1.asset_step_id, &1})

        attempts =
          Enum.map(run.attempts, fn attempt ->
            case Map.get(changed_by_id, attempt.asset_step_id) do
              nil ->
                attempt

              step ->
                attempt_from_flow_step(step, header.root_run_id, socket.assigns.current_scope)
            end
          end)

        run =
          run
          |> apply_flow_header(header)
          |> Map.put(:attempts, attempts)
          |> Map.put(:flow_projection_cursor, through)

        socket
        |> assign(:run, run)
        |> assign_flow()
        |> maybe_schedule_pending_flow_work()

      {:error, :delta_truncated} ->
        socket
        |> put_flash(:error, "Live changes exceeded the bounded delta; refreshing this range")
        |> start_flow_reconciliation()

      {:error, reason} ->
        socket
        |> put_flash(:error, error_label(reason))
        |> maybe_schedule_pending_flow_work()
        |> maybe_schedule_fallback_poll()
    end
  end

  defp drain_flow_delta(
         context,
         run_id,
         prefix,
         ids,
         acknowledged,
         through,
         cursor,
         acc,
         page_number,
         deadline
       )
       when page_number < 3 do
    opts = [
      asset_prefix: prefix,
      after: cursor,
      limit: @flow_page_size
    ]

    if System.monotonic_time(:millisecond) >= deadline do
      {:error, :timeout}
    else
      case get_operator_run_flow_delta(context, run_id, ids, acknowledged, through, opts) do
        {:ok, page} when page.has_more? ->
          drain_flow_delta(
            context,
            run_id,
            prefix,
            ids,
            acknowledged,
            page.through_publication_id,
            page.next_cursor,
            acc ++ page.items,
            page_number + 1,
            deadline
          )

        {:ok, page} ->
          {:ok, page.header, acc ++ page.items, page.through_publication_id}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp drain_flow_delta(
         _context,
         _run_id,
         _prefix,
         _ids,
         _acknowledged,
         _through,
         _cursor,
         _acc,
         _page_number,
         _deadline
       ) do
    :telemetry.execute(
      [:favn, :view, :operator_read, :delta_recovery],
      %{page_count: 3},
      %{use_case: :flow_delta, reason: :truncated, recovery: :reconcile}
    )

    Logger.debug("Flow delta truncated after three bounded pages; reconciliation required")
    {:error, :delta_truncated}
  end

  defp maybe_schedule_pending_flow_work(%{assigns: %{flow_reconcile_pending?: true}} = socket),
    do: start_flow_reconciliation(socket)

  defp maybe_schedule_pending_flow_work(socket) do
    if socket.assigns.flow_pending_watermark,
      do: RunEventRefresh.schedule_refresh(socket, run_event_refresh_opts(socket)),
      else: maybe_schedule_fallback_poll(socket)
  end

  defp apply_flow_header(run, header) do
    counts = header.counts
    window_counts = header.window_counts || %{total: 0, completed: 0, failed: 0}
    status = normalize_flow_status(header.status)

    run
    |> Map.put(:raw_status, status)
    |> Map.put(:active?, status in [:pending, :running])
    |> Map.put(:status, LogsViewModel.status_label(status))
    |> Map.put(:status_tone, LogsViewModel.status_tone(status))
    |> Map.put(:total_asset_attempts, counts.total)
    |> Map.put(:completed_asset_attempts, counts.completed)
    |> Map.put(:succeeded_asset_attempts, counts.succeeded)
    |> Map.put(:skipped_asset_attempts, counts.skipped)
    |> Map.put(:failed_asset_attempts, counts.failed)
    |> Map.put(:running_asset_attempts, counts.running)
    |> Map.put(:queued_asset_attempts, counts.queued)
    |> Map.put(:planned_asset_attempts, counts.planned)
    |> Map.put(:flow_filtered_total, header.filtered_total)
    |> Map.put(:flow_unfiltered_total, header.unfiltered_total)
    |> Map.put(:total_windows, window_counts.total)
    |> Map.put(:completed_windows, window_counts.completed)
    |> Map.put(:failed_windows, window_counts.failed)
    |> Map.put(:requested_window_counts, window_counts)
    |> Map.put(:backfill_failure_count, header.window_failure_total || 0)
  end

  # The flow is geometry over the attempts and the clock. Recomputing it here
  # rather than in `render/1` keeps it out of the diff when nothing moved, and
  # lets the tick advance the axis without refetching the run. The header's
  # elapsed duration shares the tick so both clocks read the same now.
  defp assign_flow(%{assigns: %{active_mode: :flow, run: %{found?: true} = run}} = socket) do
    run = Map.put(run, :elapsed_duration, duration_or_elapsed(run_timing(run)))

    socket
    |> assign(:run, run)
    |> assign(
      :flow,
      RunFlow.build(run.attempts, active?: run.active?, timezone: socket.assigns.current_scope)
    )
    |> maybe_schedule_flow_tick()
  end

  defp assign_flow(socket) do
    socket
    |> assign(:flow, nil)
    |> assign(:flow_tick_ref, nil)
    |> maybe_schedule_flow_tick()
  end

  defp maybe_schedule_flow_tick(%{assigns: %{active_mode: :flow, run: %{active?: true}}} = socket) do
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

        case Orchestrator.cancel_operator_run(
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
    {:noreply, patch_run_state(socket, selected_attempt_id: attempt_id)}
  end

  def handle_event("apply_flow_filter", %{"flow_filter" => params}, socket) do
    prefix = normalize_flow_prefix(Map.get(params, "asset_prefix"))
    params = run_query_params(:flow, nil, nil) |> maybe_put_query("asset_prefix", prefix)
    {:noreply, push_patch(socket, to: ~p"/runs/#{socket.assigns.run_id}?#{params}")}
  end

  def handle_event("clear_flow_filter", _params, socket) do
    {:noreply, push_patch(socket, to: ~p"/runs/#{socket.assigns.run_id}")}
  end

  def handle_event("load_more_flow", _params, socket) do
    {:noreply, start_page_mutation(socket, {:flow, :append})}
  end

  def handle_event("next_flow", _params, socket) do
    {:noreply, start_page_mutation(socket, {:flow, :next})}
  end

  def handle_event("load_more_windows", _params, socket) do
    {:noreply, start_page_mutation(socket, :windows)}
  end

  def handle_event("load_more_events", _params, socket) do
    {:noreply, start_page_mutation(socket, :events)}
  end

  def handle_event("previous_flow", _params, socket) do
    {:noreply, start_page_mutation(socket, {:flow, :previous})}
  end

  def handle_event("close_attempt", _params, socket) do
    {:noreply, patch_run_state(socket, selected_attempt_id: nil)}
  end

  def handle_event("set_mode", _params, socket), do: {:noreply, socket}

  @impl true
  def render(assigns) do
    if assigns.connected_mount? do
      :ok =
        RunDetailTelemetry.begin_render(
          assigns.active_mode,
          :connected,
          length(assigns.run[:attempts] || [])
        )
    end

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
      flow_filter_form={@flow_filter_form}
      flash={@flash}
    />
    """
  end

  @impl true
  def handle_params(params, _uri, socket) do
    active_mode = active_mode_from_params(params, socket.assigns.active_mode)
    requested_prefix = normalize_flow_prefix(params["asset_prefix"])

    filter_changed? =
      active_mode == :flow and requested_prefix != socket.assigns.flow_asset_prefix

    mode_changed? = active_mode != socket.assigns.active_mode

    socket = if filter_changed?, do: cancel_flow_task(socket), else: socket

    socket =
      socket
      |> assign(:flow_asset_prefix, requested_prefix)
      |> assign(
        :flow_filter_form,
        to_form(%{"asset_prefix" => requested_prefix || ""}, as: :flow_filter)
      )

    {socket, scope_ready?, new_grant, prepared_run} =
      if mode_changed? do
        Orchestrator.with_read_deadline(3_000, fn ->
          {socket, scope_ready?, grant} = prepare_mode_scope(socket, active_mode)

          run =
            if scope_ready?,
              do: load_mode_run(socket, active_mode),
              else: socket.assigns.run

          {socket, scope_ready?, grant, run}
        end)
      else
        {socket, true, nil, nil}
      end

    run =
      cond do
        not scope_ready? ->
          socket.assigns.run
          |> Map.put(:mode_error, "Unable to authorize live updates for this view")

        mode_changed? ->
          prepared_run

        active_mode == socket.assigns.active_mode and not filter_changed? ->
          socket.assigns.run

        true ->
          load_mode_run(socket, active_mode)
      end

    {socket, run} = maybe_cleanup_failed_mode_scope(socket, run, active_mode, new_grant)

    selected_attempt_id = selected_attempt_id_from_params(params)

    run = preserve_selected_attempt(socket.assigns.run, run, selected_attempt_id)
    run = load_selected_attempt(run, socket, active_mode, selected_attempt_id)

    selected_child_run_id =
      selected_child_run_id_from_params(params, run, socket.assigns.run_id)

    active_mode =
      if selected_child_run_id && active_mode == :flow, do: :windows, else: active_mode

    {:noreply,
     socket
     |> assign(
       run: run,
       active_mode: active_mode,
       selected_child_run_id: selected_child_run_id,
       selected_attempt_id: selected_attempt_id
     )
     |> assign_flow()
     |> sync_run_event_refresh()}
  end

  defp prepare_mode_scope(socket, active_mode) do
    context = operator_context(socket)
    run_id = socket.assigns.run_id

    authorization =
      case active_mode do
        :flow -> Orchestrator.authorize_run_subscription(context, run_id)
        _other -> Orchestrator.authorize_execution_group_subscription(context, run_id)
      end

    case authorization do
      {:ok, grant} ->
        socket = reset_run_subscriptions(socket)

        activation =
          case active_mode do
            :flow -> Orchestrator.activate_run_subscription(grant)
            _other -> Orchestrator.activate_execution_group_subscription(grant)
          end

        case activation do
          :ok ->
            {socket
             |> assign(:run_event_subscriptions, MapSet.new([run_id]))
             |> assign(:run_events_live?, true), true, grant}

          {:error, _reason} ->
            {socket, false, nil}
        end

      {:error, _reason} ->
        {socket, false, nil}
    end
  end

  defp maybe_cleanup_failed_mode_scope(socket, run, active_mode, grant) do
    failed? = Map.has_key?(run, :mode_error) or (active_mode == :flow and not run[:found?])

    if grant && failed? do
      case active_mode do
        :flow ->
          Orchestrator.deactivate_run_subscription(operator_context(socket), grant.run_id)

        _other ->
          Orchestrator.deactivate_execution_group_subscription(
            operator_context(socket),
            grant.root_run_id
          )
      end

      {assign(socket, :run_event_subscriptions, MapSet.new()), run}
    else
      {socket, run}
    end
  end

  @impl true
  def terminate(_reason, socket) do
    operator_context = operator_context(socket)
    RunEventRefresh.unsubscribe_all(socket, unsubscribe_scope_fun(socket, operator_context))
    _ = Orchestrator.unsubscribe_run_wakeups(operator_context)
    _ = Orchestrator.unsubscribe_projection_listener()

    :ok
  end

  defp load_run(
         operator_context,
         run_id,
         existing_back_asset_href,
         active_mode,
         timezone,
         extra_opts \\ []
       ) do
    if active_mode == :flow and not legacy_flow_test_adapter?() do
      load_flow_run(operator_context, run_id, existing_back_asset_href, timezone, extra_opts)
    else
      load_legacy_run(operator_context, run_id, existing_back_asset_href, active_mode, timezone)
    end
  end

  defp legacy_flow_test_adapter? do
    is_nil(Application.get_env(:favn_view, :operator_run_flow_fun)) and
      not is_nil(Application.get_env(:favn_view, :operator_run_activity_fun))
  end

  defp load_legacy_run(operator_context, run_id, existing_back_asset_href, active_mode, timezone) do
    opts = [view: detail_view(active_mode), limit: 200]
    opts = if active_mode == :events, do: Keyword.put(opts, :include, [:events]), else: opts

    case get_operator_run_activity(operator_context, run_id, opts) do
      {:ok, %{kind: :run, detail: detail}} ->
        detail_from_execution_group(
          detail,
          operator_context,
          run_id,
          existing_back_asset_href,
          active_mode,
          timezone
        )

      {:ok, %{kind: :submission, submission: submission}} ->
        submission_from_public(submission, timezone)

      {:error, reason} ->
        %{
          id: run_id,
          found?: false,
          not_found?: reason == :not_found,
          error: error_label(reason)
        }
    end
  end

  defp load_mode_run(socket, :flow) do
    load_run(
      operator_context(socket),
      socket.assigns.run_id,
      socket.assigns.run[:back_asset_href],
      :flow,
      socket.assigns.current_scope,
      asset_prefix: socket.assigns.flow_asset_prefix
    )
  end

  defp load_mode_run(socket, :windows) do
    page_windows(socket, nil).assigns.run
  end

  defp load_mode_run(socket, :events) do
    page_events(socket, nil).assigns.run
  end

  defp page_windows(socket, cursor) do
    result = fetch_windows_page(operator_context(socket), socket.assigns.run_id, cursor)

    apply_windows_page(socket, cursor, result)
  end

  defp fetch_windows_page(context, run_id, cursor) do
    fun =
      Application.get_env(
        :favn_view,
        :operator_run_windows_fun,
        &Orchestrator.page_operator_run_windows/3
      )

    Orchestrator.with_read_deadline(3_000, fn ->
      fun.(context, run_id, limit: 50, after: cursor)
    end)
  end

  defp apply_windows_page(socket, cursor, result) do
    case result do
      {:ok, page} ->
        children =
          Enum.map(page.items, &window_run_from_summary(&1, socket.assigns.current_scope))

        run =
          socket.assigns.run
          |> Map.put(:child_runs, children)
          |> Map.put(:child_runs_truncated?, page.has_more?)
          |> Map.put(:total_windows, page.total)
          |> Map.put(:window_current_cursor, cursor)
          |> Map.put(:window_page_cursor, page.next_cursor)
          |> Map.put(:summary_projection_cursor, page.projection_cursor || 0)
          |> Map.delete(:mode_error)

        assign(socket, :run, run)

      {:error, reason} ->
        run =
          socket.assigns.run
          |> Map.put_new(:child_runs, [])
          |> Map.put(:mode_error, error_label(reason))

        assign(socket, :run, run)
    end
  end

  defp page_events(socket, cursor) do
    result = fetch_events_page(operator_context(socket), socket.assigns.run_id, cursor)

    apply_events_page(socket, cursor, result)
  end

  defp fetch_events_page(context, run_id, cursor) do
    fun =
      Application.get_env(
        :favn_view,
        :operator_run_events_fun,
        &Orchestrator.page_operator_run_events/3
      )

    Orchestrator.with_read_deadline(3_000, fn ->
      fun.(context, run_id, limit: 50, after: cursor)
    end)
  end

  defp apply_events_page(socket, cursor, result) do
    case result do
      {:ok, page} ->
        events =
          page.items
          |> Enum.reverse()
          |> Enum.map(&event_summary_from_public(&1, socket.assigns.current_scope))

        run =
          socket.assigns.run
          |> Map.put(:events, events)
          |> Map.put(:event_total, page.total)
          |> Map.put(:event_current_cursor, cursor)
          |> Map.put(:event_page_cursor, page.next_cursor)
          |> Map.put(:events_truncated?, page.has_more?)
          |> Map.put(:summary_projection_cursor, page.projection_cursor || 0)
          |> Map.delete(:mode_error)

        assign(socket, :run, run)

      {:error, reason} ->
        run =
          socket.assigns.run
          |> Map.put_new(:events, [])
          |> Map.put(:mode_error, error_label(reason))

        assign(socket, :run, run)
    end
  end

  defp load_flow_run(operator_context, run_id, existing_back_asset_href, timezone, opts) do
    opts =
      opts
      |> Keyword.put_new(:limit, @flow_page_size)
      |> Keyword.put_new(:asset_prefix, nil)

    case get_operator_run_flow(operator_context, run_id, opts) do
      {:ok, %{kind: :flow, page: page}} ->
        flow_run_from_public(page, existing_back_asset_href, timezone)

      {:ok, %{kind: :submission, submission: submission}} ->
        submission_from_public(submission, timezone)

      {:ok, page} ->
        # Test adapters may still return the page directly.
        flow_run_from_public(page, existing_back_asset_href, timezone)

      {:error, reason} ->
        %{id: run_id, found?: false, not_found?: reason == :not_found, error: error_label(reason)}
    end
  end

  defp loading_run(run_id, back_asset_href) do
    %{
      id: run_id,
      found?: false,
      submission?: false,
      initializing?: true,
      active?: false,
      error: nil,
      back_asset_href: back_asset_href,
      subscribed_run_ids: []
    }
  end

  defp flow_run_from_public(page, existing_back_asset_href, timezone) do
    header = page.header
    attempts = Enum.map(page.items, &attempt_from_flow_step(&1, header.root_run_id, timezone))
    counts = header.counts
    status = normalize_flow_status(header.status)
    active? = status in [:pending, :running]
    cancellable? = if is_boolean(header.cancellable?), do: header.cancellable?, else: active?

    retry_remaining? =
      if is_boolean(header.retry_remaining?),
        do: header.retry_remaining?,
        else: not active? and counts.failed > 0

    window_counts = header.window_counts || %{total: 0, completed: 0, failed: 0}

    window_failures =
      Enum.map(header.window_failures || [], &window_failure_from_flow(&1, timezone))

    %{
      found?: true,
      id: header.run_id,
      root_run_id: header.root_run_id,
      subscribed_run_id: header.run_id,
      subscribed_run_ids: [header.run_id],
      raw_status: status,
      active?: active?,
      cancellable?: cancellable?,
      cancel_run_id: header.run_id,
      cancel_label: "Cancel run",
      retry_remaining?: retry_remaining?,
      retry_remaining_label: "Retry remaining",
      short_id: short_id(header.run_id),
      title: "Run #{short_id(header.run_id)}",
      subtitle: header.target_label || "Run",
      status: LogsViewModel.status_label(status),
      status_tone: LogsViewModel.status_tone(status),
      target: header.target_label || header.target_id || "No target",
      trigger: label(header.trigger_type),
      window: attempts |> List.first() |> then(&(&1 && &1.window_label)),
      started_at: LogsViewModel.timestamp_label(header.started_at, timezone),
      finished_at: LogsViewModel.timestamp_label(header.finished_at, timezone),
      duration:
        LogsViewModel.duration_ms_label(
          duration_ms_between(header.started_at, header.finished_at)
        ),
      duration_ms_raw: duration_ms_between(header.started_at, header.finished_at),
      started_at_raw: header.started_at,
      elapsed_duration:
        duration_or_elapsed(%{
          started_at: header.started_at,
          finished_at: header.finished_at,
          duration_ms: duration_ms_between(header.started_at, header.finished_at)
        }),
      manifest_version_id: header.manifest_version_id || "Unknown",
      total_windows: window_counts.total,
      completed_windows: window_counts.completed,
      failed_windows: window_counts.failed,
      requested_window_counts: window_counts,
      effective_window_count:
        attempts |> Enum.map(& &1.window) |> Enum.reject(&is_nil/1) |> Enum.uniq() |> length(),
      total_asset_attempts: counts.total,
      completed_asset_attempts: counts.completed,
      succeeded_asset_attempts: counts.succeeded,
      skipped_asset_attempts: counts.skipped,
      failed_asset_attempts: counts.failed,
      running_asset_attempts: counts.running,
      queued_asset_attempts: counts.queued,
      planned_asset_attempts: counts.planned,
      progress_label: progress_label(counts.completed, counts.total),
      windows: [],
      requested_windows: [],
      requested_windows_truncated?: false,
      attempts: attempts,
      asset_attempts_truncated?: page.has_next? or page.has_previous?,
      failures: Enum.filter(attempts, &(&1.status_tone == :error)),
      backfill_failures: window_failures,
      backfill_failure_count: header.window_failure_total || 0,
      child_runs: [],
      child_runs_truncated?: false,
      events: [],
      latest_event_summary: nil,
      waiting_activity?: attempts == [] and active?,
      current_activity: current_activity(attempts),
      selected_attempt: nil,
      context: [],
      back_asset_href: existing_back_asset_href,
      raw_run: nil,
      raw_events: nil,
      root_event_sequence: nil,
      run_event_sequences: %{},
      flow_asset_prefix: page.asset_prefix,
      flow_filtered_total: header.filtered_total,
      flow_unfiltered_total: header.unfiltered_total,
      flow_next_cursor: page.next_cursor,
      flow_previous_cursor: page.previous_cursor,
      flow_has_next?: page.has_next?,
      flow_has_previous?: page.has_previous?,
      flow_projection_cursor: header.projection_cursor,
      flow_lower_anchor: nil,
      flow_boundaries:
        if(page.previous_cursor,
          do: [%{first: page.previous_cursor, lower: nil}],
          else: []
        ),
      flow_anchor_history: []
    }
  end

  defp submission_from_public(submission, timezone) do
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
      enqueued_at: timestamp_label(submission.enqueued_at, timezone),
      updated_at: timestamp_label(submission.updated_at, timezone),
      terminal_at: timestamp_label(submission.terminal_at, timezone),
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
         active_mode,
         timezone
       ) do
    attempts = Enum.map(Map.get(detail, :asset_attempts, []), &attempt_from_public(&1, timezone))
    windows = Enum.map(Map.get(detail, :windows, []), &window_from_public(&1, timezone))

    requested_windows =
      detail
      |> Map.get(:requested_windows, [])
      |> Enum.map(&window_from_public(&1, timezone))
      |> Enum.sort_by(&window_sort_key/1)

    events = if active_mode == :events, do: Map.get(detail, :events, []), else: []

    child_runs =
      child_runs_from_public(
        Map.get(detail, :child_runs, []),
        attempts,
        requested_windows,
        timezone
      )

    cancel_target = cancel_target(summary, root_run, child_runs, run_id)

    active? = active_group?(summary)
    failures = Enum.filter(attempts, &(&1.status_tone == :error))

    backfill_failures =
      Enum.map(
        Map.get(detail, :backfill_failures, []),
        &backfill_failure_from_public(&1, timezone)
      )

    target = target_label(summary.target_assets)
    status = group_status(summary)

    effective_scope =
      execution_scope_label(windows, Map.get(detail, :has_non_windowed_assets?, false))

    requested_scope =
      requested_scope_label(
        requested_windows,
        summary.total_windows,
        Map.get(detail, :requested_windows_truncated?, false)
      )

    header_scope = requested_scope || effective_scope

    context_scope =
      context_scope_label(
        requested_scope,
        effective_scope,
        Map.get(detail, :has_non_windowed_assets?, false)
      )

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
      subtitle: subtitle([target, header_scope]),
      status: LogsViewModel.status_label(status),
      status_tone: LogsViewModel.status_tone(status),
      target: target || "No target",
      trigger: label(summary.trigger_type),
      window: header_scope,
      started_at: LogsViewModel.timestamp_label(summary.started_at, timezone),
      finished_at: LogsViewModel.timestamp_label(summary.finished_at, timezone),
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
        Map.get(
          summary,
          :succeeded_asset_attempts,
          max(
            summary.completed_asset_attempts - summary.failed_asset_attempts -
              Map.get(summary, :skipped_asset_attempts, 0),
            0
          )
        ),
      skipped_asset_attempts: Map.get(summary, :skipped_asset_attempts, 0),
      failed_asset_attempts: summary.failed_asset_attempts,
      running_asset_attempts: summary.running_asset_attempts,
      queued_asset_attempts: summary.queued_asset_attempts,
      planned_asset_attempts: Map.get(summary, :planned_asset_attempts, 0),
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
      events: Enum.map(events, &event_from_public(&1, timezone)),
      latest_event_summary: latest_event_summary(detail, events, timezone),
      waiting_activity?: events == [] and active_group?(summary),
      current_activity: current_activity(attempts),
      selected_attempt: nil,
      context: context_items(summary, root_run, target, context_scope),
      back_asset_href:
        existing_back_asset_href ||
          back_asset_href(operator_context, List.first(summary.target_assets)),
      raw_run: nil,
      raw_events: nil,
      root_event_sequence: Map.get(detail, :root_event_sequence),
      run_event_sequences: run_event_sequences_from_public(root_run, child_runs, events, detail)
    }
  end

  defp maybe_schedule_fallback_poll(%{assigns: %{flow_reconcile_required?: true}} = socket) do
    schedule_fallback_poll(socket)
  end

  defp maybe_schedule_fallback_poll(%{assigns: %{summary_repair_required?: true}} = socket) do
    schedule_fallback_poll(socket)
  end

  defp maybe_schedule_fallback_poll(%{assigns: %{summary_pending_watermark: watermark}} = socket)
       when not is_nil(watermark) do
    schedule_fallback_poll(socket)
  end

  defp maybe_schedule_fallback_poll(%{assigns: %{summary_refresh_pending?: true}} = socket) do
    schedule_fallback_poll(socket)
  end

  defp maybe_schedule_fallback_poll(
         %{assigns: %{run: %{submission?: true, active?: true}}} = socket
       ) do
    schedule_fallback_poll(socket)
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
    schedule_fallback_poll(socket)
  end

  defp maybe_schedule_fallback_poll(
         %{assigns: %{run_events_live?: false, run: %{active?: true}}} = socket
       ) do
    schedule_fallback_poll(socket)
  end

  defp maybe_schedule_fallback_poll(socket), do: reset_fallback_poll(socket)

  defp schedule_fallback_poll(socket) do
    if connected?(socket) do
      LiveRefresh.schedule_once(
        socket,
        :fallback_poll_ref,
        :poll_run,
        socket.assigns.fallback_poll_delay_ms
      )
    else
      socket
    end
  end

  defp bump_fallback_backoff(socket) do
    assign(
      socket,
      :fallback_poll_delay_ms,
      min(socket.assigns.fallback_poll_delay_ms * 2, @fallback_max_ms)
    )
  end

  defp reset_fallback_poll(socket) do
    assign(socket, fallback_poll_ref: nil, fallback_poll_delay_ms: @fallback_initial_ms)
  end

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
        &Orchestrator.get_operator_run_activity/3
      )

    if is_function(fun, 3), do: fun.(operator_context, run_id, opts), else: fun.(run_id, opts)
  end

  defp get_operator_run_flow(operator_context, run_id, opts) do
    fun =
      Application.get_env(
        :favn_view,
        :operator_run_flow_fun,
        &Orchestrator.get_operator_run_flow/3
      )

    Orchestrator.with_read_deadline(3_000, fn -> fun.(operator_context, run_id, opts) end)
  end

  defp get_operator_run_flow_delta(
         operator_context,
         run_id,
         asset_step_ids,
         acknowledged,
         through,
         opts
       ) do
    Orchestrator.get_operator_run_flow_delta(
      operator_context,
      run_id,
      asset_step_ids,
      acknowledged,
      through,
      opts
    )
  end

  defp get_operator_run_attempt(operator_context, run_id, asset_step_id) do
    fun =
      Application.get_env(
        :favn_view,
        :operator_run_attempt_fun,
        &Orchestrator.get_operator_run_attempt/3
      )

    Orchestrator.with_read_deadline(3_000, fn ->
      fun.(operator_context, run_id, asset_step_id)
    end)
  end

  defp start_page_mutation(%{assigns: %{flow_task_name: task}} = socket, _mutation)
       when not is_nil(task),
       do: socket

  defp start_page_mutation(socket, mutation) do
    case page_mutation_request(socket, mutation) do
      :noop ->
        socket

      request ->
        generation = socket.assigns.flow_task_generation + 1
        name = {:page_mutation, generation}

        socket
        |> assign(:flow_task_generation, generation)
        |> assign(:flow_task_name, name)
        |> start_async(name, fn -> execute_page_mutation(request) end)
    end
  end

  defp page_mutation_request(socket, {:flow, direction}) do
    run = socket.assigns.run
    retained = length(run.attempts)

    {cursor_key, cursor, limit} =
      case direction do
        :previous ->
          {:before, run.flow_previous_cursor, @flow_page_size}

        :next ->
          {:after, run.flow_next_cursor, @flow_page_size}

        :append ->
          {:after, run.flow_next_cursor, min(@flow_page_size, @flow_retained_limit - retained)}
      end

    if is_nil(cursor) or limit <= 0 do
      :noop
    else
      opts = [
        {cursor_key, cursor},
        {:limit, limit},
        {:asset_prefix, socket.assigns.flow_asset_prefix}
      ]

      {:flow, direction, cursor, operator_context(socket), socket.assigns.run_id, opts}
    end
  end

  defp page_mutation_request(socket, :windows) do
    {:windows, operator_context(socket), socket.assigns.run_id,
     socket.assigns.run[:window_page_cursor]}
  end

  defp page_mutation_request(socket, :events) do
    {:events, operator_context(socket), socket.assigns.run_id,
     socket.assigns.run[:event_page_cursor]}
  end

  defp page_mutation_request(socket, {:refresh_mode, :windows}) do
    {:summary_refresh, :windows, operator_context(socket), socket.assigns.run_id,
     socket.assigns.run[:window_current_cursor]}
  end

  defp page_mutation_request(socket, {:refresh_mode, :events}) do
    {:summary_refresh, :events, operator_context(socket), socket.assigns.run_id,
     socket.assigns.run[:event_current_cursor]}
  end

  defp execute_page_mutation({:flow, direction, cursor, context, run_id, opts}) do
    {:flow, direction, cursor,
     Orchestrator.with_read_deadline(3_000, fn -> get_operator_run_flow(context, run_id, opts) end)}
  end

  defp execute_page_mutation({:windows, context, run_id, cursor}),
    do: {:windows, cursor, fetch_windows_page(context, run_id, cursor)}

  defp execute_page_mutation({:events, context, run_id, cursor}),
    do: {:events, cursor, fetch_events_page(context, run_id, cursor)}

  defp execute_page_mutation({:summary_refresh, :windows, context, run_id, cursor}),
    do: {:summary_refresh, :windows, cursor, fetch_windows_page(context, run_id, cursor)}

  defp execute_page_mutation({:summary_refresh, :events, context, run_id, cursor}),
    do: {:summary_refresh, :events, cursor, fetch_events_page(context, run_id, cursor)}

  defp apply_page_mutation(socket, {:windows, cursor, result}),
    do: apply_windows_page(socket, cursor, result)

  defp apply_page_mutation(socket, {:events, cursor, result}),
    do: apply_events_page(socket, cursor, result)

  defp apply_page_mutation(socket, {:summary_refresh, :windows, cursor, result}),
    do: apply_windows_page(socket, cursor, result)

  defp apply_page_mutation(socket, {:summary_refresh, :events, cursor, result}),
    do: apply_events_page(socket, cursor, result)

  defp apply_page_mutation(socket, {:flow, direction, cursor, {:ok, page}}) do
    run = socket.assigns.run
    page_run = flow_run_from_public(page, run.back_asset_href, socket.assigns.current_scope)
    page_attempts = page_run.attempts

    attempts =
      case direction do
        :previous -> Enum.take(page_attempts ++ run.attempts, @flow_retained_limit)
        _other -> Enum.take(run.attempts ++ page_attempts, -@flow_retained_limit)
      end

    merged =
      run
      |> Map.merge(Map.drop(page_run, [:attempts]))
      |> Map.put(:attempts, attempts)
      |> Map.put(
        :asset_attempts_truncated?,
        page_run.flow_has_next? or page_run.flow_has_previous?
      )
      |> preserve_page_edge(direction, run, page_run, cursor)

    socket
    |> assign(:run, merged)
    |> assign_flow()
  end

  defp apply_page_mutation(socket, {:flow, _direction, _cursor, {:error, reason}}),
    do: put_flash(socket, :error, error_label(reason))

  defp finish_page_mutation(socket, {:summary_refresh, mode, _cursor, {:ok, _page}}) do
    socket =
      socket
      |> acknowledge_summary_refresh()
      |> clear_completed_summary_fallback()

    if socket.assigns.summary_refresh_pending? do
      refresh_summary_mode(socket, mode)
    else
      maybe_schedule_fallback_poll(socket)
    end
  end

  defp finish_page_mutation(socket, {:summary_refresh, _mode, _cursor, {:error, _reason}}),
    do: socket |> restore_failed_summary_refresh() |> maybe_schedule_fallback_poll()

  defp finish_page_mutation(socket, {:task_exit, _reason}) do
    if socket.assigns.active_mode in [:windows, :events] do
      socket |> restore_failed_summary_refresh() |> maybe_schedule_fallback_poll()
    else
      maybe_schedule_pending_flow_work(socket)
    end
  end

  defp finish_page_mutation(
         %{assigns: %{active_mode: mode, summary_refresh_pending?: true}} = socket,
         _result
       )
       when mode in [:windows, :events],
       do: refresh_summary_mode(socket, mode)

  defp finish_page_mutation(socket, _result), do: maybe_schedule_pending_flow_work(socket)

  defp acknowledge_summary_refresh(socket) do
    acknowledged =
      max(
        socket.assigns.summary_acknowledged_watermark,
        max(
          socket.assigns.summary_task_watermark || 0,
          socket.assigns.run[:summary_projection_cursor] || 0
        )
      )

    repair_required? =
      case socket.assigns.summary_task_repair_generation do
        generation
        when is_integer(generation) and generation == socket.assigns.flow_repair_generation ->
          false

        _not_consumed ->
          socket.assigns.summary_repair_required?
      end

    socket
    |> assign(:summary_acknowledged_watermark, acknowledged)
    |> assign(:summary_task_watermark, nil)
    |> assign(:summary_task_repair_generation, nil)
    |> assign(:summary_repair_required?, repair_required?)
  end

  defp restore_failed_summary_refresh(socket) do
    pending =
      case socket.assigns.summary_task_watermark do
        watermark when is_integer(watermark) ->
          max(watermark, socket.assigns.summary_pending_watermark || 0)

        _none ->
          socket.assigns.summary_pending_watermark
      end

    socket
    |> assign(:summary_pending_watermark, pending)
    |> assign(:summary_task_watermark, nil)
    |> assign(:summary_task_repair_generation, nil)
  end

  defp clear_completed_summary_fallback(%{assigns: %{run_events_live?: true}} = socket),
    do: assign(socket, :fallback_poll_ref, nil)

  defp clear_completed_summary_fallback(socket), do: socket

  defp preserve_page_edge(run, direction, old, page, cursor) do
    case direction do
      :append ->
        run
        |> Map.put(:flow_previous_cursor, old.flow_previous_cursor)
        |> Map.put(:flow_next_cursor, page.flow_next_cursor)
        |> Map.put(:flow_has_previous?, old.flow_has_previous?)
        |> Map.put(:flow_has_next?, page.flow_has_next?)
        |> Map.put(
          :flow_boundaries,
          append_flow_boundary(
            old.flow_boundaries,
            page.flow_previous_cursor,
            old.flow_next_cursor
          )
        )

      :next ->
        boundaries = append_flow_boundary(old.flow_boundaries, page.flow_previous_cursor, cursor)
        boundaries = if length(boundaries) > 1, do: tl(boundaries), else: boundaries

        first_boundary =
          List.first(boundaries) || %{first: page.flow_previous_cursor, lower: cursor}

        run
        |> Map.merge(
          Map.take(page, [
            :flow_previous_cursor,
            :flow_next_cursor,
            :flow_has_previous?,
            :flow_has_next?
          ])
        )
        |> Map.put(:flow_previous_cursor, first_boundary.first)
        |> Map.put(:flow_lower_anchor, first_boundary.lower)
        |> Map.put(:flow_boundaries, boundaries)
        |> Map.put(:flow_anchor_history, [flow_navigation_snapshot(old) | old.flow_anchor_history])

      :previous ->
        {snapshot, history} =
          case old.flow_anchor_history do
            [anchor | rest] -> {anchor, rest}
            [] -> {%{}, []}
          end

        run
        |> Map.merge(snapshot)
        |> Map.put_new(:flow_previous_cursor, page.flow_previous_cursor)
        |> Map.put_new(:flow_has_previous?, page.flow_has_previous?)
        |> Map.put(:flow_anchor_history, history)
    end
  end

  defp append_flow_boundary(boundaries, nil, _lower), do: boundaries

  defp append_flow_boundary(boundaries, first, lower) do
    boundary = %{first: first, lower: lower}
    if List.last(boundaries) == boundary, do: boundaries, else: boundaries ++ [boundary]
  end

  defp flow_navigation_snapshot(run) do
    Map.take(run, [
      :flow_previous_cursor,
      :flow_next_cursor,
      :flow_has_previous?,
      :flow_has_next?,
      :flow_lower_anchor,
      :flow_boundaries
    ])
  end

  defp timestamp_label(nil, _timezone), do: nil

  defp timestamp_label(%DateTime{} = value, timezone),
    do: FavnView.Time.format(value, "%b %-d, %Y %H:%M:%S %Z", timezone)

  defp subscribe_run(operator_context, run_id) do
    Application.get_env(
      :favn_view,
      :run_subscribe_fun,
      &Orchestrator.subscribe_run/2
    ).(operator_context, run_id)
  end

  defp unsubscribe_run(operator_context, run_id),
    do: Orchestrator.unsubscribe_run(operator_context, run_id)

  defp subscribe_execution_group(operator_context, run_id),
    do: Orchestrator.subscribe_execution_group(operator_context, run_id)

  defp unsubscribe_execution_group(operator_context, root_run_id),
    do: Orchestrator.unsubscribe_execution_group(operator_context, root_run_id)

  defp list_run_stream_events(operator_context, run_id, opts) do
    fun =
      Application.get_env(
        :favn_view,
        :run_stream_events_fun,
        &Orchestrator.list_run_stream_events/3
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

    {subscribe_fun, unsubscribe_fun} =
      if socket.assigns.active_mode == :flow do
        {&subscribe_run(operator_context, &1), &unsubscribe_run(operator_context, &1)}
      else
        root_run_id = socket.assigns.run[:root_run_id] || socket.assigns.run_id

        {
          &subscribe_execution_group(operator_context, &1),
          fn _run_id -> unsubscribe_execution_group(operator_context, root_run_id) end
        }
      end

    [
      subscribe_fun: subscribe_fun,
      unsubscribe_fun: unsubscribe_fun,
      list_events_fun:
        if(socket.assigns.active_mode == :flow,
          do: fn _run_id, _opts -> {:ok, []} end,
          else: &list_run_stream_events(operator_context, &1, &2)
        ),
      refresh_key: :refresh_timer_ref,
      refresh_message: :refresh_run,
      coalesce_ms: @coalesce_refresh_ms
    ]
  end

  defp reset_run_subscriptions(socket) do
    socket = cancel_flow_task(socket)
    operator_context = operator_context(socket)
    RunEventRefresh.unsubscribe_all(socket, unsubscribe_scope_fun(socket, operator_context))
    assign(socket, :run_event_subscriptions, MapSet.new())
  end

  defp cancel_flow_task(socket) do
    socket =
      if socket.assigns.flow_task_name,
        do: cancel_async(socket, socket.assigns.flow_task_name),
        else: socket

    socket
    |> assign(:flow_task_name, nil)
    |> assign(:flow_pending_watermark, nil)
    |> assign(:flow_reconcile_pending?, false)
    |> assign(:flow_reconcile_required?, false)
    |> assign(:summary_refresh_pending?, false)
    |> assign(:summary_repair_required?, false)
    |> assign(:summary_pending_watermark, nil)
    |> assign(:summary_task_watermark, nil)
    |> assign(:summary_task_repair_generation, nil)
    |> assign(:refresh_timer_ref, nil)
    |> reset_fallback_poll()
    |> assign(:flow_task_generation, socket.assigns.flow_task_generation + 1)
  end

  defp unsubscribe_scope_fun(%{assigns: %{active_mode: :flow}}, operator_context),
    do: &unsubscribe_run(operator_context, &1)

  defp unsubscribe_scope_fun(socket, operator_context) do
    root_run_id = socket.assigns.run[:root_run_id] || socket.assigns.run_id
    fn _run_id -> unsubscribe_execution_group(operator_context, root_run_id) end
  end

  defp patch_run_state(socket, updates) do
    active_mode = Keyword.get(updates, :active_mode, socket.assigns.active_mode)

    selected_attempt_id =
      Keyword.get(updates, :selected_attempt_id, socket.assigns.selected_attempt_id)

    params =
      run_query_params(
        active_mode,
        socket.assigns.selected_child_run_id,
        selected_attempt_id
      )

    push_patch(socket, to: ~p"/runs/#{socket.assigns.run_id}?#{params}")
  end

  defp run_query_params(active_mode, selected_child_run_id, selected_attempt_id) do
    %{}
    |> maybe_put_query("view", active_mode != :flow && Atom.to_string(active_mode))
    |> maybe_put_query("child_run_id", selected_child_run_id)
    |> maybe_put_query("attempt", selected_attempt_id)
  end

  defp normalize_flow_prefix(nil), do: nil

  defp normalize_flow_prefix(prefix) when is_binary(prefix) do
    case String.trim(prefix) do
      "" -> nil
      value -> value
    end
  end

  defp normalize_flow_prefix(_prefix), do: nil

  defp maybe_put_query(params, _key, value) when value in [nil, false, ""], do: params
  defp maybe_put_query(params, key, value), do: Map.put(params, key, value)

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

  defp selected_attempt_id_from_params(%{"attempt" => attempt_id})
       when is_binary(attempt_id) and attempt_id != "",
       do: attempt_id

  defp selected_attempt_id_from_params(_params), do: nil

  defp preserve_selected_attempt(old_run, new_run, attempt_id) when is_binary(attempt_id) do
    new_attempts = Map.get(new_run, :attempts, [])

    if Enum.any?(new_attempts, &(&1.id == attempt_id)) do
      new_run
    else
      case Enum.find(Map.get(old_run, :attempts, []), &(&1.id == attempt_id)) do
        nil -> new_run
        attempt -> Map.put(new_run, :attempts, new_attempts ++ [attempt])
      end
    end
  end

  defp preserve_selected_attempt(_old_run, new_run, _attempt_id), do: new_run

  defp load_selected_attempt(run, socket, :flow, asset_step_id)
       when is_binary(asset_step_id) do
    if legacy_flow_test_adapter?() do
      run
    else
      load_flow_attempt(run, socket, asset_step_id)
    end
  end

  defp load_selected_attempt(run, _socket, _mode, _asset_step_id),
    do: Map.put(run, :selected_attempt, nil)

  defp load_flow_attempt(run, socket, asset_step_id) do
    case get_operator_run_attempt(operator_context(socket), socket.assigns.run_id, asset_step_id) do
      {:ok, detail} ->
        attempt =
          detail.summary
          |> attempt_from_flow_step(run.id, socket.assigns.current_scope)
          |> Map.put(
            :error_summary,
            error_summary(detail.error) || detail.summary.failure_summary
          )
          |> Map.put(:output_metadata, detail.output_metadata)
          |> Map.put(:window, window_from_public(detail.window, socket.assigns.current_scope))

        Map.put(run, :selected_attempt, attempt)

      {:error, reason} ->
        Map.put(run, :selected_attempt, %{id: asset_step_id, detail_error: error_label(reason)})
    end
  end

  defp attempt_from_public(attempt, timezone) do
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
      started_at: LogsViewModel.timestamp_label(attempt.started_at, timezone),
      finished_at: LogsViewModel.timestamp_label(attempt.finished_at, timezone),
      duration: LogsViewModel.duration_ms_label(attempt.duration_ms),
      status: status_label(attempt.status),
      raw_status: attempt.status,
      status_tone: status_tone(attempt.status),
      error_summary: attempt.error_summary,
      output_metadata: Map.get(attempt, :output_metadata),
      window: window_from_public(attempt.window, timezone),
      window_id: window_identity(attempt.window),
      window_label: window_label(attempt.window, timezone) || "No window",
      logs_href: attempt_logs_href(attempt)
    }
  end

  defp attempt_from_flow_step(step, root_run_id, timezone) do
    window = %{
      key: nil,
      label: nil,
      kind: step.window_kind,
      start_at: step.window_start_at,
      end_at: step.window_end_at,
      timezone: step.window_timezone
    }

    window = if Enum.all?(Map.values(window), &is_nil/1), do: nil, else: window

    attempt_from_public(
      %{
        id: step.asset_step_id,
        asset_step_id: step.asset_step_id,
        root_execution_group_id: root_run_id,
        child_run_id: if(step.run_id == root_run_id, do: nil, else: step.run_id),
        run_id: step.run_id,
        asset_key: step.asset_ref,
        asset_ref: step.asset_ref,
        stage: step.stage,
        execution_pool: step.execution_pool,
        queue_reason: step.queue_reason,
        attempt_number: step.attempt_number,
        started_at: step.started_at,
        finished_at: step.finished_at,
        duration_ms: step.duration_ms || duration_ms_between(step.started_at, step.finished_at),
        status: step.status,
        error_summary: step.failure_summary,
        output_metadata: nil,
        window: window
      },
      timezone
    )
  end

  defp window_run_from_summary(summary, timezone) do
    counts = summary.counts

    %{
      id: summary.run_id || summary.window_id,
      window: nil,
      window_label: range_label(summary.window_start_at, summary.window_end_at, timezone),
      status: status_label(summary.status),
      raw_status: summary.status,
      status_tone: status_tone(summary.status),
      assets: asset_count_label(counts.total),
      outcome: asset_outcome_label(counts),
      started_at: LogsViewModel.timestamp_label(summary.window_start_at, timezone),
      finished_at: LogsViewModel.timestamp_label(summary.window_end_at, timezone),
      duration: LogsViewModel.duration_ms_label(summary.duration_ms),
      succeeded_count: counts.succeeded,
      skipped_count: counts.skipped,
      failed_count: counts.failed,
      running_count: counts.running,
      queued_count: counts.queued,
      planned_count: counts.planned,
      attempts: []
    }
  end

  defp window_failure_from_flow(failure, timezone) do
    start_at =
      failure
      |> then(&(Map.get(&1, "window_start_at") || Map.get(&1, :window_start_at)))
      |> flow_datetime()

    end_at =
      failure
      |> then(&(Map.get(&1, "window_end_at") || Map.get(&1, :window_end_at)))
      |> flow_datetime()

    %{
      window_label: range_label(start_at, end_at, timezone) || "Window",
      error_summary:
        Map.get(failure, "error_summary") || Map.get(failure, :error_summary) ||
          "Window failed",
      child_run_id: Map.get(failure, "child_run_id") || Map.get(failure, :child_run_id)
    }
  end

  defp flow_datetime(%DateTime{} = value), do: value

  defp flow_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> datetime
      _invalid -> nil
    end
  end

  defp flow_datetime(_value), do: nil

  defp event_summary_from_public(event, timezone) do
    %{
      sequence: event.sequence,
      raw_status: event.status,
      timestamp: LogsViewModel.timestamp_label(event.occurred_at, timezone),
      event_type: label(event.event_type),
      raw_event_type: event.event_type,
      status: LogsViewModel.status_label(event.status),
      status_tone: LogsViewModel.status_tone(event.status),
      asset: event.asset_step_id,
      summary: event.summary
    }
  end

  defp normalize_flow_status(:succeeded), do: :ok
  defp normalize_flow_status(:failed), do: :error
  defp normalize_flow_status(status), do: status

  defp duration_ms_between(%DateTime{} = started_at, %DateTime{} = finished_at),
    do: max(DateTime.diff(finished_at, started_at, :millisecond), 0)

  defp duration_ms_between(_started_at, _finished_at), do: nil

  defp attempt_logs_href(%{run_id: run_id, asset_step_id: asset_step_id})
       when is_binary(run_id) and is_binary(asset_step_id),
       do: ~p"/runs/#{run_id}/assets/#{asset_step_id}/logs"

  defp attempt_logs_href(_attempt), do: nil

  defp window_from_public(nil, _timezone), do: nil

  defp window_from_public(window, timezone) do
    %{
      id: window_identity(window),
      key: Map.get(window, :key),
      label: window_label(window, timezone) || "No window",
      start_at: Map.get(window, :start_at),
      end_at: Map.get(window, :end_at),
      range_label: range_label(Map.get(window, :start_at), Map.get(window, :end_at), timezone),
      status: status_label(Map.get(window, :status)),
      raw_status: Map.get(window, :status),
      status_tone: status_tone(Map.get(window, :status)),
      child_run_id: Map.get(window, :child_run_id),
      attempt_count: Map.get(window, :attempt_count),
      started_at: LogsViewModel.timestamp_label(Map.get(window, :started_at), timezone),
      finished_at: LogsViewModel.timestamp_label(Map.get(window, :finished_at), timezone),
      duration: LogsViewModel.duration_ms_label(Map.get(window, :duration_ms))
    }
  end

  defp backfill_failure_from_public(failure, timezone) do
    window = window_from_public(Map.get(failure, :window), timezone)
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
      window_label: window_label(window, timezone) || "No window",
      status: status_label(status),
      raw_status: status,
      status_tone: status_tone(status),
      error_summary:
        error_summary(Map.get(failure, :error)) || OperatorErrorLabels.run_failure_detail(nil),
      attempt_count: Map.get(failure, :attempt_count),
      started_at: LogsViewModel.timestamp_label(Map.get(failure, :started_at), timezone),
      finished_at: LogsViewModel.timestamp_label(Map.get(failure, :finished_at), timezone),
      duration: LogsViewModel.duration_ms_label(Map.get(failure, :duration_ms))
    }
  end

  # Both lookups were a scan per child run, so a thirty-window backfill walked
  # every attempt thirty times on every live refresh. One grouping pass each.
  defp child_runs_from_public(child_runs, attempts, requested_windows, timezone) do
    attempts_by_run_id = Enum.group_by(attempts, & &1.run_id)

    windows_by_child_run_id =
      requested_windows
      |> Enum.reject(&is_nil(&1.child_run_id))
      |> Map.new(&{&1.child_run_id, &1})

    child_runs
    |> Enum.map(fn child ->
      child_attempts = Map.get(attempts_by_run_id, child.id, [])

      window =
        Map.get(windows_by_child_run_id, child.id) || window_from_public(child.window, timezone)

      counts = child_asset_counts(child, child_attempts)

      %{
        id: child.id,
        window: window,
        window_label: (window && window.label) || "No window",
        status: status_label(child.status),
        raw_status: child.status,
        status_tone: status_tone(child.status),
        assets: asset_count_label(counts.total),
        outcome: asset_outcome_label(counts),
        started_at: LogsViewModel.timestamp_label(child.started_at, timezone),
        finished_at: LogsViewModel.timestamp_label(child.finished_at, timezone),
        duration: LogsViewModel.duration_ms_label(child.duration_ms),
        succeeded_count: counts.succeeded,
        skipped_count: counts.skipped,
        failed_count: counts.failed,
        running_count: counts.running,
        queued_count: counts.queued,
        planned_count: counts.planned,
        attempts: child_attempts
      }
    end)
    |> Enum.sort_by(&child_run_sort_key/1)
  end

  defp child_asset_counts(%{asset_counts: counts}, _attempts) when is_map(counts), do: counts

  defp child_asset_counts(_child, attempts) do
    %{
      total: length(attempts),
      completed: Enum.count(attempts, &terminal_status?(&1.raw_status)),
      succeeded: Enum.count(attempts, &(&1.raw_status == :ok)),
      skipped: Enum.count(attempts, &(&1.raw_status in [:skipped, :skipped_fresh])),
      failed: Enum.count(attempts, &failed_status?(&1.raw_status)),
      running: Enum.count(attempts, &running_status?(&1.raw_status)),
      queued: Enum.count(attempts, &queued_status?(&1.raw_status)),
      planned: Enum.count(attempts, &(&1.raw_status == :planned))
    }
  end

  defp child_run_sort_key(%{window: %{start_at: %DateTime{} = start_at}, id: id}),
    do: {0, DateTime.to_unix(start_at, :microsecond), id}

  defp child_run_sort_key(%{id: id}), do: {1, 0, id}

  defp asset_count_label(1), do: "1 asset"
  defp asset_count_label(count), do: "#{count} assets"

  defp asset_outcome_label(counts) do
    [
      outcome_part(counts.succeeded, "ran"),
      outcome_part(counts.skipped, "already fresh"),
      outcome_part(counts.failed, "failed"),
      outcome_part(counts.running, "running"),
      outcome_part(counts.queued, "queued"),
      outcome_part(counts.planned, "planned")
    ]
    |> Enum.reject(&is_nil/1)
    |> case do
      [] -> "No asset steps"
      parts -> Enum.join(parts, " · ")
    end
  end

  defp outcome_part(0, _label), do: nil
  defp outcome_part(count, label), do: "#{count} #{label}"

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

  defp event_from_public(event, timezone) do
    %{
      sequence: Map.get(event, :sequence),
      raw_status: Map.get(event, :status),
      timestamp: LogsViewModel.timestamp_label(Map.get(event, :occurred_at), timezone),
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

  defp requested_scope_label([], _total_windows, _truncated?), do: nil

  defp requested_scope_label(_windows, total_windows, true),
    do: requested_window_count_label(total_windows)

  defp requested_scope_label([window], _total_windows, false), do: window.label

  defp requested_scope_label(windows, total_windows, false) do
    first = List.first(windows)
    last = List.last(windows)
    "#{total_windows} windows · #{first.label} – #{last.label}"
  end

  defp requested_window_count_label(1), do: "1 window"
  defp requested_window_count_label(count), do: "#{count} windows"

  defp context_scope_label(requested_scope, _effective_scope, true)
       when is_binary(requested_scope),
       do: requested_scope <> " · includes non-windowed assets"

  defp context_scope_label(requested_scope, effective_scope, _has_non_windowed_assets?),
    do: requested_scope || effective_scope

  defp execution_scope_label(windows, true) do
    case window_range_label(windows) do
      nil -> "No window"
      range -> "No window & #{range}"
    end
  end

  defp execution_scope_label(windows, false), do: window_range_label(windows)

  defp context_items(summary, root_run, target, scope) do
    [
      %{label: "Backfill run", value: summary.id},
      %{label: "Manifest version", value: root_run.manifest_version_id || "Unknown"},
      %{label: "Target", value: target || "No target"},
      %{label: "Trigger", value: label(summary.trigger_type)},
      %{label: "Execution scope", value: scope || "No window metadata"}
    ]
  end

  defp window_sort_key(%{start_at: %DateTime{} = start_at}),
    do: {0, DateTime.to_unix(start_at, :microsecond)}

  defp window_sort_key(_window), do: {1, 0}

  defp back_asset_href(_operator_context, nil), do: nil

  defp back_asset_href(operator_context, ref) do
    ref_string = LogsViewModel.ref_label(ref)

    with {:ok, entries} <- Orchestrator.active_asset_catalogue(operator_context),
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

  defp latest_event_summary(%{latest_event: latest_event}, _events, timezone)
       when not is_nil(latest_event),
       do: latest_event |> event_from_public(timezone) |> Map.get(:summary)

  defp latest_event_summary(_detail, events, timezone),
    do: events |> Enum.map(&event_from_public(&1, timezone)) |> latest_event_summary()

  defp latest_event_summary([]), do: nil
  defp latest_event_summary(events), do: events |> List.last() |> Map.get(:summary)

  defp window_identity(nil), do: "none"

  defp window_identity(window),
    do:
      Enum.find(
        [
          Map.get(window, :key),
          datetime_iso(Map.get(window, :start_at)),
          persisted_window_label(window)
        ],
        &is_binary/1
      ) || "none"

  defp window_label(window, timezone) when is_map(window) do
    case {Map.get(window, :kind) || Map.get(window, "kind"),
          Map.get(window, :start_at) || Map.get(window, "start_at")} do
      {kind, %DateTime{} = start_at} when kind in [:hour, "hour"] ->
        FavnView.Time.format(start_at, "%b %-d %H:00", timezone)

      {kind, %DateTime{} = start_at} when kind in [:day, "day"] ->
        FavnView.Time.format(start_at, "%b %-d", timezone)

      {kind, %DateTime{} = start_at} when kind in [:month, "month"] ->
        FavnView.Time.format(start_at, "%b %Y", timezone)

      {kind, %DateTime{} = start_at} when kind in [:year, "year"] ->
        FavnView.Time.format(start_at, "%Y", timezone)

      _other ->
        persisted_window_label(window)
    end
  end

  defp window_label(_window, _timezone), do: nil

  defp persisted_window_label(%{label: label}) when is_binary(label), do: label
  defp persisted_window_label(%{"label" => label}) when is_binary(label), do: label
  defp persisted_window_label(%{key: key}) when is_binary(key), do: key
  defp persisted_window_label(%{"key" => key}) when is_binary(key), do: key
  defp persisted_window_label(_window), do: nil
  defp datetime_iso(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)
  defp datetime_iso(_datetime), do: nil

  defp range_label(%DateTime{} = start_at, %DateTime{} = end_at, timezone),
    do:
      "#{FavnView.Time.format(start_at, "%b %-d", timezone)} - #{FavnView.Time.format(end_at, "%b %-d", timezone)}"

  defp range_label(_start_at, _end_at, _timezone), do: nil

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

  defp failed_status?(status),
    do: status in [:error, :failed, :timed_out, :cancelled, :blocked]

  defp running_status?(status), do: status in [:running, :retrying]
  defp queued_status?(status), do: status in [:pending, :queued]
  defp status_label(:ok), do: "Succeeded"
  defp status_label(:error), do: "Failed"
  defp status_label(:failed), do: "Failed"
  defp status_label(:pending), do: "Queued"
  defp status_label(:planned), do: "Planned"
  defp status_label(:queued), do: "Queued"
  defp status_label(:running), do: "Running"
  defp status_label(:retrying), do: "Retrying"
  defp status_label(:skipped), do: "Skipped"
  defp status_label(:skipped_fresh), do: "Already fresh"
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
