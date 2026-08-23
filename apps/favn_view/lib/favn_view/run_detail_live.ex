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
  alias FavnView.RunEventRefresh

  @fallback_refresh_ms 5_000
  @coalesce_refresh_ms 1_000
  @valid_modes ~w(flow events)

  @impl true
  def mount(%{"run_id" => run_id} = params, _session, socket) do
    active_mode = active_mode_from_params(params)

    socket =
      socket
      |> assign(
        run_id: run_id,
        run: loading_run(run_id),
        active_mode: active_mode,
        windows: nil,
        windows_loading?: false,
        windows_error: nil,
        cancel_attempt: nil,
        retry_attempt: nil,
        nav_items: AssetCataloguePage.nav_items(:runs)
      )
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
  def handle_params(params, _uri, socket) do
    active_mode = active_mode_from_params(params)

    socket =
      if connected?(socket) and active_mode != socket.assigns.active_mode do
        socket
        |> assign(:active_mode, active_mode)
        |> refresh_run()
      else
        assign(socket, :active_mode, active_mode)
      end

    {:noreply, socket}
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

  def handle_event("load_windows", _params, socket) do
    socket = assign(socket, windows_loading?: true, windows_error: nil)

    case list_run_windows(operator_context(socket), socket.assigns.run_id) do
      {:ok, %{items: items, overflow?: overflow?}} ->
        windows = Enum.map(items, &window_choice(&1, socket.assigns.current_scope))

        windows_error =
          if overflow?, do: "Only the newest 1,000 window runs are available in this selector."

        {:noreply,
         assign(socket,
           windows: windows,
           windows_loading?: false,
           windows_error: windows_error
         )}

      {:error, _reason} ->
        {:noreply,
         assign(socket,
           windows: nil,
           windows_loading?: false,
           windows_error: "Window runs could not be loaded. Try again."
         )}
    end
  end

  def handle_event("switch_window", %{"run_id" => run_id}, socket) do
    allowed? = Enum.any?(socket.assigns.windows || [], &(&1.run_id == run_id))

    if allowed? do
      {:noreply, push_navigate(socket, to: ~p"/runs/#{run_id}")}
    else
      {:noreply, put_flash(socket, :error, "That window run is not available")}
    end
  end

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
      windows={@windows}
      windows_loading?={@windows_loading?}
      windows_error={@windows_error}
      flash={@flash}
    />
    """
  end

  @impl true
  def terminate(_reason, socket) do
    operator_context = operator_context(socket)
    RunEventRefresh.unsubscribe_all(socket, &Orchestrator.unsubscribe_run(operator_context, &1))
    _ = Orchestrator.unsubscribe_run_wakeups(operator_context)
    :ok
  end

  defp refresh_run(socket) do
    {refresh_result, loaded_run} = load_run(socket, socket.assigns.active_mode)
    run = preserve_visible_run(loaded_run, socket.assigns.run)

    socket
    |> assign(:run, run)
    |> record_refresh(refresh_result, loaded_run)
    |> sync_run_subscription()
    |> schedule_fallback()
  end

  defp record_refresh(socket, :ok, run) do
    RunEventRefresh.mark_refreshed(socket, run_event_sequences(run))
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
        {:ok,
         run_from_header(
           header,
           socket,
           Enum.map(events, &event_row(&1, socket.assigns.current_scope))
         )}

      {:ok, %{kind: :submission, submission: submission}} ->
        {:ok, submission_from_public(submission, socket.assigns.current_scope)}

      {:error, reason} ->
        {:error, error_run(socket.assigns.run_id, reason)}
    end
  end

  defp load_run(socket, :flow) do
    case get_run_flow(operator_context(socket), socket.assigns.run_id) do
      {:ok, %{kind: :run, detail: flow}} ->
        {:ok, flow_from_public(flow, socket)}

      {:ok, %{kind: :submission, submission: submission}} ->
        {:ok, submission_from_public(submission, socket.assigns.current_scope)}

      {:error, reason} ->
        {:error, error_run(socket.assigns.run_id, reason)}
    end
  end

  defp flow_from_public(%{header: header, assets: assets, overflow?: overflow?}, socket) do
    run = run_from_header(header, socket, [])

    assets =
      Enum.map(assets, fn asset ->
        asset
        |> Map.from_struct()
        |> Map.update!(:started_at, &timestamp_label(&1, socket.assigns.current_scope))
        |> Map.update!(:finished_at, &timestamp_label(&1, socket.assigns.current_scope))
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
      asset_attempts_truncated?: false,
      events: events,
      subscribed_run_ids: [header.run_id],
      run_event_sequences: %{header.run_id => header.event_sequence},
      back_asset_href: back_asset_href(header.target_id)
    }
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

  defp sync_run_subscription(socket) do
    RunEventRefresh.sync_subscriptions(
      socket,
      [socket.assigns.run_id],
      run_event_sequences(socket.assigns.run),
      run_event_refresh_opts(socket)
    )
  end

  defp schedule_fallback(socket) do
    if connected?(socket) and socket.assigns.active_mode == :flow and socket.assigns.run.active? and
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
      unsubscribe_fun: &Orchestrator.unsubscribe_run(context, &1),
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

  defp subscribe_run(context, run_id) do
    Application.get_env(:favn_view, :run_subscribe_fun, &Orchestrator.subscribe_run/2).(
      context,
      run_id
    )
  end

  defp window_choice(choice, timezone) do
    %{
      run_id: choice.run_id,
      label: window_label(choice.window_start_at, choice.window_end_at, timezone)
    }
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

  defp window_label(nil, nil, _timezone), do: nil

  defp window_label(start_at, end_at, timezone) do
    "#{timestamp_label(start_at, timezone)} – #{timestamp_label(end_at, timezone)}"
  end

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
