defmodule FavnView.RunsListLive do
  @moduledoc false

  use FavnView, :live_view

  require Logger

  alias FavnView.Components.RunsListPage
  alias FavnView.LiveRefresh
  alias FavnView.LogsViewModel
  alias FavnView.RunDays
  alias FavnView.RunsFilters

  @refresh_interval_ms 1_500
  @coalesce_refresh_ms 100
  @active_statuses [:queued, :running, :incomplete, :pending]

  @impl true
  def mount(_params, _session, socket) do
    socket =
      socket
      |> assign(
        runs: [],
        listing: {:flat, []},
        counts: nil,
        filters: %RunsFilters{},
        truncated?: false,
        filters_open?: false,
        error: nil,
        run_events_live?: false,
        nav_items: RunsListPage.nav_items(:runs)
      )
      |> LiveRefresh.init([:refresh_timer_ref, :fallback_poll_ref])
      |> maybe_subscribe_runs()

    {:ok, socket}
  end

  @impl true
  def handle_params(params, _uri, socket) do
    {:noreply, socket |> assign(:filters, RunsFilters.from_params(params)) |> load_runs()}
  end

  @impl true
  def render(assigns) do
    ~H"""
    <RunsListPage.runs_list_page
      listing={@listing}
      filters={@filters}
      counts={@counts}
      truncated?={@truncated?}
      filters_open?={@filters_open?}
      error={@error}
      nav_items={@nav_items}
    />
    """
  end

  @impl true
  def handle_event("filter_runs", %{"filters" => params}, socket) do
    {:noreply, patch(socket, RunsFilters.change(socket.assigns.filters, params))}
  end

  def handle_event("toggle_filters", _params, socket) do
    {:noreply, update(socket, :filters_open?, &(not &1))}
  end

  def handle_event("toggle_started_order", _params, socket) do
    {:noreply, patch(socket, RunsFilters.toggle_order(socket.assigns.filters))}
  end

  def handle_event("load_more", _params, socket) do
    {:noreply, patch(socket, RunsFilters.grow(socket.assigns.filters))}
  end

  @impl true
  def handle_info({:refresh_runs, token}, socket) do
    case LiveRefresh.take(socket, :refresh_timer_ref, token) do
      {:ok, socket} -> {:noreply, load_runs(socket)}
      {:stale, socket} -> {:noreply, socket}
    end
  end

  def handle_info({:poll_runs, token}, socket) do
    case LiveRefresh.take(socket, :fallback_poll_ref, token) do
      {:ok, socket} -> {:noreply, load_runs(socket)}
      {:stale, socket} -> {:noreply, socket}
    end
  end

  def handle_info({:favn_run_event, _event}, socket) do
    {:noreply, schedule_coalesced_refresh(socket)}
  end

  def handle_info(:favn_persistence_published, socket) do
    {:noreply, schedule_coalesced_refresh(socket)}
  end

  @impl true
  def terminate(_reason, socket) do
    if socket.assigns[:run_events_live?] do
      unsubscribe_runs(socket.assigns.current_scope.operator_context)
    end

    :ok
  end

  defp patch(socket, filters),
    do: push_patch(socket, to: RunsListPage.runs_path(RunsFilters.to_params(filters)))

  # One read for the page and one for the counts. The counts are of the store
  # rather than of this page, so the status buttons keep telling the truth when the
  # page is truncated, and they are narrowed the same way the page is, so the
  # number on a button is the number of rows clicking it produces.
  defp load_runs(socket) do
    now = DateTime.utc_now()
    filters = socket.assigns.filters
    operator_context = socket.assigns.current_scope.operator_context

    socket
    |> assign_page(page_execution_groups(operator_context, filters, now), filters, now)
    |> assign(:counts, counts(operator_context, filters, now))
    |> maybe_schedule_fallback_poll()
  end

  defp assign_page(socket, {:ok, %{items: runs, has_more?: truncated?}}, filters, now) do
    listing =
      RunDays.layout(runs, RunsFilters.window(filters, now), now,
        order: filters.order,
        complete?: not truncated?
      )

    assign(socket, runs: runs, listing: listing, truncated?: truncated?, error: nil)
  end

  defp assign_page(socket, {:error, reason}, _filters, _now) do
    Logger.error("runs.list failed: #{inspect(reason)}")

    assign(socket,
      runs: [],
      listing: {:flat, []},
      truncated?: false,
      error: "Backend unavailable"
    )
  end

  defp page_execution_groups(operator_context, filters, now) do
    case call_page_execution_groups(
           operator_context,
           RunsFilters.store_filters(filters, now)
         ) do
      {:ok, %{items: items} = page} ->
        {:ok, %{page | items: Enum.map(items, &run_from_public/1)}}

      {:error, _reason} = error ->
        error
    end
  end

  defp counts(operator_context, filters, now) do
    case call_count_execution_groups(operator_context, RunsFilters.count_filters(filters, now)) do
      {:ok, counts} when is_map(counts) ->
        counts

      {:error, reason} ->
        Logger.error("runs.counts failed: #{inspect(reason)}")
        nil
    end
  end

  defp schedule_coalesced_refresh(socket) do
    if connected?(socket) do
      LiveRefresh.schedule_once(socket, :refresh_timer_ref, :refresh_runs, @coalesce_refresh_ms)
    else
      socket
    end
  end

  defp maybe_schedule_fallback_poll(%{assigns: %{runs: runs}} = socket) do
    if connected?(socket) and Enum.any?(runs, &(&1.raw_status in @active_statuses)) do
      LiveRefresh.schedule_once(socket, :fallback_poll_ref, :poll_runs, @refresh_interval_ms)
    else
      socket
    end
  end

  defp maybe_subscribe_runs(socket) do
    if connected?(socket) do
      case subscribe_runs(socket.assigns.current_scope.operator_context) do
        :ok -> assign(socket, :run_events_live?, true)
        {:error, _reason} -> socket
      end
    else
      socket
    end
  end

  defp call_page_execution_groups(operator_context, opts) do
    fun =
      Application.get_env(
        :favn_view,
        :page_execution_groups_fun,
        &FavnOrchestrator.page_execution_groups/2
      )

    if is_function(fun, 2), do: fun.(operator_context, opts), else: fun.(opts)
  end

  defp call_count_execution_groups(operator_context, opts) do
    Application.get_env(
      :favn_view,
      :count_execution_groups_fun,
      &FavnOrchestrator.count_execution_groups/2
    ).(operator_context, opts)
  end

  defp subscribe_runs(operator_context) do
    Application.get_env(
      :favn_view,
      :runs_subscribe_fun,
      &FavnOrchestrator.subscribe_runs/1
    ).(operator_context)
  end

  defp unsubscribe_runs(operator_context), do: FavnOrchestrator.unsubscribe_runs(operator_context)

  defp run_from_public(group) do
    target = target(group)
    status = display_status(Map.get(group, :status))
    progress = progress(group)

    %{
      id: group.id,
      short_id: short_id(group.id),
      target: short_target(target.label),
      target_title: target.label,
      target_detail: target.detail,
      status: status,
      status_label: status_label(status),
      raw_status: Map.get(group, :status),
      trigger: label(Map.get(group, :trigger_type)),
      progress: progress,
      started_at: short_timestamp(Map.get(group, :started_at)),
      started_at_raw: Map.get(group, :started_at),
      started_at_title: full_timestamp(Map.get(group, :started_at)),
      duration: duration_label(group, progress)
    }
  end

  # A pipeline run declares every asset it will touch, so naming the first of
  # fourteen and calling the rest "+13 more" describes the plan rather than the
  # submission. The pipeline is what the operator asked for; the assets are how
  # many it covers.
  defp target(group) do
    assets = refs(Map.get(group, :target_assets, []))

    case refs(Map.get(group, :target_pipelines, [])) do
      [pipeline | _rest] -> %{label: pipeline, detail: asset_detail(assets)}
      [] -> %{label: List.first(assets) || "No target", detail: extra_detail(assets)}
    end
  end

  defp asset_detail([]), do: nil
  defp asset_detail([_single]), do: "1 asset"
  defp asset_detail(assets), do: "#{length(assets)} assets"

  defp extra_detail(assets) when length(assets) > 1, do: "+#{length(assets) - 1} more"
  defp extra_detail(_assets), do: nil

  # These counts are of the runs in the group, which is what the group projection
  # holds. For a backfill that is its window runs and the meter is the answer to
  # "how far along"; for a single run there is nothing to divide, so the row says
  # nothing rather than "1 / 1".
  defp progress(group) do
    counts = run_counts(group)
    succeeded = max(counts.completed - counts.failed, 0)

    %{
      total: counts.total,
      segments: [
        %{tone: :success, count: succeeded, label: "ok"},
        %{tone: :error, count: counts.failed, label: "failed"},
        %{tone: :info, count: counts.running, label: "running"},
        %{tone: :neutral, count: counts.queued, label: "queued"}
      ],
      summary: "#{counts.completed} / #{counts.total} runs"
    }
  end

  defp run_counts(group) do
    totals = group |> Map.get(:summary_totals, %{}) |> Map.get(:asset_attempts, %{})

    %{
      total: count(totals, :total, group, :total_asset_attempts),
      completed: count(totals, :completed, group, :completed_asset_attempts),
      failed: count(totals, :failed, group, :failed_asset_attempts),
      running: count(totals, :running, group, :running_asset_attempts),
      queued: count(totals, :queued, group, :queued_asset_attempts)
    }
  end

  defp count(totals, key, group, fallback_key),
    do: Map.get(totals, key) || Map.get(group, fallback_key, 0)

  defp display_status(:ok), do: :succeeded
  defp display_status(:error), do: :failed
  defp display_status(:pending), do: :queued
  defp display_status(status), do: status || :unknown

  defp status_label(:succeeded), do: "Succeeded"
  defp status_label(:failed), do: "Failed"
  defp status_label(:queued), do: "Queued"
  defp status_label(:running), do: "Running"
  defp status_label(:partial), do: "Partial"
  defp status_label(:incomplete), do: "Incomplete"
  defp status_label(:cancelled), do: "Cancelled"
  defp status_label(:timed_out), do: "Timed out"
  defp status_label(_status), do: "Unknown"

  # A backfill's root run is finished as soon as it has submitted its windows, so
  # its own duration is milliseconds while the backfill ran for minutes. The
  # group's row records when it was last touched, and that span is what the
  # operator means by how long it took.
  defp duration_label(group, %{total: total}) when total > 1,
    do: span_label(Map.get(group, :started_at), Map.get(group, :last_activity_at))

  defp duration_label(%{status: status, duration_ms: nil}, _progress)
       when status in [:running, :pending],
       do: "elapsed"

  defp duration_label(group, _progress),
    do: LogsViewModel.duration_ms_label(Map.get(group, :duration_ms))

  defp span_label(%DateTime{} = from, %DateTime{} = to),
    do: LogsViewModel.duration_ms_label(max(DateTime.diff(to, from, :millisecond), 0))

  defp span_label(_from, _to), do: "-"

  defp refs(refs) when is_list(refs) do
    refs
    |> Enum.map(&LogsViewModel.ref_label/1)
    |> Enum.reject(&(&1 in [nil, "", "nil"]))
  end

  defp refs(_refs), do: []

  defp short_target("No target"), do: "No target"

  defp short_target(target) when is_binary(target),
    do: LogsViewModel.display_name(target) || target

  defp short_target(target), do: target

  defp short_id(id) when is_binary(id) and byte_size(id) > 18 do
    binary_part(id, 0, 9) <> "..." <> binary_part(id, byte_size(id) - 6, 6)
  end

  defp short_id(id) when is_binary(id), do: id
  defp short_id(_id), do: "unknown"

  defp short_timestamp(%DateTime{} = value), do: Calendar.strftime(value, "%H:%M:%S")
  defp short_timestamp(_value), do: "-"

  defp full_timestamp(%DateTime{} = value),
    do: Calendar.strftime(value, "%b %-d, %Y %H:%M:%S UTC")

  defp full_timestamp(_value), do: "Not started"

  defp label(nil), do: "Unknown"

  defp label(value) do
    value |> to_string() |> String.replace("_", " ") |> String.capitalize()
  end
end
