defmodule FavnView.RunsListLive do
  @moduledoc false

  use FavnView, :live_view

  require Logger

  alias FavnView.Orchestrator
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
        more?: false,
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
    assigns = assign_new(assigns, :operator_workspaces, fn -> [] end)

    ~H"""
    <RunsListPage.runs_list_page
      listing={@listing}
      filters={@filters}
      counts={@counts}
      more?={@more?}
      filters_open?={@filters_open?}
      error={@error}
      nav_items={@nav_items}
      current_scope={@current_scope}
      operator_workspaces={@operator_workspaces}
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

  def handle_event("clear_filters", _params, socket) do
    {:noreply, patch(socket, %RunsFilters{})}
  end

  def handle_event("toggle_started_order", _params, socket) do
    {:noreply, patch(socket, RunsFilters.toggle_order(socket.assigns.filters))}
  end

  def handle_event("next_page", _params, socket) do
    last = List.last(socket.assigns.runs)

    filters =
      RunsFilters.next_page(
        socket.assigns.filters,
        last && last.started_at_raw,
        (last && last.id) || ""
      )

    {:noreply, patch(socket, filters)}
  end

  def handle_event("first_page", _params, socket) do
    {:noreply, patch(socket, RunsFilters.first_page(socket.assigns.filters))}
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
    |> assign_page(
      page_execution_groups(operator_context, filters, now, socket.assigns.current_scope),
      filters,
      now
    )
    |> assign(:counts, counts(operator_context, filters, now, socket.assigns.current_scope))
    |> maybe_schedule_fallback_poll()
  end

  # A page that has more behind it, or that started after a cursor, covers only
  # part of the range, so the day enumeration is clamped to what was loaded: a day
  # outside the page is unknown rather than empty.
  defp assign_page(socket, {:ok, %{items: runs, has_more?: more?}}, filters, now) do
    listing =
      RunDays.layout(runs, RunsFilters.window(filters, now, socket.assigns.current_scope), now,
        order: filters.order,
        timezone: socket.assigns.current_scope,
        complete?: not more? and not RunsFilters.paged?(filters)
      )

    assign(socket, runs: runs, listing: listing, more?: more?, error: nil)
  end

  defp assign_page(socket, {:error, reason}, _filters, _now) do
    Logger.error("runs.list failed: #{inspect(reason)}")

    assign(socket,
      runs: [],
      listing: {:flat, []},
      more?: false,
      error: "Backend unavailable"
    )
  end

  defp page_execution_groups(operator_context, filters, now, timezone) do
    case call_page_execution_groups(
           operator_context,
           RunsFilters.store_filters(filters, now, timezone)
         ) do
      {:ok, %{items: items} = page} ->
        {:ok, %{page | items: Enum.map(items, &run_from_public(&1, timezone))}}

      {:error, _reason} = error ->
        error
    end
  end

  defp counts(operator_context, filters, now, timezone) do
    case call_count_execution_groups(
           operator_context,
           RunsFilters.count_filters(filters, now, timezone)
         ) do
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

  # Every seam takes the operator context, because the context is what carries the
  # workspace and the authorization. A one-argument override used to be accepted
  # here, which made "call this boundary read unscoped" a supported shape.
  defp call_page_execution_groups(operator_context, opts) do
    Application.get_env(
      :favn_view,
      :page_execution_groups_fun,
      &Orchestrator.page_execution_groups/2
    ).(operator_context, opts)
  end

  defp call_count_execution_groups(operator_context, opts) do
    Application.get_env(
      :favn_view,
      :count_execution_groups_fun,
      &Orchestrator.count_execution_groups/2
    ).(operator_context, opts)
  end

  defp subscribe_runs(operator_context) do
    Application.get_env(
      :favn_view,
      :runs_subscribe_fun,
      &Orchestrator.subscribe_runs/1
    ).(operator_context)
  end

  defp unsubscribe_runs(operator_context), do: Orchestrator.unsubscribe_runs(operator_context)

  defp run_from_public(group, timezone) do
    target = target(group)
    status = display_status(Map.get(group, :status))
    assets = assets(group)

    %{
      id: group.id,
      short_id: short_id(group.id),
      target: short_target(target.label),
      target_title: target.label,
      target_detail: target.detail,
      assets: assets.label,
      assets_failed: assets.failed,
      status: status,
      status_label: LogsViewModel.status_label(Map.get(group, :status)),
      raw_status: Map.get(group, :status),
      trigger: label(Map.get(group, :trigger_type)),
      started_at: short_time(Map.get(group, :started_at), timezone),
      started_on: short_date(Map.get(group, :started_at), timezone),
      started_at_raw: Map.get(group, :started_at),
      started_at_title: full_timestamp(Map.get(group, :started_at), timezone),
      duration: duration_label(group)
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

  # How much of the work happened: asset steps finished out of asset steps the
  # group has. The runs in a group are a submission detail — one for everything
  # except a backfill, where they are its windows — and no operator opens this page
  # to count submissions.
  defp assets(group) do
    counts = asset_counts(group)

    %{label: assets_label(counts), failed: counts.failed}
  end

  defp asset_counts(group) do
    counts = Map.get(group, :asset_counts) || %{}

    %{
      total: Map.get(counts, :total, 0),
      completed: Map.get(counts, :completed, 0),
      failed: Map.get(counts, :failed, 0)
    }
  end

  # Nothing has run yet, so the plan is the only honest thing to report.
  defp assets_label(%{total: 0}), do: nil
  defp assets_label(%{total: total, completed: total}), do: "#{total} #{assets_word(total)}"

  defp assets_label(%{total: total, completed: completed}),
    do: "#{completed} / #{total} #{assets_word(total)}"

  defp assets_word(1), do: "asset"
  defp assets_word(_count), do: "assets"

  # Tone only. The label comes from `LogsViewModel.status_label/1` applied to the
  # group's own status, so this list and the run detail page — which read the same
  # execution group overview — cannot name one aggregate status two different ways.
  defp display_status(:ok), do: :succeeded
  defp display_status(:error), do: :failed
  defp display_status(:pending), do: :queued
  defp display_status(status), do: status || :unknown

  # The group's own duration, which the projection now measures from when the group
  # started to when it settled. This used to compute a span from `last_activity_at`
  # for multi-run groups, because the projection reported a backfill as finishing
  # the instant it was submitted — a workaround for a wrong number rather than a
  # different question.
  defp duration_label(%{status: status, duration_ms: nil})
       when status in [:running, :pending],
       do: "elapsed"

  defp duration_label(group), do: LogsViewModel.duration_ms_label(Map.get(group, :duration_ms))

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

  defp short_time(%DateTime{} = value, timezone),
    do: FavnView.Time.format(value, "%H:%M:%S", timezone)

  defp short_time(_value, _timezone), do: "-"

  # The day headers only group a multi-day range, and a page reached by paging back
  # can start anywhere, so each row carries its own date. The year appears only
  # when it is not this one.
  defp short_date(%DateTime{} = value, timezone) do
    local = FavnView.Time.shift(value, timezone)
    local_now = FavnView.Time.shift(DateTime.utc_now(), timezone)

    if local.year == local_now.year,
      do: Calendar.strftime(local, "%-d %b"),
      else: Calendar.strftime(local, "%-d %b %Y")
  end

  defp short_date(_value, _timezone), do: nil

  defp full_timestamp(%DateTime{} = value, timezone),
    do: FavnView.Time.format(value, "%b %-d, %Y %H:%M:%S %Z", timezone)

  defp full_timestamp(_value, _timezone), do: "Not started"

  defp label(nil), do: "Unknown"

  defp label(value) do
    value |> to_string() |> String.replace("_", " ") |> String.capitalize()
  end
end
