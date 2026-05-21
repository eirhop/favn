defmodule FavnView.RunsListLive do
  @moduledoc false

  use FavnView, :live_view

  alias FavnView.Components.RunsListPage
  alias FavnView.LogsViewModel

  @refresh_interval_ms 1_500
  @active_statuses [:queued, :running, :incomplete]
  @valid_modes ~w(list)
  @default_filters %{
    "search" => "",
    "status" => "all",
    "trigger" => "all",
    "target" => "all",
    "window" => "all",
    "only_failed" => "false",
    "only_running" => "false",
    "only_incomplete" => "false",
    "sort" => "started_desc"
  }

  @impl true
  def mount(_params, _session, socket) do
    filters = @default_filters
    {groups, error} = load_groups(filters)

    socket =
      assign(socket,
        groups: groups,
        visible_groups: groups,
        group_details: %{},
        expanded_group_ids: MapSet.new(),
        filters: filters,
        filter_options: filter_options(groups),
        summary: overview_summary(groups),
        active_mode: :list,
        loading: false,
        error: error,
        run_events_live?: false,
        nav_items: RunsListPage.nav_items(:runs)
      )
      |> maybe_subscribe_runs()
      |> maybe_schedule_refresh()

    {:ok, socket}
  end

  @impl true
  def handle_info(:refresh_runs, socket) do
    {groups, error} = load_groups(socket.assigns.filters)

    {:noreply,
     socket
     |> assign_groups(groups, error)
     |> refresh_expanded_details()
     |> maybe_schedule_refresh()}
  end

  def handle_info({:favn_run_event, _event}, socket) do
    {groups, error} = load_groups(socket.assigns.filters)

    {:noreply,
     socket
     |> assign_groups(groups, error)
     |> refresh_expanded_details()
     |> maybe_schedule_refresh()}
  end

  @impl true
  def handle_event("set_mode", %{"mode" => mode}, socket) when mode in @valid_modes do
    {:noreply, assign(socket, :active_mode, String.to_existing_atom(mode))}
  end

  def handle_event("set_mode", _params, socket), do: {:noreply, socket}

  def handle_event("filter_groups", %{"filters" => params}, socket) do
    filters = normalize_filters(socket.assigns.filters, params)
    {visible_groups, error} = load_groups(filters)

    {:noreply,
     assign(socket,
       filters: filters,
       visible_groups: visible_groups,
       summary: overview_summary(visible_groups),
       error: error
     )}
  end

  def handle_event("toggle_group", %{"id" => group_id}, socket) do
    expanded = socket.assigns.expanded_group_ids

    socket =
      if MapSet.member?(expanded, group_id) do
        assign(socket, :expanded_group_ids, MapSet.delete(expanded, group_id))
      else
        socket
        |> assign(:expanded_group_ids, MapSet.put(expanded, group_id))
        |> ensure_group_detail(group_id)
      end

    {:noreply, socket}
  end

  def handle_event("clear_filters", _params, socket) do
    {groups, error} = load_groups(@default_filters)

    {:noreply,
     socket
     |> assign_groups(groups, error)
     |> assign(filters: @default_filters)}
  end

  @impl true
  def render(assigns) do
    ~H"""
    <RunsListPage.runs_list_page
      groups={@visible_groups}
      all_groups={@groups}
      group_details={@group_details}
      expanded_group_ids={@expanded_group_ids}
      filters={@filters}
      filter_options={@filter_options}
      summary={@summary}
      active_mode={@active_mode}
      loading={@loading}
      error={@error}
      nav_items={@nav_items}
    />
    """
  end

  @impl true
  def terminate(_reason, socket) do
    if socket.assigns[:run_events_live?] do
      FavnOrchestrator.unsubscribe_runs()
    end

    :ok
  end

  defp load_groups(filters) do
    case FavnOrchestrator.page_execution_groups(orchestrator_filters(filters)) do
      {:ok, %{items: groups}} -> {Enum.map(groups, &group_from_public/1), nil}
      {:error, reason} -> {[], inspect(reason)}
    end
  end

  defp maybe_schedule_refresh(%{assigns: %{groups: groups}} = socket) do
    if connected?(socket) and Enum.any?(groups, &active_status?(&1.status)) do
      Process.send_after(self(), :refresh_runs, @refresh_interval_ms)
    end

    socket
  end

  defp maybe_schedule_refresh(socket), do: socket

  defp maybe_subscribe_runs(socket) do
    if connected?(socket) do
      case FavnOrchestrator.subscribe_runs() do
        :ok -> assign(socket, :run_events_live?, true)
        {:error, _reason} -> socket
      end
    else
      socket
    end
  end

  defp assign_groups(socket, groups, error) do
    assign(socket,
      groups: groups,
      visible_groups: groups,
      filter_options: filter_options(groups),
      summary: overview_summary(groups),
      error: error
    )
  end

  defp orchestrator_filters(filters) do
    []
    |> Keyword.put(:limit, 100)
    |> maybe_put_search_filter(Map.get(filters, "search"))
    |> maybe_put_status_filter(Map.get(filters, "status"))
    |> maybe_put_atom_filter(:trigger_type, Map.get(filters, "trigger"))
    |> maybe_put_filter(:target_asset, Map.get(filters, "target"))
    |> maybe_put_atom_filter(:window, Map.get(filters, "window"))
    |> maybe_put_boolean_filter(:only_failed, Map.get(filters, "only_failed"))
    |> maybe_put_boolean_filter(:only_running, Map.get(filters, "only_running"))
    |> maybe_put_boolean_filter(:only_incomplete, Map.get(filters, "only_incomplete"))
    |> Keyword.put(:sort, sort_filter(Map.get(filters, "sort")))
  end

  defp maybe_put_search_filter(opts, value) when is_binary(value) and value != "",
    do: Keyword.put(opts, :search, value)

  defp maybe_put_search_filter(opts, _value), do: opts

  defp maybe_put_status_filter(opts, "failed"), do: Keyword.put(opts, :only_failed, true)
  defp maybe_put_status_filter(opts, "running"), do: Keyword.put(opts, :only_running, true)
  defp maybe_put_status_filter(opts, "incomplete"), do: Keyword.put(opts, :only_incomplete, true)
  defp maybe_put_status_filter(opts, "succeeded"), do: Keyword.put(opts, :status, :ok)
  defp maybe_put_status_filter(opts, "partial"), do: Keyword.put(opts, :status, :partial)
  defp maybe_put_status_filter(opts, "queued"), do: Keyword.put(opts, :status, :pending)
  defp maybe_put_status_filter(opts, _value), do: opts

  defp maybe_put_atom_filter(opts, _key, value) when value in [nil, "", "all"], do: opts

  defp maybe_put_atom_filter(opts, key, value) do
    case known_filter_atom(value) do
      nil -> opts
      atom -> Keyword.put(opts, key, atom)
    end
  end

  defp maybe_put_filter(opts, _key, value) when value in [nil, "", "all"], do: opts
  defp maybe_put_filter(opts, key, value), do: Keyword.put(opts, key, value)

  defp maybe_put_boolean_filter(opts, key, "true"), do: Keyword.put(opts, key, true)
  defp maybe_put_boolean_filter(opts, _key, _value), do: opts

  defp sort_filter(value) when value in ["status_priority", "failed_first", "running_first"],
    do: String.to_existing_atom(value)

  defp sort_filter(_value), do: :started_desc

  defp known_filter_atom("backfill"), do: :backfill
  defp known_filter_atom("manual"), do: :manual
  defp known_filter_atom("schedule"), do: :schedule
  defp known_filter_atom("retry"), do: :retry
  defp known_filter_atom("has_window"), do: :has_window
  defp known_filter_atom("no_window"), do: :no_window
  defp known_filter_atom(_value), do: nil

  defp ensure_group_detail(socket, group_id) do
    if Map.has_key?(socket.assigns.group_details, group_id) do
      socket
    else
      case FavnOrchestrator.get_execution_group_detail(group_id) do
        {:ok, detail} ->
          update(socket, :group_details, &Map.put(&1, group_id, detail_from_public(detail)))

        {:error, reason} ->
          update(socket, :group_details, &Map.put(&1, group_id, %{error: inspect(reason)}))
      end
    end
  end

  defp refresh_expanded_details(socket) do
    Enum.reduce(
      socket.assigns.expanded_group_ids,
      assign(socket, :group_details, %{}),
      fn group_id, acc ->
        ensure_group_detail(acc, group_id)
      end
    )
  end

  defp group_from_public(group) do
    targets = targets(Map.get(group, :target_assets, []), nil)
    target = List.first(targets) || "No target"
    status = display_group_status(Map.get(group, :status))
    current_activity = current_activity(Map.get(group, :currently_running_asset_attempts, []))
    window = window_range_label(group)
    progress = progress(group)
    health = health(group)

    %{
      id: group.id,
      short_id: short_id(group.id),
      target: short_target(target),
      target_title: target,
      targets: targets,
      status: status,
      raw_status: Map.get(group, :status),
      trigger: label(Map.get(group, :trigger_type)),
      trigger_type: Map.get(group, :trigger_type),
      window: window,
      window_count_label: window_count_label(group),
      progress: progress,
      health: health,
      current_activity: current_activity,
      started_at: short_timestamp(Map.get(group, :started_at)),
      started_at_sort: timestamp_sort(Map.get(group, :started_at)),
      duration: duration_label(group),
      child_run_ids: Map.get(group, :child_run_ids, []),
      total_windows: Map.get(group, :total_windows, 0),
      completed_windows: Map.get(group, :completed_windows, 0),
      failed_windows: Map.get(group, :failed_windows, 0),
      total_asset_attempts: Map.get(group, :total_asset_attempts, 0),
      completed_asset_attempts: Map.get(group, :completed_asset_attempts, 0),
      failed_asset_attempts: Map.get(group, :failed_asset_attempts, 0),
      running_asset_attempts: Map.get(group, :running_asset_attempts, 0),
      queued_asset_attempts: Map.get(group, :queued_asset_attempts, 0)
    }
  end

  defp detail_from_public(%{child_runs: child_runs, windows: windows}) do
    %{child_runs: Enum.map(child_runs, &child_run_from_public(&1, windows)), windows: windows}
  end

  defp child_run_from_public(run, windows) do
    window = Enum.find(windows, &(Map.get(&1, :child_run_id) == run.id))
    targets = targets(Map.get(run, :target_refs, []), Map.get(run, :asset_ref))
    progress = Map.get(run, :progress)

    %{
      id: run.id,
      short_id: short_id(run.id),
      status: display_run_status(Map.get(run, :status)),
      raw_status: Map.get(run, :status),
      target: short_target(List.first(targets) || "No target"),
      window: window_label(window || Map.get(run, :window)) || "-",
      progress: progress_label(progress, targets),
      started_at: short_timestamp(Map.get(run, :started_at)),
      duration: LogsViewModel.duration_ms_label(Map.get(run, :duration_ms))
    }
  end

  defp normalize_filters(existing, params) do
    @default_filters
    |> Map.merge(existing || %{})
    |> Map.merge(params || %{})
  end

  defp filter_options(groups) do
    %{
      targets: groups |> Enum.flat_map(& &1.targets) |> Enum.uniq() |> Enum.sort(),
      triggers:
        groups
        |> Enum.map(& &1.trigger_type)
        |> Enum.reject(&is_nil/1)
        |> Enum.uniq()
        |> Enum.sort()
    }
  end

  defp overview_summary(groups) do
    health =
      Enum.reduce(groups, %{succeeded: 0, failed: 0, running: 0, queued: 0}, fn group, acc ->
        Map.update!(acc, health_bucket(group.status), &(&1 + 1))
      end)

    %{
      total_groups: length(groups),
      total_windows: Enum.sum(Enum.map(groups, & &1.total_windows)),
      completed_windows: Enum.sum(Enum.map(groups, & &1.completed_windows)),
      total_asset_attempts: Enum.sum(Enum.map(groups, & &1.total_asset_attempts)),
      completed_asset_attempts: Enum.sum(Enum.map(groups, & &1.completed_asset_attempts)),
      failed_asset_attempts: Enum.sum(Enum.map(groups, & &1.failed_asset_attempts)),
      running_asset_attempts: Enum.sum(Enum.map(groups, & &1.running_asset_attempts)),
      queued_asset_attempts: Enum.sum(Enum.map(groups, & &1.queued_asset_attempts)),
      health: health,
      last_updated: short_timestamp(DateTime.utc_now())
    }
  end

  defp health_bucket(:failed), do: :failed
  defp health_bucket(:partial), do: :failed
  defp health_bucket(:running), do: :running
  defp health_bucket(:queued), do: :queued
  defp health_bucket(:incomplete), do: :queued
  defp health_bucket(_status), do: :succeeded

  defp display_group_status(:ok), do: :succeeded
  defp display_group_status(:error), do: :failed
  defp display_group_status(:pending), do: :queued
  defp display_group_status(status), do: status || :unknown

  defp display_run_status(:ok), do: :succeeded
  defp display_run_status(:error), do: :failed
  defp display_run_status(:pending), do: :queued
  defp display_run_status(status), do: status || :unknown

  defp current_activity([attempt | _]) do
    window = window_label(Map.get(attempt, :window)) || "current window"
    %{asset: Map.get(attempt, :asset_key) || Map.get(attempt, :asset_ref), window: window}
  end

  defp current_activity(_attempts), do: nil

  defp progress(group) do
    totals = Map.get(group, :summary_totals, %{})
    window_counts = Map.get(totals, :windows, %{})

    attempt_counts =
      Map.get(totals, :asset_attempts, Map.get(group, :progress, %{})[:counts] || %{})

    completed_windows = Map.get(window_counts, :completed, group.completed_windows)
    total_windows = Map.get(window_counts, :total, group.total_windows)
    completed_attempts = Map.get(attempt_counts, :completed, group.completed_asset_attempts)
    total_attempts = Map.get(attempt_counts, :total, group.total_asset_attempts)
    window_label = "#{completed_windows} / #{total_windows} windows"
    attempt_label = "#{completed_attempts} / #{total_attempts} attempts"
    total = max(total_attempts, 1)
    percent = min(100, round(completed_attempts * 100 / total))

    %{
      window_label: if(group.total_windows > 0, do: window_label, else: "No windows"),
      attempt_label: attempt_label,
      percent: percent,
      title: "#{window_label}; #{attempt_label}",
      tone: progress_tone(group)
    }
  end

  defp progress_tone(%{status: :failed}), do: :error
  defp progress_tone(%{status: :partial}), do: :warning
  defp progress_tone(%{status: :running}), do: :info
  defp progress_tone(%{health: :error}), do: :error
  defp progress_tone(%{health: :warning}), do: :warning
  defp progress_tone(%{health: :active}), do: :info
  defp progress_tone(_group), do: :success

  defp health(group) do
    counts = group |> Map.get(:summary_totals, %{}) |> Map.get(:asset_attempts, %{})
    failed = Map.get(counts, :failed, group.failed_asset_attempts)
    running = Map.get(counts, :running, group.running_asset_attempts)
    queued = Map.get(counts, :queued, group.queued_asset_attempts)
    completed = Map.get(counts, :completed, group.completed_asset_attempts)
    succeeded = max(completed - failed, 0)

    %{
      succeeded: succeeded,
      failed: failed,
      running: running,
      queued: queued,
      status: group.health
    }
  end

  defp window_range_label(%{total_windows: 0}), do: "-"

  defp window_range_label(group) do
    case {Map.get(group, :total_windows), Map.get(group, :completed_windows)} do
      {1, _} -> "1 window"
      {total, _} -> "#{total} windows"
    end
  end

  defp window_count_label(%{total_windows: 0}), do: "No explicit window"
  defp window_count_label(%{total_windows: 1}), do: "1 window"
  defp window_count_label(%{total_windows: total}), do: "#{total} windows"

  defp duration_label(%{status: status, duration_ms: nil}) when status in [:running, :queued],
    do: "elapsed"

  defp duration_label(group), do: LogsViewModel.duration_ms_label(Map.get(group, :duration_ms))

  defp targets([], asset_ref), do: targets(List.wrap(asset_ref), nil)

  defp targets(refs, _asset_ref) do
    case refs |> Enum.map(&LogsViewModel.ref_label/1) |> Enum.reject(&(&1 in [nil, "", "nil"])) do
      [] -> ["No target"]
      targets -> targets
    end
  end

  defp short_target("No target"), do: "No target"

  defp short_target(target) when is_binary(target),
    do: LogsViewModel.display_name(target) || target

  defp short_target(target), do: target

  defp window_label(%{label: label}) when is_binary(label), do: label
  defp window_label(%{"label" => label}) when is_binary(label), do: label
  defp window_label(%{key: key}) when is_binary(key), do: key
  defp window_label(%{"key" => key}) when is_binary(key), do: key
  defp window_label(_window), do: nil

  defp progress_label(%{label: label} = progress, _targets) when is_binary(label) do
    %{label: label, title: progress_title(progress)}
  end

  defp progress_label(_progress, targets) do
    total = length(targets)
    %{label: "-", title: "#{total} target #{if(total == 1, do: "asset", else: "assets")}"}
  end

  defp progress_title(%{counts: counts}) when is_map(counts), do: inspect(counts)
  defp progress_title(%{"counts" => counts}) when is_map(counts), do: inspect(counts)
  defp progress_title(%{label: label}), do: label

  defp active_status?(status), do: status in @active_statuses

  defp timestamp_sort(%DateTime{} = value), do: DateTime.to_unix(value, :microsecond)
  defp timestamp_sort(_value), do: 0

  defp short_id(id) when is_binary(id) and byte_size(id) > 18 do
    binary_part(id, 0, 9) <> "..." <> binary_part(id, byte_size(id) - 6, 6)
  end

  defp short_id(id) when is_binary(id), do: id
  defp short_id(_id), do: "unknown"

  defp short_timestamp(%DateTime{} = value), do: Calendar.strftime(value, "%b %-d %H:%M")
  defp short_timestamp(_value), do: "-"

  defp label(nil), do: "Unknown"

  defp label(value) do
    value
    |> to_string()
    |> String.replace("_", " ")
    |> String.capitalize()
  end
end
