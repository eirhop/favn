defmodule FavnView.RunsListLive do
  @moduledoc false

  use FavnView, :live_view

  alias FavnView.Components.RunsListPage
  alias FavnView.LogsViewModel

  @refresh_interval_ms 1_500
  @active_statuses [:pending, :running]
  @valid_modes ~w(list)

  @impl true
  def mount(_params, _session, socket) do
    {runs, error} = load_runs()

    socket =
      assign(socket,
        runs: runs,
        active_mode: :list,
        loading: false,
        error: error,
        nav_items: RunsListPage.nav_items(:runs)
      )
      |> maybe_schedule_refresh()

    {:ok, socket}
  end

  @impl true
  def handle_info(:refresh_runs, socket) do
    {runs, error} = load_runs()

    socket =
      socket
      |> assign(runs: runs, error: error)
      |> maybe_schedule_refresh()

    {:noreply, socket}
  end

  @impl true
  def handle_event("set_mode", %{"mode" => mode}, socket) when mode in @valid_modes do
    {:noreply, assign(socket, :active_mode, String.to_existing_atom(mode))}
  end

  def handle_event("set_mode", _params, socket), do: {:noreply, socket}

  @impl true
  def render(assigns) do
    ~H"""
    <RunsListPage.runs_list_page
      runs={@runs}
      active_mode={@active_mode}
      loading={@loading}
      error={@error}
      nav_items={@nav_items}
    />
    """
  end

  defp load_runs do
    case FavnOrchestrator.list_runs(limit: 100) do
      {:ok, runs} ->
        {Enum.map(runs, &run_from_public/1), nil}

      {:error, reason} ->
        {[], inspect(reason)}
    end
  end

  defp maybe_schedule_refresh(%{assigns: %{runs: runs}} = socket) do
    if connected?(socket) and Enum.any?(runs, &active_status?(&1.raw_status)) do
      Process.send_after(self(), :refresh_runs, @refresh_interval_ms)
    end

    socket
  end

  defp maybe_schedule_refresh(socket), do: socket

  defp run_from_public(run) do
    targets = targets(run)
    progress = progress(run, targets)

    %{
      id: run.id,
      short_id: short_id(run.id),
      target: List.first(targets) || "No target",
      targets: targets,
      raw_status: Map.get(run, :status),
      trigger: trigger_label(Map.get(run, :trigger, %{}), Map.get(run, :submit_kind)),
      window: window_label(Map.get(run, :params, %{}), Map.get(run, :metadata, %{})) || "-",
      progress: progress,
      started_at: short_timestamp(Map.get(run, :started_at)),
      duration:
        duration_label(
          Map.get(run, :started_at),
          Map.get(run, :finished_at),
          Map.get(run, :status)
        )
    }
  end

  defp targets(run) do
    refs =
      case Map.get(run, :target_refs, []) do
        [] -> List.wrap(Map.get(run, :asset_ref))
        refs -> refs
      end

    refs
    |> Enum.map(&LogsViewModel.ref_label/1)
    |> Enum.reject(&(&1 in [nil, "", "nil"]))
    |> case do
      [] -> ["No target"]
      targets -> targets
    end
  end

  defp trigger_label(%{kind: kind}, _submit_kind), do: humanize(kind)
  defp trigger_label(%{"kind" => kind}, _submit_kind), do: humanize(kind)
  defp trigger_label(_trigger, nil), do: "Unknown"
  defp trigger_label(_trigger, submit_kind), do: humanize(submit_kind)

  defp window_label(params, metadata) do
    window =
      Map.get(params, :window) || Map.get(params, "window") || Map.get(metadata, :selected_window) ||
        Map.get(metadata, "selected_window") || Map.get(metadata, :window) ||
        Map.get(metadata, "window")

    cond do
      is_binary(window) ->
        window

      is_map(window) ->
        Map.get(window, :label) || Map.get(window, "label") || Map.get(window, :id) ||
          Map.get(window, "id") || Map.get(window, :key) || Map.get(window, "key")

      true ->
        nil
    end
  end

  defp progress(run, targets) do
    node_results = raw_node_results(run)

    if node_results == [] do
      asset_progress(run, targets)
    else
      step_progress(run, node_results)
    end
  end

  defp asset_progress(run, targets) do
    results = Map.get(run, :asset_results, %{}) |> result_values()
    progress_count(run, targets, results, "asset", "target progress")
  end

  defp step_progress(run, results) do
    progress_count(run, [], results, "step", "execution progress")
  end

  defp progress_count(run, targets, results, unit, fallback_title) do
    total = max(length(targets), length(results))
    done = Enum.count(results, &terminal_result?/1)
    units = pluralize(unit, total)

    cond do
      results == [] && active_status?(Map.get(run, :status)) ->
        %{label: "Waiting", title: "Run accepted. Waiting for #{fallback_title}."}

      total == 0 ->
        %{label: "-", title: "No #{fallback_title} available"}

      true ->
        %{
          label: "#{done}/#{total} #{units}",
          title: "#{done} of #{total} #{units} have reported terminal results"
        }
    end
  end

  defp pluralize(unit, 1), do: unit
  defp pluralize(unit, _total), do: unit <> "s"

  defp result_values(results) when is_map(results), do: Map.values(results)
  defp result_values(results) when is_list(results), do: results
  defp result_values(_results), do: []

  defp raw_node_results(run) do
    run
    |> Map.get(:result, %{})
    |> case do
      %{node_results: results} -> result_values(results)
      %{"node_results" => results} -> result_values(results)
      _other -> []
    end
  end

  defp terminal_result?(result) do
    Map.get(result, :status) in [
      :ok,
      :partial,
      :error,
      :blocked,
      :cancelled,
      :timed_out,
      :skipped_fresh
    ]
  end

  defp active_status?(status), do: status in @active_statuses

  defp short_id(id) when is_binary(id) and byte_size(id) > 18 do
    binary_part(id, 0, 9) <> "..." <> binary_part(id, byte_size(id) - 6, 6)
  end

  defp short_id(id) when is_binary(id), do: id
  defp short_id(_id), do: "unknown"

  defp short_timestamp(%DateTime{} = value), do: Calendar.strftime(value, "%b %-d %H:%M")
  defp short_timestamp(_value), do: "-"

  defp duration_label(started_at, nil, status) when status in @active_statuses do
    LogsViewModel.duration_label(started_at, nil)
  end

  defp duration_label(started_at, finished_at, _status),
    do: LogsViewModel.duration_label(started_at, finished_at)

  defp humanize(value) do
    value
    |> to_string()
    |> String.replace("_", " ")
    |> String.capitalize()
  end
end
