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
    {:noreply, socket |> assign(runs: runs, error: error) |> maybe_schedule_refresh()}
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
    case FavnOrchestrator.list_run_summaries(limit: 100) do
      {:ok, runs} -> {Enum.map(runs, &run_from_public/1), nil}
      {:error, reason} -> {[], inspect(reason)}
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
    targets = targets(Map.get(run, :target_refs, []), Map.get(run, :asset_ref))

    %{
      id: run.id,
      short_id: short_id(run.id),
      target: List.first(targets) || "No target",
      targets: targets,
      raw_status: Map.get(run, :status),
      trigger: label(Map.get(run, :kind) || Map.get(run, :role)),
      window: window_label(Map.get(run, :window)) || "-",
      progress: progress_label(Map.get(run, :progress), targets),
      started_at: short_timestamp(Map.get(run, :started_at)),
      duration: LogsViewModel.duration_ms_label(Map.get(run, :duration_ms))
    }
  end

  defp targets([], asset_ref), do: targets(List.wrap(asset_ref), nil)

  defp targets(refs, _asset_ref) do
    case refs |> Enum.map(&LogsViewModel.ref_label/1) |> Enum.reject(&(&1 in [nil, "", "nil"])) do
      [] -> ["No target"]
      targets -> targets
    end
  end

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
