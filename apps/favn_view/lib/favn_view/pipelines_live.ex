defmodule FavnView.PipelinesLive do
  @moduledoc false

  use FavnView, :live_view

  alias FavnView.Components.PipelinesPage
  alias FavnView.LogsViewModel

  @default_filters %{search: "", status: "all"}
  @valid_modes ~w(list)

  @impl true
  def mount(_params, _session, socket) do
    {pipelines, error} = load_pipelines()

    socket =
      assign(socket,
        all_pipelines: pipelines,
        pipelines: pipelines,
        filters: @default_filters,
        active_mode: :list,
        loading: false,
        error: error,
        nav_items: PipelinesPage.nav_items(:pipelines),
        status_options: status_options(pipelines)
      )

    {:ok, socket}
  end

  @impl true
  def handle_event("filter_pipelines", %{"filters" => params}, socket) do
    filters = normalize_filters(params)

    {:noreply,
     assign(socket,
       filters: filters,
       pipelines: filter_pipelines(socket.assigns.all_pipelines, filters)
     )}
  end

  def handle_event("set_mode", %{"mode" => mode}, socket) when mode in @valid_modes do
    {:noreply, assign(socket, :active_mode, String.to_existing_atom(mode))}
  end

  def handle_event("set_mode", _params, socket), do: {:noreply, socket}

  @impl true
  def render(assigns) do
    ~H"""
    <PipelinesPage.pipelines_page
      pipelines={@pipelines}
      filters={@filters}
      active_mode={@active_mode}
      loading={@loading}
      error={@error}
      nav_items={@nav_items}
      status_options={@status_options}
    />
    """
  end

  defp load_pipelines do
    case FavnOrchestrator.active_pipeline_catalogue() do
      {:ok, entries} -> {Enum.map(entries, &pipeline_from_entry/1), nil}
      {:error, reason} -> {[], inspect(reason)}
    end
  end

  defp pipeline_from_entry(entry) do
    selected_assets = Map.get(entry, :selected_assets, [])
    status = Map.get(entry, :status, :unknown)

    %{
      id: Map.fetch!(entry, :target_id),
      name: Map.get(entry, :name) || pipeline_name(Map.fetch!(entry, :label)),
      label: Map.fetch!(entry, :label),
      selected_assets: Enum.map(selected_assets, &asset_ref_label/1),
      asset_count: length(selected_assets),
      dependencies: Map.get(entry, :dependencies, :unknown),
      dependencies_label: dependencies_label(Map.get(entry, :dependencies, :unknown)),
      window_label: window_label(Map.get(entry, :window)),
      status: status,
      status_label: status_label(status),
      last_run_label: last_run_label(Map.get(entry, :latest_run_at)),
      runtime_label: LogsViewModel.duration_ms_label(Map.get(entry, :latest_run_duration_ms))
    }
  end

  defp normalize_filters(params) do
    %{
      search: Map.get(params, "search", ""),
      status: Map.get(params, "status", "all")
    }
  end

  defp filter_pipelines(pipelines, filters) do
    search = filters.search |> String.downcase() |> String.trim()

    Enum.filter(pipelines, fn pipeline ->
      search_text =
        [pipeline.name, pipeline.label | pipeline.selected_assets]
        |> Enum.join(" ")
        |> String.downcase()

      matches_search? = search == "" || String.contains?(search_text, search)

      matches_status? =
        filters.status == "all" || Atom.to_string(pipeline.status) == filters.status

      matches_search? && matches_status?
    end)
  end

  defp status_options(pipelines) do
    options =
      pipelines
      |> Enum.map(& &1.status)
      |> Enum.uniq()
      |> Enum.sort()
      |> Enum.map(&{status_label(&1), Atom.to_string(&1)})

    [{"Health", "all"} | options]
  end

  defp pipeline_name(label) do
    label
    |> String.split(".")
    |> List.last()
  end

  defp asset_ref_label(ref) when is_binary(ref) do
    ref
    |> String.split(":")
    |> List.last()
  end

  defp asset_ref_label(ref), do: to_string(ref)

  defp dependencies_label(:all), do: "Include deps"
  defp dependencies_label(:none), do: "Selected only"
  defp dependencies_label(_dependencies), do: "Unknown deps"

  defp window_label(nil), do: "No window"
  defp window_label(%{kind: kind, timezone: timezone}), do: window_label(kind, timezone)
  defp window_label(%{"kind" => kind, "timezone" => timezone}), do: window_label(kind, timezone)
  defp window_label(_window), do: "Windowed"

  defp window_label(kind, nil), do: humanize(kind)
  defp window_label(kind, timezone), do: "#{humanize(kind)} #{timezone}"

  defp status_label(:healthy), do: "Healthy"
  defp status_label(:running), do: "Running"
  defp status_label(:failed), do: "Failed"
  defp status_label(:unknown), do: "Unknown"
  defp status_label(_status), do: "Unknown"

  defp last_run_label(%DateTime{} = datetime) do
    seconds = DateTime.diff(DateTime.utc_now(), datetime, :second)

    cond do
      seconds < 60 -> "just now"
      seconds < 3_600 -> "#{div(seconds, 60)}m ago"
      seconds < 86_400 -> "#{div(seconds, 3_600)}h ago"
      true -> Calendar.strftime(datetime, "%b %-d %H:%M")
    end
  end

  defp last_run_label(_value), do: "No runs yet"

  defp humanize(value) do
    value
    |> to_string()
    |> String.replace("_", " ")
    |> String.capitalize()
  end
end
