defmodule FavnView.AssetCatalogueLive do
  @moduledoc false

  use FavnView, :live_view

  alias FavnView.AssetRoute
  alias FavnView.Components.AssetCataloguePage

  @default_filters %{search: "", connection: "all", catalogue: "all"}
  @valid_modes ~w(list lineage)
  @valid_lineage_modes ~w(all)

  @impl true
  def mount(_params, _session, socket) do
    {assets, error} = load_assets(socket.assigns.current_scope.operator_context)

    socket =
      assign(socket,
        all_assets: assets,
        assets: assets,
        filters: @default_filters,
        active_mode: :list,
        lineage_view_mode: :all,
        lineage_graph: nil,
        lineage_inspector: nil,
        lineage_selected_id: nil,
        lineage_selected_kind: nil,
        lineage_loading: false,
        lineage_error: nil,
        lineage_search: "",
        lineage_zoom: 62,
        lineage_inspector_open?: true,
        loading: false,
        error: error,
        nav_items: AssetCataloguePage.nav_items(),
        connection_options: connection_options(assets),
        catalogue_options: catalogue_options(assets)
      )

    {:ok, socket}
  end

  @impl true
  def handle_params(params, _uri, socket) do
    active_mode = normalize_mode(Map.get(params, "mode"))

    {:noreply,
     socket
     |> assign(:active_mode, active_mode)
     |> maybe_load_lineage()}
  end

  @impl true
  def handle_event("filter_assets", %{"filters" => params}, socket) do
    filters = normalize_filters(params)

    {:noreply,
     assign(socket,
       filters: filters,
       assets: filter_assets(socket.assigns.all_assets, filters)
     )}
  end

  def handle_event("set_mode", %{"mode" => mode}, socket) when mode in @valid_modes do
    {:noreply, push_patch(socket, to: ~p"/assets?#{mode_query(mode)}")}
  end

  def handle_event("set_mode", %{"mode" => mode}, socket) when mode in @valid_lineage_modes do
    {:noreply,
     socket
     |> assign(:lineage_view_mode, String.to_existing_atom(mode))
     |> load_lineage()}
  end

  def handle_event("set_mode", _params, socket), do: {:noreply, socket}

  def handle_event("select_node", %{"id" => id, "kind" => kind}, socket)
      when kind in ["group", "asset"] do
    {:noreply,
     socket
     |> assign(
       lineage_selected_id: id,
       lineage_selected_kind: String.to_existing_atom(kind),
       lineage_inspector_open?: true
     )
     |> load_lineage_inspector()}
  end

  def handle_event("select_edge", %{"id" => id}, socket) do
    {:noreply,
     socket
     |> assign(
       lineage_selected_id: id,
       lineage_selected_kind: :edge,
       lineage_inspector_open?: true
     )
     |> load_lineage_inspector()}
  end

  def handle_event("close_inspector", _params, socket) do
    {:noreply, assign(socket, :lineage_inspector_open?, false)}
  end

  def handle_event("zoom_in", _params, socket) do
    {:noreply, update(socket, :lineage_zoom, &min(&1 + 8, 140))}
  end

  def handle_event("zoom_out", _params, socket) do
    {:noreply, update(socket, :lineage_zoom, &max(&1 - 8, 35))}
  end

  def handle_event("fit_graph", _params, socket),
    do: {:noreply, assign(socket, :lineage_zoom, 62)}

  @impl true
  def render(assigns) do
    ~H"""
    <AssetCataloguePage.asset_catalogue_page
      assets={@assets}
      filters={@filters}
      active_mode={@active_mode}
      loading={@loading}
      error={@error}
      nav_items={@nav_items}
      connection_options={@connection_options}
      catalogue_options={@catalogue_options}
      lineage_graph={@lineage_graph}
      lineage_inspector={@lineage_inspector}
      lineage_loading={@lineage_loading}
      lineage_error={@lineage_error}
      lineage_search={@lineage_search}
      lineage_zoom={@lineage_zoom}
      lineage_inspector_open?={@lineage_inspector_open?}
    />
    """
  end

  defp maybe_load_lineage(%{assigns: %{active_mode: :lineage}} = socket), do: load_lineage(socket)
  defp maybe_load_lineage(socket), do: socket

  defp load_lineage(socket) do
    opts = [
      view_mode: socket.assigns.lineage_view_mode,
      selected_id: socket.assigns.lineage_selected_id
    ]

    case get_graph(socket, opts) do
      {:ok, graph} ->
        socket
        |> assign(lineage_graph: graph, lineage_error: nil, lineage_loading: false)
        |> load_lineage_inspector()

      {:error, error} ->
        assign(socket,
          lineage_graph: nil,
          lineage_inspector: nil,
          lineage_error: error,
          lineage_loading: false
        )
    end
  end

  defp load_lineage_inspector(%{assigns: %{lineage_selected_id: nil}} = socket),
    do: assign(socket, :lineage_inspector, nil)

  defp load_lineage_inspector(
         %{
           assigns: %{
             lineage_selected_kind: :group,
             lineage_selected_id: id,
             lineage_view_mode: view_mode
           }
         } = socket
       ) do
    assign_lineage_inspector(socket, get_group(socket, id, view_mode: view_mode))
  end

  defp load_lineage_inspector(
         %{
           assigns: %{
             lineage_selected_kind: :asset,
             lineage_selected_id: id,
             lineage_view_mode: view_mode
           }
         } = socket
       ) do
    assign_lineage_inspector(socket, get_asset(socket, id, view_mode: view_mode))
  end

  defp load_lineage_inspector(
         %{
           assigns: %{
             lineage_selected_kind: :edge,
             lineage_selected_id: id,
             lineage_view_mode: view_mode
           }
         } = socket
       ) do
    assign_lineage_inspector(socket, get_edge(socket, id, view_mode: view_mode))
  end

  defp load_lineage_inspector(socket), do: assign(socket, :lineage_inspector, nil)

  defp assign_lineage_inspector(socket, {:ok, inspector}),
    do: assign(socket, lineage_inspector: inspector, lineage_error: nil)

  defp assign_lineage_inspector(socket, {:error, _error}),
    do: assign(socket, :lineage_inspector, nil)

  defp normalize_mode(mode) when mode in @valid_modes, do: String.to_existing_atom(mode)
  defp normalize_mode(_mode), do: :list

  defp mode_query("list"), do: %{}
  defp mode_query(mode), do: %{mode: mode}

  defp get_graph(socket, opts) do
    configured_fun(:lineage_get_graph_fun, &FavnOrchestrator.get_operator_lineage_graph/2).(
      socket.assigns.current_scope.operator_context,
      opts
    )
  end

  defp get_group(socket, id, opts),
    do:
      configured_fun(:lineage_get_group_fun, &FavnOrchestrator.get_operator_lineage_group/3).(
        socket.assigns.current_scope.operator_context,
        id,
        opts
      )

  defp get_asset(socket, id, opts),
    do:
      configured_fun(:lineage_get_asset_fun, &FavnOrchestrator.get_operator_lineage_asset/3).(
        socket.assigns.current_scope.operator_context,
        id,
        opts
      )

  defp get_edge(socket, id, opts),
    do:
      configured_fun(:lineage_get_edge_fun, &FavnOrchestrator.get_operator_lineage_edge/3).(
        socket.assigns.current_scope.operator_context,
        id,
        opts
      )

  defp configured_fun(key, default), do: Application.get_env(:favn_view, key, default)

  defp normalize_filters(params) do
    %{
      search: Map.get(params, "search", ""),
      connection: Map.get(params, "connection", "all"),
      catalogue: Map.get(params, "catalogue", "all")
    }
  end

  defp filter_assets(assets, filters) do
    search = filters.search |> String.downcase() |> String.trim()

    Enum.filter(assets, fn asset ->
      matches_search? = search == "" || String.contains?(String.downcase(asset.name), search)
      matches_connection? = filters.connection == "all" || asset.connection == filters.connection
      matches_catalogue? = filters.catalogue == "all" || asset.catalogue == filters.catalogue

      matches_search? && matches_connection? && matches_catalogue?
    end)
  end

  defp load_assets(operator_context) do
    fun =
      configured_fun(
        :active_asset_catalogue_fun,
        &FavnOrchestrator.active_asset_catalogue/1
      )

    result = if is_function(fun, 1), do: fun.(operator_context), else: fun.()

    case result do
      {:ok, entries} ->
        {Enum.map(entries, &asset_from_entry/1), nil}

      {:error, reason} ->
        {[], reason}
    end
  end

  defp asset_from_entry(entry) do
    relation = Map.get(entry, :relation) || %{}

    %{
      id: Map.fetch!(entry, :target_id),
      route_id: entry |> Map.fetch!(:target_id) |> AssetRoute.to_param(),
      name: relation_field(relation, :name) || asset_name(entry),
      connection: relation_field(relation, :connection) || "unknown",
      catalogue: relation_field(relation, :catalog) || "uncatalogued",
      type: entry[:type] || "asset",
      status: entry[:status] || :unknown,
      last_run_label: last_run_label(entry[:latest_run_at])
    }
  end

  defp connection_options(assets), do: options_from_assets(assets, :connection, "Connection")
  defp catalogue_options(assets), do: options_from_assets(assets, :catalogue, "Catalogue")

  defp options_from_assets(assets, field, label) do
    options =
      assets
      |> Enum.map(&Map.fetch!(&1, field))
      |> Enum.reject(&(&1 in [nil, ""]))
      |> Enum.uniq()
      |> Enum.sort()
      |> Enum.map(&{option_label(&1), &1})

    [{label, "all"} | options]
  end

  defp relation_field(relation, field) do
    Map.get(relation, field) || Map.get(relation, Atom.to_string(field))
  end

  defp asset_name(target) do
    target
    |> Map.get(:asset_ref, target[:label] || target[:target_id])
    |> to_string()
    |> String.split(":")
    |> List.last()
  end

  defp option_label(value) do
    value
    |> String.replace("_", " ")
    |> String.capitalize()
  end

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
end
