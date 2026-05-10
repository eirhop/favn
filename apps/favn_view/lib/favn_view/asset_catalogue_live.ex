defmodule FavnView.AssetCatalogueLive do
  @moduledoc false

  use FavnView, :live_view

  alias FavnView.Components.AssetCataloguePage

  @default_filters %{search: "", connection: "all", catalogue: "all"}

  @impl true
  def mount(_params, _session, socket) do
    {assets, error} = load_assets()

    socket =
      assign(socket,
        all_assets: assets,
        assets: assets,
        filters: @default_filters,
        active_mode: :list,
        loading: false,
        error: error,
        nav_items: AssetCataloguePage.nav_items(),
        connection_options: connection_options(assets),
        catalogue_options: catalogue_options(assets)
      )

    {:ok, socket}
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

  def handle_event("set_mode", %{"mode" => mode}, socket) do
    {:noreply, assign(socket, :active_mode, String.to_existing_atom(mode))}
  end

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
    />
    """
  end

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

  defp load_assets do
    case FavnOrchestrator.active_manifest_targets() do
      {:ok, %{assets: targets}} ->
        {Enum.map(targets, &asset_from_target/1), nil}

      {:error, reason} ->
        {[], reason}
    end
  end

  defp asset_from_target(target) do
    relation = Map.get(target, :relation) || %{}

    %{
      id: Map.fetch!(target, :target_id),
      name: relation_field(relation, :name) || asset_name(target),
      connection: relation_field(relation, :connection) || "unknown",
      catalogue: relation_field(relation, :catalog) || "uncatalogued",
      type: target[:type] || "asset",
      status: :unknown,
      last_run_label: "No runs yet"
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
end
