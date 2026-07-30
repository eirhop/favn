defmodule FavnView.AssetCatalogueFilters do
  @moduledoc """
  Pure filter logic for the asset catalogue.

  The connection and catalogue selects are dependent: choosing a connection
  narrows the catalogue options to that connection's catalogues, and a
  catalogue that stops existing under the new connection falls back to `"all"`
  rather than silently filtering everything away. Both rules live here, pure
  and testable, so the LiveView only wires events to them.
  """

  @type filters :: %{search: String.t(), connection: String.t(), catalogue: String.t()}

  @doc """
  Filter state from form params, with `"all"` defaults.

  ## Examples

      iex> FavnView.AssetCatalogueFilters.normalize(%{"connection" => "duckdb"})
      %{search: "", connection: "duckdb", catalogue: "all"}
  """
  @spec normalize(map()) :: filters()
  def normalize(params) do
    %{
      search: Map.get(params, "search", ""),
      connection: Map.get(params, "connection", "all"),
      catalogue: Map.get(params, "catalogue", "all")
    }
  end

  @doc """
  Drops a catalogue choice that does not exist under the chosen connection.

  ## Examples

      iex> assets = [%{connection: "duckdb", catalogue: "sales"}]
      iex> FavnView.AssetCatalogueFilters.reconcile_catalogue(
      ...>   %{search: "", connection: "duckdb", catalogue: "crm"},
      ...>   assets
      ...> )
      %{search: "", connection: "duckdb", catalogue: "all"}

      iex> assets = [%{connection: "duckdb", catalogue: "sales"}]
      iex> FavnView.AssetCatalogueFilters.reconcile_catalogue(
      ...>   %{search: "", connection: "duckdb", catalogue: "sales"},
      ...>   assets
      ...> )
      %{search: "", connection: "duckdb", catalogue: "sales"}
  """
  @spec reconcile_catalogue(filters(), [map()]) :: filters()
  def reconcile_catalogue(%{catalogue: "all"} = filters, _assets), do: filters

  def reconcile_catalogue(filters, assets) do
    valid? =
      assets
      |> Enum.filter(&(filters.connection == "all" or &1.connection == filters.connection))
      |> Enum.any?(&(&1.catalogue == filters.catalogue))

    if valid?, do: filters, else: %{filters | catalogue: "all"}
  end

  @doc """
  Catalogue select options for the chosen connection.

  ## Examples

      iex> assets = [
      ...>   %{connection: "duckdb", catalogue: "sales"},
      ...>   %{connection: "postgres", catalogue: "crm"}
      ...> ]
      iex> FavnView.AssetCatalogueFilters.catalogue_options(assets, "duckdb")
      [{"Catalogue", "all"}, {"Sales", "sales"}]
  """
  @spec catalogue_options([map()], String.t()) :: [{String.t(), String.t()}]
  def catalogue_options(assets, "all"), do: options(assets, :catalogue, "Catalogue")

  def catalogue_options(assets, connection) do
    assets
    |> Enum.filter(&(&1.connection == connection))
    |> options(:catalogue, "Catalogue")
  end

  @doc """
  Connection select options.
  """
  @spec connection_options([map()]) :: [{String.t(), String.t()}]
  def connection_options(assets), do: options(assets, :connection, "Connection")

  @doc """
  Assets matching the search, connection, and catalogue.

  ## Examples

      iex> assets = [
      ...>   %{name: "stg_orders", connection: "duckdb", catalogue: "sales"},
      ...>   %{name: "accounts", connection: "postgres", catalogue: "crm"}
      ...> ]
      iex> FavnView.AssetCatalogueFilters.filter(
      ...>   assets,
      ...>   %{search: "", connection: "duckdb", catalogue: "all"}
      ...> ) |> Enum.map(& &1.name)
      ["stg_orders"]
  """
  @spec filter([map()], filters()) :: [map()]
  def filter(assets, filters) do
    search = filters.search |> String.downcase() |> String.trim()

    Enum.filter(assets, fn asset ->
      matches_search? = search == "" || String.contains?(String.downcase(asset.name), search)
      matches_connection? = filters.connection == "all" || asset.connection == filters.connection
      matches_catalogue? = filters.catalogue == "all" || asset.catalogue == filters.catalogue

      matches_search? && matches_connection? && matches_catalogue?
    end)
  end

  defp options(assets, field, label) do
    options =
      assets
      |> Enum.map(&Map.fetch!(&1, field))
      |> Enum.reject(&(&1 in [nil, ""]))
      |> Enum.uniq()
      |> Enum.sort()
      |> Enum.map(&{option_label(&1), &1})

    [{label, "all"} | options]
  end

  defp option_label(value) do
    value
    |> to_string()
    |> String.replace("_", " ")
    |> String.capitalize()
  end
end
