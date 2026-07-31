defmodule FavnView.AssetCatalogueFilters do
  @moduledoc """
  Pure filter logic for the asset catalogue.

  The connection and catalogue selects are dependent: choosing a connection
  narrows the catalogue options to that connection's catalogues, and a
  catalogue that stops existing under the new connection falls back to `"all"`
  rather than silently filtering everything away. Both rules live here, pure
  and testable, so the LiveView only wires events to them.
  """

  @type filters :: %{
          search: String.t(),
          connection: String.t(),
          catalogue: String.t(),
          scope: String.t()
        }

  # The three states the catalogue reports are independent, and each has its own
  # bad news: a run that failed, a window that was never materialised, a target
  # that no longer matches its contract. One scope each, because an operator
  # arrives asking about one of them, and the count says whether to bother.
  @scopes [
    %{
      id: "all",
      label: "All",
      icon: "hero-list-bullet",
      tone: :neutral,
      hint: "Every asset in the active manifest"
    },
    %{
      id: "unhealthy",
      label: "Unhealthy",
      icon: "hero-x-circle",
      tone: :error,
      hint: "The last run failed, or the asset is stale, missed, or degraded"
    },
    %{
      id: "incomplete_coverage",
      label: "Missing data",
      icon: "hero-exclamation-circle",
      tone: :warning,
      hint: "Declared windows have never been materialised"
    },
    %{
      id: "target_attention",
      label: "Target",
      icon: "hero-arrow-path-rounded-square",
      tone: :warning,
      hint: "A rebuild, a drift, or an operator decision is blocking writes"
    }
  ]

  @unhealthy_statuses [:failed, :missed, :stale, :degraded]
  @target_attention_statuses [:rebuild_required, :unexpected_drift, :operator_decision]

  @doc """
  Filter state from form params, with `"all"` defaults.

  ## Examples

      iex> FavnView.AssetCatalogueFilters.normalize(%{"connection" => "duckdb"})
      %{search: "", connection: "duckdb", catalogue: "all", scope: "all"}
  """
  @spec normalize(map()) :: filters()
  def normalize(params) do
    %{
      search: Map.get(params, "search", ""),
      connection: Map.get(params, "connection", "all"),
      catalogue: Map.get(params, "catalogue", "all"),
      scope: Map.get(params, "scope", "all")
    }
  end

  @doc "The default, nothing-narrowed filter state."
  @spec defaults() :: filters()
  def defaults, do: %{search: "", connection: "all", catalogue: "all", scope: "all"}

  @doc """
  Scope choices for `FavnView.UI.Data.scope_rail/1`.

  Counts come from every asset rather than the filtered page, so the number on a
  button is the number of rows clicking it produces.

  ## Examples

      iex> assets = [
      ...>   %{status: :failed, coverage_status: :complete, compatibility_status: :ready},
      ...>   %{status: :healthy, coverage_status: :incomplete, compatibility_status: :ready}
      ...> ]
      iex> FavnView.AssetCatalogueFilters.scope_choices(assets, %{scope: "all"})
      ...> |> Enum.map(&{&1.id, &1.count})
      [{"all", 2}, {"unhealthy", 1}, {"incomplete_coverage", 1}, {"target_attention", 0}]
  """
  @spec scope_choices([map()], map()) :: [map()]
  def scope_choices(assets, filters) do
    active = Map.get(filters, :scope, "all")

    Enum.map(@scopes, fn scope ->
      Map.merge(scope, %{
        count: Enum.count(assets, &in_scope?(&1, scope.id)),
        count_label: "assets",
        active?: scope.id == active
      })
    end)
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
      ...>   %{search: "", connection: "duckdb", catalogue: "all", scope: "all"}
      ...> ) |> Enum.map(& &1.name)
      ["stg_orders"]
  """
  @spec filter([map()], filters()) :: [map()]
  def filter(assets, filters) do
    search = filters.search |> String.downcase() |> String.trim()
    scope = Map.get(filters, :scope, "all")

    Enum.filter(assets, fn asset ->
      matches_search? = search == "" || String.contains?(String.downcase(asset.name), search)
      matches_connection? = filters.connection == "all" || asset.connection == filters.connection
      matches_catalogue? = filters.catalogue == "all" || asset.catalogue == filters.catalogue

      matches_search? && matches_connection? && matches_catalogue? && in_scope?(asset, scope)
    end)
  end

  defp in_scope?(_asset, "all"), do: true

  defp in_scope?(asset, "unhealthy"), do: Map.get(asset, :status) in @unhealthy_statuses

  defp in_scope?(asset, "incomplete_coverage"),
    do: Map.get(asset, :coverage_status) == :incomplete

  defp in_scope?(asset, "target_attention"),
    do: Map.get(asset, :compatibility_status) in @target_attention_statuses

  defp in_scope?(_asset, _unknown), do: true

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
