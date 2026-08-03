defmodule FavnView.AssetCatalogueFilters do
  @moduledoc """
  Pure filter logic for the asset catalogue.

  The three namespace selects are a dependent chain, in relation order:
  connection narrows catalogue, and connection plus catalogue narrow schema. A
  choice that stops existing under a wider selection falls back to `"all"`
  rather than silently filtering everything away. The rules live here, pure and
  testable, so the LiveView only wires events to them.
  """

  @type filters :: %{
          search: String.t(),
          connection: String.t(),
          catalogue: String.t(),
          schema: String.t(),
          scope: String.t()
        }

  # In relation order, each narrowed by every level above it.
  @namespace_levels [:connection, :catalogue, :schema]

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
      %{search: "", connection: "duckdb", catalogue: "all", schema: "all", scope: "all"}
  """
  @spec normalize(map()) :: filters()
  def normalize(params) do
    %{
      search: Map.get(params, "search", ""),
      connection: Map.get(params, "connection", "all"),
      catalogue: Map.get(params, "catalogue", "all"),
      schema: Map.get(params, "schema", "all"),
      scope: Map.get(params, "scope", "all")
    }
  end

  @doc "The default, nothing-narrowed filter state."
  @spec defaults() :: filters()
  def defaults,
    do: %{search: "", connection: "all", catalogue: "all", schema: "all", scope: "all"}

  @doc """
  Whether anything narrows the list beyond its default view.

  ## Examples

      iex> FavnView.AssetCatalogueFilters.narrowed?(FavnView.AssetCatalogueFilters.defaults())
      false

      iex> FavnView.AssetCatalogueFilters.narrowed?(
      ...>   %{FavnView.AssetCatalogueFilters.defaults() | scope: "unhealthy"}
      ...> )
      true
  """
  @spec narrowed?(map()) :: boolean()
  def narrowed?(filters) do
    String.trim(to_string(Map.get(filters, :search, ""))) != "" or
      Enum.any?([:scope | @namespace_levels], &(Map.get(filters, &1, "all") != "all"))
  end

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
  Drops a catalogue or schema choice that the levels above it no longer allow.

  Reconciliation runs in relation order, so narrowing a connection can reset the
  catalogue, and that reset in turn widens which schemas are allowed.

  ## Examples

      iex> assets = [%{connection: "duckdb", catalogue: "raw", schema: "sales"}]
      iex> FavnView.AssetCatalogueFilters.reconcile_namespace(
      ...>   %{connection: "duckdb", catalogue: "mart", schema: "sales"},
      ...>   assets
      ...> )
      %{connection: "duckdb", catalogue: "all", schema: "sales"}

      iex> assets = [%{connection: "duckdb", catalogue: "raw", schema: "sales"}]
      iex> FavnView.AssetCatalogueFilters.reconcile_namespace(
      ...>   %{connection: "duckdb", catalogue: "raw", schema: "finance"},
      ...>   assets
      ...> )
      %{connection: "duckdb", catalogue: "raw", schema: "all"}
  """
  @spec reconcile_namespace(map(), [map()]) :: map()
  def reconcile_namespace(filters, assets) do
    Enum.reduce(@namespace_levels, filters, fn level, acc ->
      if Map.get(acc, level, "all") == "all" or
           Enum.any?(narrow(assets, acc, level), &(Map.get(&1, level) == Map.get(acc, level))) do
        acc
      else
        Map.put(acc, level, "all")
      end
    end)
  end

  @doc """
  Connection select options.
  """
  @spec connection_options([map()]) :: [{String.t(), String.t()}]
  def connection_options(assets), do: options(assets, :connection, "Connection")

  @doc """
  Catalogue select options allowed by the chosen connection.

  ## Examples

      iex> assets = [
      ...>   %{connection: "duckdb", catalogue: "raw", schema: "sales"},
      ...>   %{connection: "postgres", catalogue: "crm", schema: "sales"}
      ...> ]
      iex> FavnView.AssetCatalogueFilters.catalogue_options(assets, %{connection: "duckdb"})
      [{"Catalogue", "all"}, {"Raw", "raw"}]
  """
  @spec catalogue_options([map()], map()) :: [{String.t(), String.t()}]
  def catalogue_options(assets, filters),
    do: assets |> narrow(filters, :catalogue) |> options(:catalogue, "Catalogue")

  @doc """
  Schema select options allowed by the chosen connection and catalogue.

  ## Examples

      iex> assets = [
      ...>   %{connection: "duckdb", catalogue: "raw", schema: "sales"},
      ...>   %{connection: "duckdb", catalogue: "mart", schema: "finance"}
      ...> ]
      iex> FavnView.AssetCatalogueFilters.schema_options(
      ...>   assets,
      ...>   %{connection: "duckdb", catalogue: "raw"}
      ...> )
      [{"Schema", "all"}, {"Sales", "sales"}]
  """
  @spec schema_options([map()], map()) :: [{String.t(), String.t()}]
  def schema_options(assets, filters),
    do: assets |> narrow(filters, :schema) |> options(:schema, "Schema")

  # Assets still allowed by every namespace level above `level`.
  defp narrow(assets, filters, level) do
    above = Enum.take_while(@namespace_levels, &(&1 != level))

    Enum.filter(assets, fn asset ->
      Enum.all?(above, fn key ->
        chosen = Map.get(filters, key, "all")
        chosen == "all" or Map.get(asset, key) == chosen
      end)
    end)
  end

  @doc """
  Assets matching the search, connection, and catalogue.

  ## Examples

      iex> assets = [
      ...>   %{name: "stg_orders", connection: "duckdb", catalogue: "raw", schema: "sales"},
      ...>   %{name: "accounts", connection: "postgres", catalogue: "crm", schema: "sales"}
      ...> ]
      iex> FavnView.AssetCatalogueFilters.filter(
      ...>   assets,
      ...>   %{search: "", connection: "duckdb", catalogue: "all", schema: "all", scope: "all"}
      ...> ) |> Enum.map(& &1.name)
      ["stg_orders"]
  """
  @spec filter([map()], filters()) :: [map()]
  def filter(assets, filters) do
    search = filters.search |> String.downcase() |> String.trim()
    scope = Map.get(filters, :scope, "all")

    Enum.filter(assets, fn asset ->
      matches_search? = search == "" || String.contains?(String.downcase(asset.name), search)

      matches_namespace? =
        Enum.all?(@namespace_levels, fn level ->
          chosen = Map.get(filters, level, "all")
          chosen == "all" or Map.get(asset, level) == chosen
        end)

      matches_search? && matches_namespace? && in_scope?(asset, scope)
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
      |> Enum.map(&Map.get(&1, field))
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
