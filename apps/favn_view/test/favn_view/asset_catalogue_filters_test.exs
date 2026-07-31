defmodule FavnView.AssetCatalogueFiltersTest do
  use ExUnit.Case, async: true

  alias FavnView.AssetCatalogueFilters

  doctest AssetCatalogueFilters

  @assets [
    %{name: "stg_orders", connection: "duckdb", catalogue: "sales"},
    %{name: "mart_daily_sales", connection: "duckdb", catalogue: "sales"},
    %{name: "raw_events", connection: "duckdb", catalogue: "platform"},
    %{name: "accounts", connection: "postgres", catalogue: "crm"}
  ]

  test "choosing a connection narrows the catalogue options to its catalogues" do
    assert AssetCatalogueFilters.catalogue_options(@assets, "duckdb") == [
             {"Catalogue", "all"},
             {"Platform", "platform"},
             {"Sales", "sales"}
           ]

    assert AssetCatalogueFilters.catalogue_options(@assets, "postgres") == [
             {"Catalogue", "all"},
             {"Crm", "crm"}
           ]
  end

  test "switching connection resets a catalogue that no longer exists" do
    filters = %{search: "", connection: "postgres", catalogue: "sales"}

    assert AssetCatalogueFilters.reconcile_catalogue(filters, @assets).catalogue == "all"
  end

  test "a still-valid catalogue survives a connection switch" do
    filters = %{search: "", connection: "duckdb", catalogue: "sales"}

    assert AssetCatalogueFilters.reconcile_catalogue(filters, @assets) == filters
  end

  test "search, connection, and catalogue narrow together" do
    filters = %{search: "stg", connection: "duckdb", catalogue: "sales"}

    assert AssetCatalogueFilters.filter(@assets, filters) |> Enum.map(& &1.name) ==
             ["stg_orders"]
  end

  @stateful [
    %{
      name: "failed_asset",
      connection: "duckdb",
      catalogue: "sales",
      status: :failed,
      coverage_status: :complete,
      compatibility_status: :ready
    },
    %{
      name: "stale_asset",
      connection: "duckdb",
      catalogue: "sales",
      status: :stale,
      coverage_status: :complete,
      compatibility_status: :ready
    },
    %{
      name: "gappy_asset",
      connection: "duckdb",
      catalogue: "sales",
      status: :healthy,
      coverage_status: :incomplete,
      compatibility_status: :ready
    },
    %{
      name: "drifted_asset",
      connection: "duckdb",
      catalogue: "sales",
      status: :healthy,
      coverage_status: :complete,
      compatibility_status: :unexpected_drift
    },
    %{
      name: "fine_asset",
      connection: "duckdb",
      catalogue: "sales",
      status: :healthy,
      coverage_status: :complete,
      compatibility_status: :ready
    }
  ]

  test "each scope's count is what filtering by it returns" do
    for choice <- AssetCatalogueFilters.scope_choices(@stateful, %{scope: "all"}) do
      filters = %{AssetCatalogueFilters.defaults() | scope: choice.id}
      filtered = AssetCatalogueFilters.filter(@stateful, filters)

      assert choice.count == length(filtered),
             "#{choice.id} promised #{choice.count}, filter returned #{length(filtered)}"
    end
  end

  test "the three states are independent scopes" do
    names = fn scope ->
      @stateful
      |> AssetCatalogueFilters.filter(%{AssetCatalogueFilters.defaults() | scope: scope})
      |> Enum.map(& &1.name)
    end

    assert names.("unhealthy") == ["failed_asset", "stale_asset"]
    assert names.("incomplete_coverage") == ["gappy_asset"]
    assert names.("target_attention") == ["drifted_asset"]
  end

  test "a scope narrows alongside the other filters rather than replacing them" do
    filters = %{search: "gappy", connection: "all", catalogue: "all", scope: "unhealthy"}

    assert AssetCatalogueFilters.filter(@stateful, filters) == []
  end

  test "counts are of the collection, not of an already narrowed page" do
    choices = AssetCatalogueFilters.scope_choices(@stateful, %{scope: "unhealthy"})

    assert Enum.find(choices, &(&1.id == "all")).count == 5
    assert Enum.find(choices, &(&1.id == "unhealthy")).active?
  end
end
