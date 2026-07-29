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
end
