defmodule FavnView.AssetCatalogueFiltersTest do
  use ExUnit.Case, async: true

  alias FavnView.AssetCatalogueFilters

  doctest AssetCatalogueFilters

  @assets [
    %{name: "stg_orders", connection: "duckdb", catalogue: "raw", schema: "sales"},
    %{name: "mart_daily_sales", connection: "duckdb", catalogue: "mart", schema: "sales"},
    %{name: "raw_events", connection: "duckdb", catalogue: "raw", schema: "platform"},
    %{name: "accounts", connection: "postgres", catalogue: "core", schema: "crm"},
    # What the catalogue projects for an asset owning no relation: nil levels and
    # a module path. The path is deliberately not folded into the levels — it
    # addresses nothing, so it must not become an option an operator can pick.
    %{
      name: "landed_events",
      connection: nil,
      catalogue: nil,
      schema: nil,
      module_path: ["lifecycle", "probes"]
    }
  ]

  defp filters(overrides), do: Map.merge(AssetCatalogueFilters.defaults(), overrides)

  test "choosing a connection narrows the catalogue options to its catalogues" do
    assert AssetCatalogueFilters.catalogue_options(@assets, %{connection: "duckdb"}) == [
             {"Catalogue", "all"},
             {"Mart", "mart"},
             {"Raw", "raw"}
           ]

    assert AssetCatalogueFilters.catalogue_options(@assets, %{connection: "postgres"}) == [
             {"Catalogue", "all"},
             {"Core", "core"}
           ]
  end

  test "schema options narrow by connection and catalogue together" do
    assert AssetCatalogueFilters.schema_options(@assets, %{
             connection: "duckdb",
             catalogue: "raw"
           }) == [{"Schema", "all"}, {"Platform", "platform"}, {"Sales", "sales"}]

    assert AssetCatalogueFilters.schema_options(@assets, %{
             connection: "duckdb",
             catalogue: "mart"
           }) == [{"Schema", "all"}, {"Sales", "sales"}]
  end

  test "an asset that owns no relation contributes no namespace options" do
    # Its module path segments must not surface as connections, which is what
    # happened while the path stood in for the levels: "Lifecycle" appeared in the
    # Connection select beside real connections and filtered to inferred rows.
    assert AssetCatalogueFilters.connection_options(@assets) == [
             {"Connection", "all"},
             {"Duckdb", "duckdb"},
             {"Postgres", "postgres"}
           ]

    refute AssetCatalogueFilters.schema_options(@assets, %{})
           |> Enum.any?(&(elem(&1, 1) == "probes"))
  end

  test "switching connection resets a catalogue that no longer exists" do
    reconciled =
      AssetCatalogueFilters.reconcile_namespace(
        filters(%{connection: "postgres", catalogue: "raw"}),
        @assets
      )

    assert reconciled.catalogue == "all"
  end

  test "a still-valid catalogue survives a connection switch" do
    given = filters(%{connection: "duckdb", catalogue: "raw"})

    assert AssetCatalogueFilters.reconcile_namespace(given, @assets) == given
  end

  test "resetting a catalogue does not drag down a schema the connection still allows" do
    reconciled =
      AssetCatalogueFilters.reconcile_namespace(
        filters(%{connection: "duckdb", catalogue: "core", schema: "sales"}),
        @assets
      )

    assert reconciled.catalogue == "all"
    assert reconciled.schema == "sales"
  end

  test "a schema outside the chosen catalogue resets" do
    reconciled =
      AssetCatalogueFilters.reconcile_namespace(
        filters(%{connection: "duckdb", catalogue: "mart", schema: "platform"}),
        @assets
      )

    assert reconciled.catalogue == "mart"
    assert reconciled.schema == "all"
  end

  test "search and all three namespace levels narrow together" do
    given = filters(%{search: "stg", connection: "duckdb", catalogue: "raw", schema: "sales"})

    assert AssetCatalogueFilters.filter(@assets, given) |> Enum.map(& &1.name) ==
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
    given = filters(%{search: "gappy", scope: "unhealthy"})

    assert AssetCatalogueFilters.filter(@stateful, given) == []
  end

  test "counts are of the collection, not of an already narrowed page" do
    choices = AssetCatalogueFilters.scope_choices(@stateful, %{scope: "unhealthy"})

    assert Enum.find(choices, &(&1.id == "all")).count == 5
    assert Enum.find(choices, &(&1.id == "unhealthy")).active?
  end
end
