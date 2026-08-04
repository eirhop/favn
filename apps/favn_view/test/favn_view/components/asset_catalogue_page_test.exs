defmodule FavnView.Components.AssetCataloguePageTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias FavnView.Components.AssetCataloguePage

  doctest AssetCataloguePage, import: true

  defp asset(attrs) do
    Map.merge(
      %{
        id: "asset",
        route_id: "asset",
        name: "asset",
        connection: "duckdb",
        catalogue: "raw",
        schema: "sales",
        type: "table",
        status: :healthy,
        coverage_status: :complete,
        compatibility_status: :ready,
        last_run_label: "6m ago"
      },
      attrs
    )
  end

  test "sorts by namespace and states it once per row" do
    assets =
      for {connection, catalogue, name} <- [
            {"postgres", "crm", "accounts"},
            {"duckdb", "raw", "stg_orders"},
            {"duckdb", "raw", "mart_daily_sales"}
          ] do
        asset(%{
          id: "#{connection}.#{catalogue}.#{name}",
          route_id: "#{connection}.#{catalogue}.#{name}",
          name: name,
          connection: connection,
          catalogue: catalogue
        })
      end

    sorted = AssetCataloguePage.sorted_assets(assets)

    assert Enum.map(sorted, & &1.name) == ["mart_daily_sales", "stg_orders", "accounts"]

    html = render_component(&AssetCataloguePage.asset_table/1, assets: sorted)

    # Namespace is one cell per row - the levels read as a single address, each a
    # separately coloured segment. The tone is the contract here, so the
    # assertion tolerates the layout classes beside it.
    assert length(Regex.scan(~r/data-testid="asset-namespace"/, html)) == 3
    assert html =~ ~r{<span class="[^"]*text-secondary[^"]*">\s*duckdb\s*</span>}
    assert html =~ ~r{<span class="[^"]*text-accent[^"]*">\s*raw\s*</span>}
    assert html =~ ~r{<span class="[^"]*text-secondary[^"]*">\s*postgres\s*</span>}
    assert html =~ ~r{<span class="[^"]*text-accent[^"]*">\s*crm\s*</span>}
  end

  test "shows every relation level the asset declares, schema included" do
    html =
      render_component(&AssetCataloguePage.asset_namespace/1,
        asset: asset(%{connection: "warehouse", catalogue: "source", schema: "crm"})
      )

    assert html =~ "warehouse"
    assert html =~ "source"
    assert html =~ "crm"
    # The whole address is available on hover, since any level may truncate.
    assert html =~ ~s(title="warehouse.source.crm")
    refute html =~ "data-testid=\"asset-namespace-defect\""
  end

  test "omits a level the asset does not declare rather than inventing one" do
    html =
      render_component(&AssetCataloguePage.asset_namespace/1,
        asset: asset(%{connection: "warehouse", catalogue: nil, schema: "core"})
      )

    assert html =~ ~s(title="warehouse.core")
    refute html =~ "uncatalogued"
    refute html =~ "unknown"
  end

  test "an asset with no relation shows its module path, marked as inferred" do
    html =
      render_component(&AssetCataloguePage.asset_namespace/1,
        asset:
          asset(%{
            connection: nil,
            catalogue: nil,
            schema: nil,
            module_path: ["landing", "crm", "daily"]
          })
      )

    assert html =~ ~s(data-namespace-source="module")
    assert html =~ "landing"
    assert html =~ "crm"
    assert html =~ "daily"
    # The reader has to be able to tell an inferred address from a physical one.
    assert html =~ "hero-code-bracket-square"
    assert html =~ "Derived from the module path"
    refute html =~ "hero-circle-stack"
  end

  test "a relation carrying only a name still falls back to the module path" do
    # `Favn.RelationRef` only enforces `:name`, so a relation with no namespace
    # levels is legal. Keying the fallback off the relation being absent skipped
    # it here and claimed the asset owned no relation at all.
    html =
      render_component(&AssetCataloguePage.asset_namespace/1,
        asset:
          asset(%{
            connection: nil,
            catalogue: nil,
            schema: nil,
            module_path: ["warehouse", "source"]
          })
      )

    assert html =~ ~s(data-namespace-source="module")
    assert html =~ ~s(title="Derived from the module path)
    assert html =~ "warehouse"
    assert html =~ "source"
  end

  test "a relation address is never marked as inferred" do
    html = render_component(&AssetCataloguePage.asset_namespace/1, asset: asset(%{}))

    assert html =~ ~s(data-namespace-source="relation")
    assert html =~ "hero-circle-stack"
    refute html =~ "Derived from the module path"
  end

  test "an asset with neither a relation nor module segments says so" do
    html =
      render_component(&AssetCataloguePage.asset_namespace/1,
        asset: asset(%{connection: nil, catalogue: nil, schema: nil, module_path: []})
      )

    assert html =~ "No namespace"
    refute html =~ "unknown"
    refute html =~ "uncatalogued"
  end

  test "the namespace glyph is suppressed where the row already shows one" do
    # The mobile card renders the connection glyph in its own tile, so a second
    # one beside the address is duplication — and for an inferred address the two
    # disagreed about whether the asset lives in a data system.
    html =
      render_component(&AssetCataloguePage.asset_namespace/1, asset: asset(%{}), icon: false)

    assert html =~ "duckdb"
    refute html =~ "hero-circle-stack"
  end

  test "a module-derived namespace is never flagged as a misconfigured relation" do
    refute AssetCataloguePage.namespace_defect(%{
             connection: nil,
             catalogue: nil,
             schema: nil,
             module_path: ["lifecycle", "elastic_scale_probe"]
           })
  end

  test "flags a catalogue without a schema, which cannot resolve" do
    html =
      render_component(&AssetCataloguePage.asset_namespace/1,
        asset: asset(%{connection: "warehouse", catalogue: "source", schema: nil})
      )

    assert html =~ ~s(data-testid="asset-namespace-defect")
    assert html =~ "needs a schema"
  end

  test "renders compatibility independently from health and coverage" do
    assets =
      [
        :ready,
        :rebuild_available,
        :uninitialized,
        :rebuild_required,
        :unexpected_drift,
        :operator_decision
      ]
      |> Enum.with_index()
      |> Enum.map(fn {compatibility_status, index} ->
        asset(%{
          id: "asset-#{index}",
          route_id: "asset-#{index}",
          name: "asset-#{index}",
          compatibility_status: compatibility_status
        })
      end)

    html = render_component(&AssetCataloguePage.asset_table/1, assets: assets)

    # Each row renders all three states, and compatibility keeps its own wording
    # per state. The states are glyphs now rather than words, so this asserts on
    # the accessible name — which is what a reader who cannot use the colour has.
    assert length(Regex.scan(~r/data-testid="asset-states"/, html)) == 6
    assert html =~ "Target compatible with the contract"
    assert html =~ "A rebuild is available"
    assert html =~ "Target not initialised yet"
    assert html =~ "Rebuild required before writes"
    assert html =~ "Target drift"
    assert html =~ "Waiting on an operator decision"
  end

  test "every state is a distinct glyph, so colour is never the only signal" do
    glyphs =
      for status <- [:healthy, :failed, :stale, :running] do
        html =
          render_component(&AssetCataloguePage.asset_state_icons/1, asset: %{status: status})

        [_all, glyph] = Regex.run(~r/hero-([a-z0-9-]+)/, html)
        glyph
      end

    assert glyphs == Enum.uniq(glyphs)
  end

  test "uses the blocking tone for compatibility states that reject writes" do
    for status <- [:rebuild_required, :unexpected_drift, :operator_decision] do
      html = render_component(&AssetCataloguePage.compatibility_badge/1, status: status)

      assert html =~ "badge-error"
    end
  end
end
