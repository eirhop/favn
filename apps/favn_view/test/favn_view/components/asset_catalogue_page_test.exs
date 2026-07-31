defmodule FavnView.Components.AssetCataloguePageTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias FavnView.Components.AssetCataloguePage

  doctest AssetCataloguePage, import: true

  test "sorts by namespace and states it once per row" do
    assets =
      for {connection, catalogue, name} <- [
            {"postgres", "crm", "accounts"},
            {"duckdb", "sales", "stg_orders"},
            {"duckdb", "sales", "mart_daily_sales"}
          ] do
        %{
          id: "#{connection}.#{catalogue}.#{name}",
          route_id: "#{connection}.#{catalogue}.#{name}",
          name: name,
          connection: connection,
          catalogue: catalogue,
          type: "table",
          status: :healthy,
          coverage_status: :complete,
          compatibility_status: :ready,
          last_run_label: "6m ago"
        }
      end

    sorted = AssetCataloguePage.sorted_assets(assets)

    assert Enum.map(sorted, & &1.name) == ["mart_daily_sales", "stg_orders", "accounts"]

    html = render_component(&AssetCataloguePage.asset_table/1, assets: sorted)

    # Namespace is one cell per row - the pair reads as a single address, with
    # connection and catalogue as separately coloured segments. The tone is the
    # contract here, so the assertion tolerates the layout classes beside it.
    assert length(Regex.scan(~r/data-testid="asset-namespace"/, html)) == 3
    assert html =~ ~r{<span class="[^"]*text-secondary[^"]*">\s*duckdb\s*</span>}
    assert html =~ ~r{<span class="[^"]*text-accent[^"]*">\s*sales\s*</span>}
    assert html =~ ~r{<span class="[^"]*text-secondary[^"]*">\s*postgres\s*</span>}
    assert html =~ ~r{<span class="[^"]*text-accent[^"]*">\s*crm\s*</span>}
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
        %{
          id: "asset-#{index}",
          name: "asset-#{index}",
          connection: "postgres",
          catalogue: "sales",
          type: "table",
          status: :healthy,
          coverage_status: :complete,
          compatibility_status: compatibility_status,
          last_run_label: "6m ago"
        }
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
