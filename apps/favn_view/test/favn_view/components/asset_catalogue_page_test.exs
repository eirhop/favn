defmodule FavnView.Components.AssetCataloguePageTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias FavnView.Components.AssetCataloguePage

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

    assert length(Regex.scan(~r/data-testid="asset-compatibility-status"/, html)) == 6
    assert html =~ "Compatible"
    assert html =~ "Rebuild available"
    assert html =~ "Not initialized"
    assert html =~ "Rebuild required"
    assert html =~ "Target drift"
    assert html =~ "Operator decision"
  end

  test "uses the blocking tone for compatibility states that reject writes" do
    for status <- [:rebuild_required, :unexpected_drift, :operator_decision] do
      html = render_component(&AssetCataloguePage.compatibility_badge/1, status: status)

      assert html =~ "badge-error"
    end
  end
end
