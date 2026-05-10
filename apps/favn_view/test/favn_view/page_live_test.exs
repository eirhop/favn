defmodule FavnView.PageLiveTest do
  use FavnView.ConnCase, async: false

  import Phoenix.LiveViewTest

  alias Favn.Manifest
  alias Favn.Manifest.Version
  alias FavnOrchestrator.Storage.Adapter.Memory

  setup do
    Memory.reset()

    version = manifest_version()
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)

    :ok
  end

  test "renders the Favn HUD shell placeholder", %{conn: conn} do
    {:ok, view, html} = live(conn, ~p"/")

    assert html =~ "customer_orders_daily"
    assert html =~ "Healthy"
    assert html =~ "Window timeline"
    assert has_element?(view, ~s([data-testid="window-timeline-panel"]))
    assert has_element?(view, ~s([aria-label="Primary navigation"]))
    assert has_element?(view, ~s([aria-label="View modes"]))
  end

  test "renders the asset catalogue", %{conn: conn} do
    {:ok, view, html} = live(conn, ~p"/assets")

    assert html =~ "Asset catalogue"
    assert html =~ "Browse and monitor all assets"
    assert has_element?(view, ~s([data-testid="asset-table"]))
    assert has_element?(view, ~s([data-testid="asset-card-list"]))

    assert has_element?(
             view,
             ~s(a[href*="customer_orders_daily"]),
             "customer_orders_daily"
           )

    assert has_element?(view, ~s([aria-label="View modes"]))
    assert has_element?(view, ~s([aria-label="Connection filter"]))
    assert has_element?(view, ~s([aria-label="Catalogue filter"]))
  end

  test "filters assets by search, connection, and catalogue", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/assets")

    view
    |> element("form")
    |> render_change(%{
      "filters" => %{"search" => "payments", "connection" => "s3", "catalogue" => "finance"}
    })

    assert has_element?(view, ~s(a[href*="raw_payments"]), "raw_payments")
    refute has_element?(view, ~s(a[href*="stg_payments"]), "stg_payments")
  end

  test "shows empty state when filters do not match", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/assets")

    view
    |> element("form")
    |> render_change(%{
      "filters" => %{"search" => "not_real", "connection" => "all", "catalogue" => "all"}
    })

    assert has_element?(view, ~s([data-testid="asset-empty-state"]), "No assets found")
  end

  test "opens the asset detail placeholder", %{conn: conn} do
    {:ok, _view, html} = live(conn, ~p"/assets/customer_orders_daily")

    assert html =~ "customer_orders_daily"
    assert html =~ "Asset detail coming soon"
  end

  defp manifest_version do
    manifest = %Manifest{
      assets: [
        asset(:customer_orders_daily, :snowflake, "sales", :sql),
        asset(:raw_payments, :s3, "finance", :source),
        asset(:stg_payments, :postgres, "finance", :sql)
      ]
    }

    {:ok, version} = Version.new(manifest, manifest_version_id: "mv_view_assets")
    version
  end

  defp asset(name, connection, catalog, type) do
    %Favn.Manifest.Asset{
      ref: {__MODULE__.Assets, name},
      module: __MODULE__.Assets,
      name: name,
      type: type,
      relation: %{connection: connection, catalog: catalog, name: Atom.to_string(name)}
    }
  end
end
