defmodule FavnView.PageLiveTest do
  use FavnView.ConnCase, async: false

  import Phoenix.LiveViewTest

  alias Favn.Manifest
  alias Favn.Manifest.Version
  alias Favn.Run.AssetResult
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.Storage.Adapter.Memory

  setup do
    Memory.reset()

    version = manifest_version()
    assert :ok = FavnOrchestrator.register_manifest(version)
    assert :ok = FavnOrchestrator.activate_manifest(version.manifest_version_id)
    assert :ok = Storage.put_run(run_state(:customer_orders_daily, :ok, -600))
    assert :ok = Storage.put_run(run_state(:raw_payments, :running, -30))

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

    assert html =~ "Healthy"
    assert html =~ "10m ago"
    assert html =~ "Running"

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

  test "ignores invalid mode events", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/assets")

    assert render_click(view, "set_mode", %{"mode" => "not_real"}) =~ "Asset catalogue"
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

  defp run_state(name, status, seconds_from_now) do
    ref = {__MODULE__.Assets, name}
    finished_at = DateTime.add(DateTime.utc_now(), seconds_from_now, :second)
    started_at = DateTime.add(finished_at, -1, :second)

    RunState.new(
      id: "run_#{name}",
      manifest_version_id: "mv_view_assets",
      manifest_content_hash: "hash_view_assets",
      asset_ref: ref,
      target_refs: [ref]
    )
    |> RunState.transition(
      status: status,
      result: %{
        asset_results: [
          %AssetResult{
            ref: ref,
            stage: 0,
            status: status,
            started_at: started_at,
            finished_at: if(status in [:pending, :running], do: nil, else: finished_at),
            duration_ms: 1
          }
        ]
      }
    )
    |> Map.put(:inserted_at, started_at)
    |> Map.put(:updated_at, finished_at)
    |> RunState.with_snapshot_hash()
  end
end
