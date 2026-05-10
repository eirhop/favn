defmodule FavnView.PageLiveTest do
  use FavnView.ConnCase, async: false

  import Phoenix.LiveViewTest

  alias Favn.Manifest
  alias Favn.Manifest.Version
  alias Favn.Run.AssetResult
  alias Favn.Window.Spec, as: WindowSpec
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
    seed_run_events!("run_customer_orders_daily")

    :ok
  end

  test "redirects home to the asset catalogue", %{conn: conn} do
    assert {:error, {:redirect, %{to: "/assets"}}} = live(conn, ~p"/")
  end

  test "renders the asset catalogue", %{conn: conn} do
    {:ok, view, html} = live(conn, ~p"/assets")

    assert html =~ "Asset catalogue"
    assert html =~ "Browse and monitor all assets"
    assert has_element?(view, ~s([data-testid="asset-table"]))
    assert has_element?(view, ~s([data-testid="asset-card-list"]))

    assert has_element?(view, "a", "customer_orders_daily")

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

    assert has_element?(view, "a", "raw_payments")
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

  test "renders the selected asset detail page", %{conn: conn} do
    {:ok, view, html} = live(conn, detail_path(:customer_orders_daily))

    assert html =~ "customer_orders_daily"
    assert html =~ "Healthy"
    assert html =~ "Window timeline"
    assert has_element?(view, ~s([data-testid="window-timeline-panel"]))
    assert has_element?(view, ~s([aria-label="View modes"]))
  end

  test "asset detail defaults to timeline mode", %{conn: conn} do
    {:ok, view, _html} = live(conn, detail_path(:customer_orders_daily))

    assert has_element?(view, ~s([data-testid="window-timeline-panel"]), "Window timeline")
    assert has_element?(view, ~s([data-testid="selected-window-actions"]), today_label())
    assert has_element?(view, "[data-testid='run-selected-window']:not([disabled])")
    refute has_element?(view, ~s([data-testid="create-backfill"]))
    refute has_element?(view, ~s([data-testid="asset-mode-placeholder"]))
  end

  test "run selected window submits and navigates to run detail", %{conn: conn} do
    {:ok, view, _html} = live(conn, detail_path(:customer_orders_daily))

    view
    |> element(~s([data-testid="run-selected-window"]), "Run this window")
    |> render_click()

    assert {run_path, %{"info" => "Run submitted"}} = assert_redirect(view)
    assert String.starts_with?(run_path, "/runs/run_")

    {:ok, run_view, html} = live(conn, run_path)

    assert html =~ "Run run_"
    assert has_element?(run_view, ~s([data-testid="run-overview-panel"]))
  end

  test "non-runnable selected window keeps run disabled", %{conn: conn} do
    {:ok, view, _html} = live(conn, detail_path(:stg_payments))

    assert has_element?(view, ~s([data-testid="run-selected-window"][disabled]))
    refute has_element?(view, ~s([data-testid="create-backfill"]))
    assert has_element?(view, ~s([data-testid="selected-window-actions"]), "No window policy")
  end

  test "asset detail mode rail changes the central panel", %{conn: conn} do
    {:ok, view, _html} = live(conn, detail_path(:customer_orders_daily))

    view
    |> element(~s([data-testid="view-mode-rail"] button[aria-label="Runs"]))
    |> render_click()

    assert has_element?(view, ~s([data-testid="asset-mode-placeholder"]), "Runs coming soon")
    refute has_element?(view, ~s([data-testid="window-timeline-panel"]))
    assert has_element?(view, ~s(button[aria-label="Runs"][aria-pressed="true"]))
  end

  test "clicking a timeline window changes the selected window", %{conn: conn} do
    {:ok, view, _html} = live(conn, detail_path(:customer_orders_daily))
    window_id = "window:day:#{Date.utc_today() |> Date.add(-29) |> Date.to_iso8601()}"
    window_label = Date.utc_today() |> Date.add(-29) |> Calendar.strftime("%b %-d, %Y")

    view
    |> element(~s([data-testid="timeline-window-#{window_id}"]))
    |> render_click()

    assert has_element?(view, ~s([data-testid="selected-window-actions"]), window_label)

    assert has_element?(
             view,
             ~s([data-testid="timeline-window-#{window_id}"][aria-pressed="true"])
           )

    assert has_element?(
             view,
             ~s([data-testid="timeline-window-#{window_id}"][aria-label*="pending"])
           )
  end

  test "asset detail ignores invalid mode and window events", %{conn: conn} do
    {:ok, view, _html} = live(conn, detail_path(:customer_orders_daily))

    assert render_click(view, "set_mode", %{"mode" => "not_real"}) =~ "Window timeline"
    assert has_element?(view, ~s([data-testid="selected-window-actions"]), today_label())

    assert render_click(view, "select_window", %{"window-id" => "not-real"}) =~ today_label()
    assert has_element?(view, ~s([data-testid="window-timeline-panel"]))
  end

  test "shows not-found state for unknown asset ids", %{conn: conn} do
    {:ok, view, html} = live(conn, ~p"/assets/not_real")

    assert html =~ "Asset not found"
    assert has_element?(view, ~s([data-testid="asset-not-found-state"]))
  end

  test "renders existing run detail overview", %{conn: conn} do
    {:ok, view, html} = live(conn, ~p"/runs/run_customer_orders_daily")

    assert html =~ "Run run_customer_order"
    assert has_element?(view, ~s([data-testid="run-overview-panel"]))
    assert has_element?(view, ~s([data-testid="run-summary-row"]), "Succeeded")
    assert has_element?(view, ~s([data-testid="run-summary-row"]), "mv_view_assets")
  end

  test "run detail renders events when present", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/runs/run_customer_orders_daily")

    assert has_element?(view, ~s([data-testid="run-event-timeline"]), "Run started")
    assert has_element?(view, ~s([data-testid="run-event-timeline"]), "Run finished")
  end

  test "run detail renders asset results when present", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/runs/run_customer_orders_daily")

    assert has_element?(view, ~s([data-testid="run-asset-results"]), "customer_orders_daily")
    assert has_element?(view, ~s([data-testid="run-asset-results"]), "Stage 0")
  end

  test "run detail mode rail changes to placeholder modes", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/runs/run_customer_orders_daily")

    view
    |> element(~s([data-testid="view-mode-rail"] button[aria-label="Events"]))
    |> render_click()

    assert has_element?(view, ~s([data-testid="run-mode-placeholder"]), "Events")
    refute has_element?(view, ~s([data-testid="run-overview-panel"]))
  end

  test "run detail ignores invalid mode events", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/runs/run_customer_orders_daily")

    assert render_click(view, "set_mode", %{"mode" => "not_real"}) =~ "Asset execution"
    assert has_element?(view, ~s([data-testid="run-overview-panel"]))
  end

  test "shows not-found state for unknown run ids", %{conn: conn} do
    {:ok, view, html} = live(conn, ~p"/runs/run_missing")

    assert html =~ "Run not found"
    assert has_element?(view, ~s([data-testid="run-not-found-state"]), "run_missing")
  end

  test "catalogue links open the detail route", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/assets")
    detail_path = detail_path(:customer_orders_daily)

    {:ok, detail_view, html} =
      view
      |> element(
        ~s(tr[data-testid="asset-row"] a[href="#{detail_path}"]),
        "customer_orders_daily"
      )
      |> render_click()
      |> follow_redirect(conn, detail_path)

    assert html =~ "customer_orders_daily"
    assert has_element?(detail_view, ~s([data-testid="window-timeline-panel"]))
  end

  test "ignores invalid mode events", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/assets")

    assert render_click(view, "set_mode", %{"mode" => "not_real"}) =~ "Asset catalogue"
  end

  defp manifest_version do
    manifest = %Manifest{
      assets: [
        asset(:customer_orders_daily, :snowflake, "sales", :sql, WindowSpec.new!(:day)),
        asset(:raw_payments, :s3, "finance", :source),
        asset(:stg_payments, :postgres, "finance", :sql)
      ]
    }

    {:ok, version} = Version.new(manifest, manifest_version_id: "mv_view_assets")
    version
  end

  defp asset(name, connection, catalog, type, window \\ nil) do
    %Favn.Manifest.Asset{
      ref: {__MODULE__.Assets, name},
      module: __MODULE__.Assets,
      name: name,
      type: type,
      window: window,
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

  defp seed_run_events!(run_id) do
    now = DateTime.utc_now()

    assert :ok =
             Storage.append_run_event(run_id, %{
               run_id: run_id,
               sequence: 1,
               event_type: :run_started,
               occurred_at: DateTime.add(now, -1, :second),
               status: :running,
               data: %{message: "Run started"}
             })

    assert :ok =
             Storage.append_run_event(run_id, %{
               run_id: run_id,
               sequence: 2,
               event_type: :run_finished,
               occurred_at: now,
               status: :ok,
               data: %{message: "Run finished"}
             })
  end

  defp target_id(name) do
    {:ok, entries} = FavnOrchestrator.active_asset_catalogue()

    entries
    |> Enum.find(&(get_in(&1, [:relation, :name]) == Atom.to_string(name)))
    |> Map.fetch!(:target_id)
  end

  defp detail_path(name) do
    ~p"/assets/#{FavnView.AssetRoute.to_param(target_id(name))}"
  end

  defp today_label do
    Date.utc_today() |> Calendar.strftime("%b %-d, %Y")
  end
end
