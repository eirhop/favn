defmodule FavnView.AssetCatalogueLineageTest do
  use FavnView.ConnCase, async: false

  import Phoenix.LiveViewTest

  alias FavnOrchestrator.Auth
  alias FavnOrchestrator.Auth.Store, as: AuthStore
  alias FavnView.Auth.BrowserSessionStore
  alias FavnView.Components.LineagePage

  setup %{conn: conn} do
    ensure_auth_store_started()
    :ok = AuthStore.reset()
    :ok = BrowserSessionStore.reset()

    graph = LineagePage.sample_graph()
    inspector = LineagePage.sample_group_inspector()

    put_view_env(:lineage_get_graph_fun, fn _opts -> {:ok, graph} end)
    put_view_env(:lineage_get_group_fun, fn _id, _opts -> {:ok, inspector} end)
    put_view_env(:active_asset_catalogue_fun, fn -> {:ok, asset_catalogue_entries()} end)

    {:ok, conn: authenticate_conn(conn), graph: graph}
  end

  test "renders lineage as an asset catalogue mode without top KPI cards", %{conn: conn} do
    {:ok, view, html} = live(conn, ~p"/assets?mode=lineage")

    assert html =~ "Asset catalogue"
    assert html =~ "Search assets, groups, or schemas"
    assert has_element?(view, ~s([data-testid="lineage-toolbar"]))
    assert has_element?(view, ~s([data-testid="lineage-canvas"]))
    assert has_element?(view, ~s([data-testid="lineage-inspector"]))
    assert has_element?(view, ~s([data-testid="lineage-minimap"]))
    assert has_element?(view, ~s([data-testid="lineage-group-node"]), "GitHub raw")
    assert has_element?(view, ~s([data-testid="lineage-edge-label"]), "18 deps")

    assert has_element?(
             view,
             ~s([data-testid="view-mode-rail"] button[aria-label="Lineage"][aria-pressed="true"])
           )

    refute html =~ "lineage-kpi-card"
  end

  test "selecting a group loads the inspector through the lineage facade", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/assets?mode=lineage")

    view
    |> element(~s([data-testid="lineage-group-node"][phx-value-id="group:raw:github"]))
    |> render_click()

    assert has_element?(view, ~s([data-testid="lineage-inspector"]), "About this group")
    assert render(view) =~ "Health summary"
  end

  test "lineage inspector can be closed to expand graph workspace", %{conn: conn} do
    {:ok, view, _html} = live(conn, ~p"/assets?mode=lineage")

    assert has_element?(view, ~s([data-testid="lineage-inspector"]))

    view
    |> element(~s([data-testid="lineage-inspector-close"]))
    |> render_click()

    refute has_element?(view, ~s([data-testid="lineage-inspector"]))
    assert has_element?(view, ~s([data-testid="lineage-canvas"]))
  end

  test "renders sanitized lineage backend errors", %{conn: conn} do
    put_view_env(:lineage_get_graph_fun, fn _opts ->
      {:error,
       %{message: "No active manifest is available.", details: %{secret: "do-not-render"}}}
    end)

    {:ok, view, html} = live(conn, ~p"/assets?mode=lineage")

    assert has_element?(
             view,
             ~s([data-testid="lineage-error-state"]),
             "No active manifest is available."
           )

    refute html =~ "do-not-render"
  end

  defp authenticate_conn(conn) do
    username = "operator-#{System.unique_integer([:positive])}"

    assert {:ok, _actor} =
             Auth.create_actor(username, "operator-password-long", "Test Operator", [:operator])

    conn
    |> post(~p"/login", %{
      "operator" => %{"username" => username, "password" => "operator-password-long"}
    })
    |> recycle()
  end

  defp ensure_auth_store_started do
    case Process.whereis(AuthStore) do
      nil -> start_supervised!({AuthStore, []})
      _pid -> :ok
    end
  end

  defp put_view_env(key, value) do
    original = Application.get_env(:favn_view, key, :__missing__)
    Application.put_env(:favn_view, key, value)

    on_exit(fn ->
      case original do
        :__missing__ -> Application.delete_env(:favn_view, key)
        value -> Application.put_env(:favn_view, key, value)
      end
    end)
  end

  defp asset_catalogue_entries do
    [
      %{
        target_id: "asset:test:customer_orders_daily",
        relation: %{name: "customer_orders_daily", connection: "snowflake", catalog: "sales"},
        type: "table",
        status: :healthy,
        latest_run_at: DateTime.utc_now()
      }
    ]
  end
end
