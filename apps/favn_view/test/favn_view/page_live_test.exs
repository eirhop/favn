defmodule FavnView.PageLiveTest do
  use FavnView.ConnCase, async: true

  import Phoenix.LiveViewTest

  test "renders the Favn HUD shell placeholder", %{conn: conn} do
    {:ok, view, html} = live(conn, ~p"/")

    assert html =~ "customer_orders_daily"
    assert html =~ "Healthy"
    assert html =~ "Window timeline"
    assert has_element?(view, ~s([data-testid="window-timeline-panel"]))
    assert has_element?(view, ~s([aria-label="Primary navigation"]))
    assert has_element?(view, ~s([aria-label="View modes"]))
  end
end
