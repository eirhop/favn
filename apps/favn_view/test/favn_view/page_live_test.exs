defmodule FavnView.PageLiveTest do
  use FavnView.ConnCase, async: true

  import Phoenix.LiveViewTest

  test "renders the placeholder shell", %{conn: conn} do
    {:ok, _view, html} = live(conn, ~p"/")

    assert html =~ "Favn View"
    assert html =~ "Phoenix and LiveView shell"
  end
end
