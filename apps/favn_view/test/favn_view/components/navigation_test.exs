defmodule FavnView.Components.NavigationTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest, only: [render_component: 2]

  alias FavnView.Components.NavRail
  alias FavnView.Components.Navigation

  test "the admin destination points to the existing admin page and marks it active" do
    assert Navigation.admin_item(true) == %{
             label: "Admin",
             icon: "hero-cog-6-tooth",
             href: "/admin",
             active: true
           }
  end

  test "the base navigation does not expose admin by default" do
    refute Enum.any?(Navigation.items(:status), &(&1.label == "Admin"))
  end

  test "the mobile menu keeps account security discoverable without crowding the header" do
    html = render_component(&NavRail.nav_menu/1, items: Navigation.items(), open: true)

    assert html =~ ~s(href="/account/security")
    assert html =~ "Account security"
  end
end
