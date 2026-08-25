defmodule FavnView.UIFieldTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest, only: [render_component: 2]

  alias FavnView.UI.Field

  test "checkbox help is available on hover and keyboard focus" do
    html =
      render_component(&Field.input/1,
        id: "combine-windows",
        type: "checkbox",
        name: "combine_windows",
        label: "Combine windows",
        checked: false,
        tooltip: "Use one child run for every selected window."
      )

    assert html =~ ~s(id="combine-windows-tooltip" role="tooltip")
    assert html =~ ~s(type="button")
    assert html =~ ~s(aria-label="More information about Combine windows")
    assert html =~ ~s(aria-describedby="combine-windows-tooltip")
    assert html =~ "Use one child run for every selected window."
  end
end
