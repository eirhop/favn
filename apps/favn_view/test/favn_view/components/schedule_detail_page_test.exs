defmodule FavnView.Components.ScheduleDetailPageTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest, only: [render_component: 2]

  alias FavnView.Components.ScheduleDetailPage

  doctest ScheduleDetailPage, import: true

  test "an enabled schedule offers to disable, and confirms first" do
    html = render_actions(:enabled)

    assert html =~ ~s(phx-value-action="disable")
    assert html =~ "data-confirm"
    refute html =~ ~s(phx-value-action="enable")
  end

  test "a disabled schedule offers to enable without a confirmation" do
    html = render_actions(:disabled)

    assert html =~ ~s(phx-value-action="enable")
    refute html =~ "data-confirm"
  end

  test "the activation control cannot be double-submitted" do
    assert render_actions(:disabled) =~ "phx-disable-with"
  end

  test "a definition changed after approval says so before enabling" do
    html = render_actions(:needs_review)

    assert html =~ "Approve and enable"
    assert html =~ "definition changed"
  end

  defp render_actions(activation_state) do
    render_component(&ScheduleDetailPage.schedule_actions/1,
      schedule: %{id: "pipeline:Demo:default", activation_state: activation_state}
    )
  end
end
