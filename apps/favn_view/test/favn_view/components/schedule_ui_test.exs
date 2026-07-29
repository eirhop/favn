defmodule FavnView.Components.ScheduleUiTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest, only: [render_component: 2]

  alias FavnView.Components.ScheduleUi

  doctest ScheduleUi

  test "a neutral state is painted by the design system, not by DaisyUI" do
    html =
      render_component(&ScheduleUi.runtime_badge/1, state: :inactive, label: "Inactive")

    assert html =~ "favn-badge-neutral"
    refute html =~ ~r/(?<![-\w])badge-neutral/
  end

  test "an occurrence that will not run is not coloured like one that will" do
    upcoming =
      render_component(&ScheduleUi.occurrence_status_badge/1,
        status: :upcoming,
        label: "Upcoming"
      )

    blocked =
      render_component(&ScheduleUi.occurrence_status_badge/1, status: :blocked, label: "Blocked")

    assert upcoming =~ "badge-info"
    assert blocked =~ "badge-error"
  end

  test "no scheduler error renders a dash rather than an empty cell" do
    assert render_component(&ScheduleUi.scheduler_error_badge/1, error: nil) =~ "-"
  end

  test "a scheduler error shows the phase and keeps the message for hover" do
    html =
      render_component(&ScheduleUi.scheduler_error_badge/1,
        error: %{phase_label: "Submit run", message: "Window policy invalid"}
      )

    assert html =~ "Submit run"
    assert html =~ ~s(title="Window policy invalid")
  end
end
