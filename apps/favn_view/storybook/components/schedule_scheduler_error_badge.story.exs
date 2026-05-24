defmodule FavnView.Storybook.Components.ScheduleSchedulerErrorBadge do
  alias FavnView.Components.ScheduleUi

  use PhoenixStorybook.Story, :component

  def function, do: &ScheduleUi.scheduler_error_badge/1
  def layout, do: :one_column
  def render_source, do: :function

  def template do
    """
    <div data-theme="favn-dark" class="favn-shell-bg min-h-40 p-8">
      <.psb-variation/>
    </div>
    """
  end

  def variations do
    [
      %Variation{id: :none, attributes: %{error: nil}},
      %Variation{
        id: :submit_failure,
        attributes: %{error: %{phase_label: "Submit run", message: "Window policy invalid"}}
      }
    ]
  end
end
