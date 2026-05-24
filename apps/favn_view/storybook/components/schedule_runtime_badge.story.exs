defmodule FavnView.Storybook.Components.ScheduleRuntimeBadge do
  alias FavnView.Components.ScheduleUi

  use PhoenixStorybook.Story, :component

  def function, do: &ScheduleUi.runtime_badge/1
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
      %Variation{id: :inactive, attributes: %{state: :inactive, label: "Inactive"}},
      %Variation{id: :idle, attributes: %{state: :idle, label: "Idle"}},
      %Variation{id: :running, attributes: %{state: :running, label: "Running"}},
      %Variation{id: :queued, attributes: %{state: :queued, label: "Queued"}}
    ]
  end
end
