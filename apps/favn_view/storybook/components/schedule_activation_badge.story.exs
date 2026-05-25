defmodule FavnView.Storybook.Components.ScheduleActivationBadge do
  alias FavnView.Components.ScheduleUi

  use PhoenixStorybook.Story, :component

  def function, do: &ScheduleUi.activation_badge/1
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
      %Variation{
        id: :pending_activation,
        attributes: %{state: :pending_activation, label: "Pending activation"}
      },
      %Variation{id: :enabled, attributes: %{state: :enabled, label: "Enabled"}},
      %Variation{id: :disabled, attributes: %{state: :disabled, label: "Disabled"}},
      %Variation{id: :needs_review, attributes: %{state: :needs_review, label: "Needs review"}},
      %Variation{id: :retired, attributes: %{state: :retired, label: "Retired"}}
    ]
  end
end
