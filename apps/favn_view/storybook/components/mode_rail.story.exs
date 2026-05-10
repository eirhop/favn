defmodule FavnView.Storybook.Components.ModeRail do
  alias FavnView.Components.ModeRail

  use PhoenixStorybook.Story, :component

  def function, do: &ModeRail.mode_rail/1
  def layout, do: :one_column
  def render_source, do: :function

  def container, do: {:iframe, style: "width: 100%; height: 760px; border: 0;"}

  def template do
    """
    <div data-theme="favn-dark" class="favn-shell-bg min-h-screen">
      <.psb-variation/>
    </div>
    """
  end

  def variations do
    [
      %Variation{
        id: :asset_modes,
        attributes: %{
          active: :list,
          modes: [
            %{id: :list, label: "List", icon: "hero-list-bullet"},
            %{id: :tree, label: "Tree", icon: "hero-share", disabled: true},
            %{id: :filters, label: "Filters", icon: "hero-funnel"},
            %{id: :more, label: "More", icon: "hero-ellipsis-vertical"}
          ]
        }
      }
    ]
  end
end
