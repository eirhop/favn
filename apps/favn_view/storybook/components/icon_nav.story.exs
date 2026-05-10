defmodule FavnView.Storybook.Components.IconNav do
  alias FavnView.Components.AssetDetailPage
  alias FavnView.Components.IconNav

  use PhoenixStorybook.Story, :component

  def function, do: &IconNav.icon_nav/1
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
        id: :desktop_primary_nav,
        attributes: %{items: AssetDetailPage.sample_nav_items()}
      }
    ]
  end
end
