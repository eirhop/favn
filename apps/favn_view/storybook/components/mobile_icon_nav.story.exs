defmodule FavnView.Storybook.Components.MobileIconNav do
  alias FavnView.Components.AssetDetailPage
  alias FavnView.Components.IconNav

  use PhoenixStorybook.Story, :component

  def function, do: &IconNav.mobile_icon_nav/1
  def render_source, do: :function

  def container, do: {:iframe, style: "width: 390px; height: 640px; border: 0;"}

  def template do
    """
    <div data-theme="favn-dark" class="favn-shell-bg min-h-screen p-5">
      <.psb-variation/>
    </div>
    """
  end

  def variations do
    [
      %Variation{
        id: :closed,
        attributes: %{items: AssetDetailPage.sample_nav_items()}
      },
      %Variation{
        id: :open,
        attributes: %{items: AssetDetailPage.sample_nav_items(), open: true}
      }
    ]
  end
end
