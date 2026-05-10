defmodule FavnView.Storybook.Components.AssetDetailPage do
  alias FavnView.Components.AssetDetailPage

  use PhoenixStorybook.Story, :component

  def function, do: &AssetDetailPage.asset_detail_page/1
  def layout, do: :one_column
  def render_source, do: :function

  def container, do: {:iframe, style: "width: 100%; height: 900px; border: 0;"}

  def variations do
    [
      %Variation{
        id: :healthy_asset,
        attributes: %{
          title: "customer_orders_daily",
          status: "Healthy",
          window_range: "May 24 - Jun 22, 2026",
          nav_items: AssetDetailPage.sample_nav_items(),
          timeline: AssetDetailPage.sample_timeline()
        }
      }
    ]
  end
end
