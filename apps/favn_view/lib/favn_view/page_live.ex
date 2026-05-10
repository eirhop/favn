defmodule FavnView.PageLive do
  @moduledoc false

  use FavnView, :live_view

  alias FavnView.Components.AssetDetailPage

  @impl true
  def mount(_params, _session, socket) do
    socket =
      assign(socket,
        title: "customer_orders_daily",
        status: "Healthy",
        window_range: "May 24 - Jun 22, 2026",
        nav_items: AssetDetailPage.sample_nav_items(),
        timeline: AssetDetailPage.sample_timeline()
      )

    {:ok, socket}
  end

  @impl true
  def render(assigns) do
    ~H"""
    <AssetDetailPage.asset_detail_page
      title={@title}
      status={@status}
      window_range={@window_range}
      nav_items={@nav_items}
      timeline={@timeline}
    />
    """
  end
end
