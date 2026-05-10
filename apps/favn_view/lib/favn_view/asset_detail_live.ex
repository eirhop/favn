defmodule FavnView.AssetDetailLive do
  @moduledoc false

  use FavnView, :live_view

  alias FavnView.Components.AssetCataloguePage
  alias FavnView.Components.AppShell
  alias FavnView.Components.GlassPanel

  @impl true
  def mount(%{"asset_id" => asset_id}, _session, socket) do
    {:ok, assign(socket, asset_id: asset_id, nav_items: AssetCataloguePage.nav_items())}
  end

  @impl true
  def render(assigns) do
    ~H"""
    <AppShell.app_shell title={@asset_id} subtitle="Asset detail" nav_items={@nav_items}>
      <div class="mx-auto w-full max-w-4xl">
        <GlassPanel.glass_panel class="p-8 text-center" data-testid="asset-detail-placeholder">
          <h2 class="text-xl font-medium">Asset detail coming soon</h2>
          <p class="mt-2 text-base-content/60">
            The catalogue can open assets now; detailed runs, lineage, docs, and code will follow.
          </p>
          <.link navigate={~p"/assets"} class="btn btn-primary btn-soft mt-6">
            Back to catalogue
          </.link>
        </GlassPanel.glass_panel>
      </div>
    </AppShell.app_shell>
    """
  end
end
