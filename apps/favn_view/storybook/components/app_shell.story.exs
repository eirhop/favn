defmodule FavnView.Storybook.Components.AppShell do
  alias FavnView.Components.AppShell
  alias FavnView.Components.AssetDetailPage
  alias FavnView.Components.GlassPanel
  alias FavnView.Components.ModeRail

  use PhoenixStorybook.Story, :component

  def function, do: &AppShell.app_shell/1
  def imports, do: [{GlassPanel, glass_panel: 1}, {ModeRail, mode_rail: 1}]
  def layout, do: :one_column
  def render_source, do: :function

  def container, do: {:iframe, style: "width: 100%; height: 900px; border: 0;"}

  def variations do
    [
      %Variation{
        id: :asset_shell,
        attributes: %{
          title: "customer_orders_daily",
          status: "Healthy",
          nav_items: AssetDetailPage.sample_nav_items()
        },
        slots: [
          """
          <.glass_panel class="mx-auto w-full max-w-4xl p-10">
            <h2 class="text-xl font-medium">Shell content slot</h2>
            <p class="mt-2 text-base-content/60">Page components provide the centered content.</p>
          </.glass_panel>
          """,
          """
          <:mode_rail>
            <.mode_rail>
              <:item label="Overview" icon="hero-play-circle" active>Overview</:item>
              <:item label="Docs" icon="hero-book-open">Docs</:item>
              <:item label="Settings" icon="hero-cog-6-tooth">Settings</:item>
            </.mode_rail>
          </:mode_rail>
          """
        ]
      }
    ]
  end
end
