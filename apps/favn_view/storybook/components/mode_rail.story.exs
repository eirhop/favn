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
        slots: [
          """
          <:item label="Overview" icon="hero-play-circle" active>Overview</:item>
          <:item label="Docs" icon="hero-book-open">Docs</:item>
          <:item label="Definition" icon="hero-code-bracket">Definition</:item>
          <:item label="Lineage" icon="hero-share">Lineage</:item>
          <:item label="Notes" icon="hero-document-text">Notes</:item>
          <:item label="Settings" icon="hero-cog-6-tooth">Settings</:item>
          """
        ]
      }
    ]
  end
end
