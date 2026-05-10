defmodule FavnView.Storybook.Components.GlassPanel do
  alias FavnView.Components.GlassPanel

  use PhoenixStorybook.Story, :component

  def function, do: &GlassPanel.glass_panel/1
  def layout, do: :one_column
  def render_source, do: :function

  def template do
    """
    <div data-theme="favn-dark" class="favn-shell-bg min-h-[28rem] p-12">
      <.psb-variation/>
    </div>
    """
  end

  def variations do
    [
      %Variation{
        id: :floating_panel,
        attributes: %{class: "mx-auto max-w-2xl p-8"},
        slots: [
          """
          <h2 class="text-xl font-medium">Glass panel</h2>
          <p class="mt-2 text-base-content/60">A sparse floating content surface over the Favn ambient background.</p>
          """
        ]
      }
    ]
  end
end
