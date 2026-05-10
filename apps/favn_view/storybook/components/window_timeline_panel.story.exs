defmodule FavnView.Storybook.Components.WindowTimelinePanel do
  alias FavnView.Components.AssetDetailPage

  use PhoenixStorybook.Story, :component

  def function, do: &AssetDetailPage.window_timeline_panel/1
  def layout, do: :one_column
  def render_source, do: :function

  def template do
    """
    <div data-theme="favn-dark" class="favn-shell-bg min-h-[42rem] p-12">
      <.psb-variation/>
    </div>
    """
  end

  def variations do
    [
      %Variation{
        id: :thirty_day_window,
        attributes: %{
          window_range: "May 24 - Jun 22, 2026",
          timeline: AssetDetailPage.sample_timeline()
        }
      }
    ]
  end
end
