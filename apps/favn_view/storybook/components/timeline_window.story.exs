defmodule FavnView.Storybook.Components.TimelineWindow do
  alias FavnView.Components.AssetDetailPage

  use PhoenixStorybook.Story, :component

  def function, do: &AssetDetailPage.timeline_window/1
  def render_source, do: :function

  def template do
    """
    <div data-theme="favn-dark" class="favn-shell-bg flex min-h-[18rem] items-center justify-center p-12">
      <.psb-variation/>
    </div>
    """
  end

  def variations do
    [
      %Variation{
        id: :healthy,
        attributes: %{window: %{day: "12", month: "Jun", status: :success, current: true}}
      },
      %Variation{
        id: :late,
        attributes: %{window: %{day: "28", month: "May", status: :warning}}
      },
      %Variation{
        id: :pending,
        attributes: %{window: %{day: "29", month: "May", status: :muted}}
      }
    ]
  end
end
