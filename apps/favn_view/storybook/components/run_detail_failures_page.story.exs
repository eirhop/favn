defmodule FavnView.Storybook.Components.RunDetailFailuresPage do
  alias FavnView.Components.RunDetailPage

  use PhoenixStorybook.Story, :component

  def function, do: &RunDetailPage.run_detail_page/1
  def layout, do: :one_column
  def render_source, do: :function

  def container, do: {:iframe, style: "width: 100%; height: 920px; border: 0;"}

  def variations do
    [
      %Variation{
        id: :default,
        attributes: %{
          run: RunDetailPage.sample_run(:partial),
          run_id: "run_backfill_8f2c9d1",
          nav_items: RunDetailPage.sample_nav_items(),
          active_mode: :failures
        }
      }
    ]
  end
end
