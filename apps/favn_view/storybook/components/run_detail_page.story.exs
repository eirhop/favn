defmodule FavnView.Storybook.Components.RunDetailPage do
  alias FavnView.Components.RunDetailPage

  use PhoenixStorybook.Story, :component

  def function, do: &RunDetailPage.run_detail_page/1
  def layout, do: :one_column
  def render_source, do: :function

  def container, do: {:iframe, style: "width: 100%; height: 920px; border: 0;"}

  def variations do
    [
      %Variation{
        id: :running_run,
        attributes: %{
          run: RunDetailPage.sample_run(:running),
          run_id: "run_01jrun_detail_sample",
          nav_items: RunDetailPage.sample_nav_items()
        }
      },
      %Variation{
        id: :succeeded_with_asset_results,
        attributes: %{
          run: RunDetailPage.sample_run(:ok),
          run_id: "run_01jrun_detail_sample",
          nav_items: RunDetailPage.sample_nav_items()
        }
      },
      %Variation{
        id: :failed_with_event_timeline,
        attributes: %{
          run: RunDetailPage.sample_run(:error),
          run_id: "run_01jrun_detail_sample",
          nav_items: RunDetailPage.sample_nav_items()
        }
      },
      %Variation{
        id: :empty_no_events,
        attributes: %{
          run: RunDetailPage.empty_run(),
          run_id: "run_01jrun_detail_sample",
          nav_items: RunDetailPage.sample_nav_items()
        }
      },
      %Variation{
        id: :not_found,
        attributes: %{
          run: RunDetailPage.not_found_run(),
          run_id: "run_missing",
          nav_items: RunDetailPage.sample_nav_items()
        }
      }
    ]
  end
end
