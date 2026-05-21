defmodule FavnView.Storybook.Components.RunDetailTimelinePage do
  alias FavnView.Components.RunDetailPage

  use PhoenixStorybook.Story, :component

  def function, do: &RunDetailPage.run_detail_page/1
  def layout, do: :one_column
  def render_source, do: :function

  def container, do: {:iframe, style: "width: 100%; height: 920px; border: 0;"}

  def variations do
    [
      %Variation{
        id: :live,
        attributes: %{
          run: RunDetailPage.sample_timeline_run(),
          run_id: "run_sales_backfill_timeline",
          nav_items: RunDetailPage.sample_nav_items(),
          active_mode: :timeline
        }
      },
      %Variation{
        id: :manual_zoom,
        attributes: %{
          run: RunDetailPage.sample_timeline_run(),
          run_id: "run_sales_backfill_timeline",
          nav_items: RunDetailPage.sample_nav_items(),
          active_mode: :timeline,
          timeline_state: %{
            mode: :manual,
            zoom: "1h",
            live_follow?: false,
            search: "",
            status: "all",
            window: "all",
            failed_only?: false,
            running_only?: false
          }
        }
      },
      %Variation{
        id: :fit_completed,
        attributes: %{
          run: RunDetailPage.sample_completed_timeline_run(),
          run_id: "run_sales_backfill_timeline_done",
          nav_items: RunDetailPage.sample_nav_items(),
          active_mode: :timeline,
          timeline_state: %{
            mode: :fit,
            zoom: "full",
            live_follow?: false,
            search: "",
            status: "all",
            window: "all",
            failed_only?: false,
            running_only?: false
          }
        }
      },
      %Variation{
        id: :skipped_filter,
        attributes: %{
          run: RunDetailPage.sample_completed_timeline_run(),
          run_id: "run_sales_backfill_timeline_done",
          nav_items: RunDetailPage.sample_nav_items(),
          active_mode: :timeline,
          timeline_state: %{
            mode: :fit,
            zoom: "full",
            live_follow?: false,
            search: "",
            status: "skipped",
            window: "all",
            failed_only?: false,
            running_only?: false
          }
        }
      }
    ]
  end
end
