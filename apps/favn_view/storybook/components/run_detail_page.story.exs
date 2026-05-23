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
        id: :backfill_overview_matrix,
        attributes: %{
          run:
            RunDetailPage.sample_run(:running)
            |> Map.merge(%{cancellable?: true, cancel_run_id: "run_backfill_8f2c9d1"}),
          run_id: "run_backfill_8f2c9d1",
          nav_items: RunDetailPage.sample_nav_items(),
          active_mode: :overview
        }
      },
      %Variation{
        id: :attempt_drawer_open,
        attributes: %{
          run: RunDetailPage.sample_run(:partial),
          run_id: "run_backfill_8f2c9d1",
          nav_items: RunDetailPage.sample_nav_items(),
          active_mode: :overview,
          selected_attempt_id: "revenue_metrics-2026-02"
        }
      },
      %Variation{
        id: :single_window_run,
        attributes: %{
          run: RunDetailPage.sample_single_window_run(),
          run_id: "run_daily_orders_2026_05_19",
          nav_items: RunDetailPage.sample_nav_items(),
          active_mode: :overview
        }
      },
      %Variation{
        id: :full_refresh_no_window,
        attributes: %{
          run: RunDetailPage.sample_full_refresh_run(),
          run_id: "run_full_refresh_sales",
          nav_items: RunDetailPage.sample_nav_items(),
          active_mode: :overview
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
