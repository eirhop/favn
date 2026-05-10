defmodule FavnView.Storybook.Components.AssetDetailPage do
  alias FavnView.Components.AssetDetailPage

  use PhoenixStorybook.Story, :component

  def function, do: &AssetDetailPage.asset_detail_page/1
  def layout, do: :one_column
  def render_source, do: :function

  def container, do: {:iframe, style: "width: 100%; height: 900px; border: 0;"}

  def variations do
    [
      %Variation{
        id: :default_timeline,
        attributes: %{
          title: "customer_orders_daily",
          status: "Healthy",
          status_tone: :success,
          window_range: "May 24 - Jun 22, 2026",
          nav_items: AssetDetailPage.sample_nav_items(),
          timeline: AssetDetailPage.sample_timeline(),
          active_mode: :timeline,
          selected_window: AssetDetailPage.selected_sample_window()
        }
      },
      %Variation{
        id: :selected_runnable_window,
        attributes: %{
          title: "customer_orders_daily",
          status: "Healthy",
          status_tone: :success,
          window_range: "May 24 - Jun 22, 2026",
          nav_items: AssetDetailPage.sample_nav_items(),
          timeline: AssetDetailPage.sample_timeline(),
          active_mode: :timeline,
          selected_window: AssetDetailPage.selected_sample_window()
        }
      },
      %Variation{
        id: :selected_non_runnable_window,
        attributes: %{
          title: "raw_payments",
          status: "Unknown",
          status_tone: :neutral,
          window_range: "May 24 - Jun 22, 2026",
          nav_items: AssetDetailPage.sample_nav_items(),
          timeline: AssetDetailPage.non_runnable_timeline(),
          active_mode: :timeline,
          selected_window: AssetDetailPage.selected_non_runnable_window()
        }
      },
      %Variation{
        id: :submit_success,
        attributes: %{
          title: "customer_orders_daily",
          status: "Healthy",
          status_tone: :success,
          window_range: "May 24 - Jun 22, 2026",
          nav_items: AssetDetailPage.sample_nav_items(),
          timeline: AssetDetailPage.sample_timeline(),
          active_mode: :timeline,
          selected_window: AssetDetailPage.selected_sample_window(),
          submitted_run_id: "run_01HZ"
        }
      },
      %Variation{
        id: :submit_error,
        attributes: %{
          title: "customer_orders_daily",
          status: "Healthy",
          status_tone: :success,
          window_range: "May 24 - Jun 22, 2026",
          nav_items: AssetDetailPage.sample_nav_items(),
          timeline: AssetDetailPage.sample_timeline(),
          active_mode: :timeline,
          selected_window: AssetDetailPage.selected_sample_window(),
          selected_window_error: "Could not submit run."
        }
      },
      %Variation{
        id: :selected_muted_window,
        attributes: %{
          title: "raw_payments",
          status: "Unknown",
          status_tone: :neutral,
          window_range: "May 24 - Jun 22, 2026",
          nav_items: AssetDetailPage.sample_nav_items(),
          timeline: AssetDetailPage.muted_timeline(),
          active_mode: :timeline,
          selected_window: AssetDetailPage.selected_muted_window()
        }
      },
      %Variation{
        id: :placeholder_mode,
        attributes: %{
          title: "customer_orders_daily",
          status: "Healthy",
          status_tone: :success,
          window_range: "May 24 - Jun 22, 2026",
          nav_items: AssetDetailPage.sample_nav_items(),
          timeline: AssetDetailPage.sample_timeline(),
          active_mode: :runs,
          selected_window: AssetDetailPage.selected_sample_window()
        }
      }
    ]
  end
end
