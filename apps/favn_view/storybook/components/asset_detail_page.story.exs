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
          freshness: AssetDetailPage.sample_freshness(:fresh),
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
          freshness: AssetDetailPage.sample_freshness(:fresh),
          active_mode: :timeline,
          selected_window: AssetDetailPage.selected_sample_window()
        }
      },
      %Variation{
        id: :fresh_freshness_detail,
        attributes: freshness_attributes(:fresh)
      },
      %Variation{
        id: :stale_with_upstream_reason,
        attributes: freshness_attributes(:stale)
      },
      %Variation{
        id: :unknown_never_run_freshness,
        attributes: freshness_attributes(:unknown)
      },
      %Variation{
        id: :always_run_freshness,
        attributes: freshness_attributes(:always_run)
      },
      %Variation{
        id: :failed_latest_run_stale_unknown,
        attributes:
          freshness_attributes(:failed_unknown)
          |> Map.merge(%{status: "Failed", status_tone: :error})
      },
      %Variation{
        id: :default_auto_run_config,
        attributes: run_config_attributes(%{dependencies: "all", refresh: "auto"})
      },
      %Variation{
        id: :missing_only_run_config,
        attributes: run_config_attributes(%{dependencies: "all", refresh: "missing"})
      },
      %Variation{
        id: :force_selected_asset_run_config,
        attributes: run_config_attributes(%{dependencies: "all", refresh: "force_selected"})
      },
      %Variation{
        id: :force_selected_upstream_run_config,
        attributes:
          run_config_attributes(%{dependencies: "all", refresh: "force_selected_upstream"})
      },
      %Variation{
        id: :force_full_graph_run_config,
        attributes: run_config_attributes(%{dependencies: "all", refresh: "force_all"})
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
          freshness: AssetDetailPage.sample_freshness(:unknown),
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
          freshness: AssetDetailPage.sample_freshness(:fresh),
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
          freshness: AssetDetailPage.sample_freshness(:fresh),
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
          freshness: AssetDetailPage.sample_freshness(:unknown),
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
          freshness: AssetDetailPage.sample_freshness(:fresh),
          active_mode: :runs,
          selected_window: AssetDetailPage.selected_sample_window()
        }
      }
    ]
  end

  defp run_config_attributes(run_config) do
    %{
      title: "customer_orders_daily",
      status: "Healthy",
      status_tone: :success,
      window_range: "May 24 - Jun 22, 2026",
      nav_items: AssetDetailPage.sample_nav_items(),
      timeline: AssetDetailPage.sample_timeline(),
      freshness: AssetDetailPage.sample_freshness(:fresh),
      active_mode: :timeline,
      selected_window: AssetDetailPage.selected_sample_window(),
      run_config_open?: true,
      run_config: run_config
    }
  end

  defp freshness_attributes(state) do
    %{
      title: "customer_orders_daily",
      status: "Healthy",
      status_tone: :success,
      window_range: "May 24 - Jun 22, 2026",
      nav_items: AssetDetailPage.sample_nav_items(),
      timeline: AssetDetailPage.sample_timeline(),
      freshness: AssetDetailPage.sample_freshness(state),
      active_mode: :details,
      selected_window: AssetDetailPage.selected_sample_window()
    }
  end
end
