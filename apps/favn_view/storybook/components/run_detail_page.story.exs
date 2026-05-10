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
        id: :succeeded_with_asset_rows,
        attributes: %{
          run: RunDetailPage.sample_run(:ok),
          run_id: "run_01jrun_detail_sample",
          nav_items: RunDetailPage.sample_nav_items()
        }
      },
      %Variation{
        id: :running_no_asset_results,
        attributes: %{
          run: RunDetailPage.empty_run(),
          run_id: "run_01jrun_detail_sample",
          nav_items: RunDetailPage.sample_nav_items()
        }
      },
      %Variation{
        id: :running_with_partial_asset_results,
        attributes: %{
          run: RunDetailPage.sample_run(:running),
          run_id: "run_01jrun_detail_sample",
          nav_items: RunDetailPage.sample_nav_items()
        }
      },
      %Variation{
        id: :failed_with_asset_error,
        attributes: %{
          run: RunDetailPage.sample_run(:error),
          run_id: "run_01jrun_detail_sample",
          nav_items: RunDetailPage.sample_nav_items()
        }
      },
      %Variation{
        id: :failed_no_asset_results_error_event,
        attributes: %{
          run: RunDetailPage.sample_run_with_no_results(:error),
          run_id: "run_01jrun_detail_sample",
          nav_items: RunDetailPage.sample_nav_items()
        }
      },
      %Variation{
        id: :partial_run,
        attributes: %{
          run: RunDetailPage.sample_run(:partial),
          run_id: "run_01jrun_detail_sample",
          nav_items: RunDetailPage.sample_nav_items()
        }
      },
      %Variation{
        id: :cancelled_run,
        attributes: %{
          run: RunDetailPage.sample_run(:cancelled),
          run_id: "run_01jrun_detail_sample",
          nav_items: RunDetailPage.sample_nav_items()
        }
      },
      %Variation{
        id: :timed_out_run,
        attributes: %{
          run: RunDetailPage.sample_run(:timed_out),
          run_id: "run_01jrun_detail_sample",
          nav_items: RunDetailPage.sample_nav_items()
        }
      },
      %Variation{
        id: :long_asset_names,
        attributes: %{
          run: RunDetailPage.sample_run_with_long_asset_names(),
          run_id: "run_01jrun_detail_sample",
          nav_items: RunDetailPage.sample_nav_items()
        }
      },
      %Variation{
        id: :node_statuses,
        attributes: %{
          run: RunDetailPage.sample_run_with_node_statuses(),
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
