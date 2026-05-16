defmodule FavnView.Storybook.Components.PipelineDetailPage do
  use PhoenixStorybook.Story, :component

  alias FavnView.Components.PipelineDetailPage
  alias FavnView.Components.PipelinesPage

  def function, do: &PipelineDetailPage.pipeline_detail_page/1

  def imports, do: [{PipelineDetailPage, []}]

  def variations do
    [
      %Variation{
        id: :default,
        attributes: %{
          pipeline: PipelineDetailPage.sample_pipeline(),
          nav_items: PipelinesPage.nav_items(:pipelines),
          active_mode: :runs,
          backfill_config: %{from: "2024-01", to: "2026-12", kind: "month"}
        }
      },
      %Variation{
        id: :empty_history,
        attributes: %{
          pipeline: %{
            PipelineDetailPage.sample_pipeline()
            | runs: [],
              status: :unknown,
              status_label: "Unknown"
          },
          nav_items: PipelinesPage.nav_items(:pipelines),
          active_mode: :runs,
          backfill_config: %{from: "2024-01", to: "2026-12", kind: "month"}
        }
      }
    ]
  end
end
