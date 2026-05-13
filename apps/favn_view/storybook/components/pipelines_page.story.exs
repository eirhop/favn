defmodule FavnView.Storybook.Components.PipelinesPage do
  alias FavnView.Components.PipelinesPage

  use PhoenixStorybook.Story, :component

  def function, do: &PipelinesPage.pipelines_page/1
  def layout, do: :one_column
  def render_source, do: :function

  def container, do: {:iframe, style: "width: 100%; height: 980px; border: 0;"}

  def template do
    """
    <div data-theme="favn-dark">
      <.psb-variation/>
    </div>
    """
  end

  def variations do
    [
      %Variation{
        id: :pipelines,
        attributes: %{
          pipelines: PipelinesPage.sample_pipelines(),
          filters: %{search: "", status: "all"},
          active_mode: :list,
          loading: false,
          error: nil,
          nav_items: PipelinesPage.nav_items(),
          status_options: PipelinesPage.status_options()
        }
      },
      %Variation{
        id: :empty,
        attributes: %{
          pipelines: [],
          filters: %{search: "not_real", status: "failed"},
          active_mode: :filters,
          loading: false,
          error: nil,
          nav_items: PipelinesPage.nav_items(),
          status_options: PipelinesPage.status_options()
        }
      },
      %Variation{
        id: :loading,
        attributes: %{
          pipelines: [],
          filters: %{search: "", status: "all"},
          active_mode: :list,
          loading: true,
          error: nil,
          nav_items: PipelinesPage.nav_items(),
          status_options: PipelinesPage.status_options()
        }
      },
      %Variation{
        id: :error,
        attributes: %{
          pipelines: [],
          filters: %{search: "", status: "all"},
          active_mode: :list,
          loading: false,
          error: "load_failed",
          nav_items: PipelinesPage.nav_items(),
          status_options: PipelinesPage.status_options()
        }
      }
    ]
  end
end
