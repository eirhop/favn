defmodule FavnView.Storybook.Components.RunsListPage do
  alias FavnView.Components.RunsListPage

  use PhoenixStorybook.Story, :component

  def function, do: &RunsListPage.runs_list_page/1
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
        id: :runs,
        attributes: %{
          runs: RunsListPage.sample_runs(),
          active_mode: :list,
          loading: false,
          error: nil,
          nav_items: RunsListPage.nav_items()
        }
      },
      %Variation{
        id: :empty,
        attributes: %{
          runs: [],
          active_mode: :list,
          loading: false,
          error: nil,
          nav_items: RunsListPage.nav_items()
        }
      },
      %Variation{
        id: :loading,
        attributes: %{
          runs: [],
          active_mode: :list,
          loading: true,
          error: nil,
          nav_items: RunsListPage.nav_items()
        }
      },
      %Variation{
        id: :error,
        attributes: %{
          runs: [],
          active_mode: :list,
          loading: false,
          error: "load_failed",
          nav_items: RunsListPage.nav_items()
        }
      }
    ]
  end
end
