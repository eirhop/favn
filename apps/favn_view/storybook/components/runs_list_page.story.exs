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
        id: :execution_groups,
        attributes: %{
          groups: RunsListPage.sample_groups(),
          all_groups: RunsListPage.sample_groups(),
          group_details: %{"run_backfill_8f2c9d1" => RunsListPage.sample_detail()},
          expanded_group_ids: MapSet.new(["run_backfill_8f2c9d1"]),
          filters: RunsListPage.sample_filters(),
          filter_options: RunsListPage.sample_filter_options(),
          summary: RunsListPage.sample_summary(),
          active_mode: :list,
          loading: false,
          error: nil,
          nav_items: RunsListPage.nav_items()
        }
      },
      %Variation{
        id: :empty,
        attributes: %{
          groups: [],
          all_groups: [],
          group_details: %{},
          expanded_group_ids: MapSet.new(),
          filters: RunsListPage.sample_filters(),
          filter_options: %{targets: [], triggers: []},
          summary: RunsListPage.sample_summary([]),
          active_mode: :list,
          loading: false,
          error: nil,
          nav_items: RunsListPage.nav_items()
        }
      },
      %Variation{
        id: :loading,
        attributes: %{
          groups: [],
          all_groups: [],
          group_details: %{},
          expanded_group_ids: MapSet.new(),
          filters: RunsListPage.sample_filters(),
          filter_options: %{targets: [], triggers: []},
          summary: RunsListPage.sample_summary([]),
          active_mode: :list,
          loading: true,
          error: nil,
          nav_items: RunsListPage.nav_items()
        }
      },
      %Variation{
        id: :error,
        attributes: %{
          groups: [],
          all_groups: [],
          group_details: %{},
          expanded_group_ids: MapSet.new(),
          filters: RunsListPage.sample_filters(),
          filter_options: %{targets: [], triggers: []},
          summary: RunsListPage.sample_summary([]),
          active_mode: :list,
          loading: false,
          error: "load_failed",
          nav_items: RunsListPage.nav_items()
        }
      }
    ]
  end
end
