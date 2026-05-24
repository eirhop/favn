defmodule FavnView.Storybook.Components.SchedulesPage do
  alias FavnView.Components.SchedulesPage

  use PhoenixStorybook.Story, :component

  def function, do: &SchedulesPage.schedules_page/1
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
        id: :schedules,
        attributes: %{
          schedules: SchedulesPage.sample_schedules(),
          all_schedules: SchedulesPage.sample_schedules(),
          filters: SchedulesPage.sample_filters(),
          filter_options: SchedulesPage.sample_filter_options(),
          summary: SchedulesPage.sample_summary(),
          active_mode: :list,
          loading: false,
          error: nil,
          nav_items: SchedulesPage.nav_items()
        }
      },
      %Variation{
        id: :empty,
        attributes: %{
          schedules: [],
          all_schedules: [],
          filters: SchedulesPage.sample_filters(),
          filter_options: %{pipelines: [], windows: []},
          summary: SchedulesPage.sample_summary([]),
          active_mode: :list,
          loading: false,
          error: nil,
          nav_items: SchedulesPage.nav_items()
        }
      },
      %Variation{
        id: :loading,
        attributes: %{
          schedules: [],
          all_schedules: [],
          filters: SchedulesPage.sample_filters(),
          filter_options: %{pipelines: [], windows: []},
          summary: SchedulesPage.sample_summary([]),
          active_mode: :list,
          loading: true,
          error: nil,
          nav_items: SchedulesPage.nav_items()
        }
      },
      %Variation{
        id: :error,
        attributes: %{
          schedules: [],
          all_schedules: [],
          filters: SchedulesPage.sample_filters(),
          filter_options: %{pipelines: [], windows: []},
          summary: SchedulesPage.sample_summary([]),
          active_mode: :list,
          loading: false,
          error: "load_failed",
          nav_items: SchedulesPage.nav_items()
        }
      }
    ]
  end
end
