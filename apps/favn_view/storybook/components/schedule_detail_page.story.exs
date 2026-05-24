defmodule FavnView.Storybook.Components.ScheduleDetailPage do
  alias FavnView.Components.ScheduleDetailPage

  use PhoenixStorybook.Story, :component

  def function, do: &ScheduleDetailPage.schedule_detail_page/1
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
        id: :overview,
        attributes: %{
          schedule: ScheduleDetailPage.sample_schedule(),
          occurrence_preview: ScheduleDetailPage.sample_occurrences(),
          occurrence_error: nil,
          active_view: :overview,
          loading: false,
          error: nil,
          nav_items: ScheduleDetailPage.nav_items()
        }
      },
      %Variation{
        id: :occurrences,
        attributes: %{
          schedule:
            ScheduleDetailPage.sample_schedule(%{
              activation_state: :enabled,
              activation_label: "Enabled",
              activation_tone: :success,
              runtime_state: :idle,
              runtime_label: "Idle",
              effective_enabled?: true
            }),
          occurrence_preview: ScheduleDetailPage.sample_occurrences(),
          occurrence_error: nil,
          active_view: :occurrences,
          loading: false,
          error: nil,
          nav_items: ScheduleDetailPage.nav_items()
        }
      },
      %Variation{
        id: :occurrences_disabled,
        attributes: %{
          schedule: ScheduleDetailPage.sample_schedule(),
          occurrence_preview: ScheduleDetailPage.sample_occurrences(),
          occurrence_error: nil,
          active_view: :occurrences,
          loading: false,
          error: nil,
          nav_items: ScheduleDetailPage.nav_items()
        }
      },
      %Variation{
        id: :occurrences_empty,
        attributes: %{
          schedule:
            ScheduleDetailPage.sample_schedule(%{
              activation_state: :enabled,
              activation_label: "Enabled",
              activation_tone: :success,
              effective_enabled?: true
            }),
          occurrence_preview: [],
          occurrence_error: nil,
          active_view: :occurrences,
          loading: false,
          error: nil,
          nav_items: ScheduleDetailPage.nav_items()
        }
      },
      %Variation{
        id: :occurrences_error,
        attributes: %{
          schedule: ScheduleDetailPage.sample_schedule(),
          occurrence_preview: [],
          occurrence_error: "invalid_cron_or_timezone",
          active_view: :occurrences,
          loading: false,
          error: nil,
          nav_items: ScheduleDetailPage.nav_items()
        }
      },
      %Variation{
        id: :scheduler_error,
        attributes: %{
          schedule:
            ScheduleDetailPage.sample_schedule(%{
              last_scheduler_error: %{
                occurred_label: "May 24 12:02",
                phase_label: "Submit run",
                code_label: "Invalid scheduled window policy",
                message: "Window policy could not be resolved"
              }
            }),
          occurrence_preview: ScheduleDetailPage.sample_occurrences(),
          occurrence_error: nil,
          active_view: :overview,
          loading: false,
          error: nil,
          nav_items: ScheduleDetailPage.nav_items()
        }
      },
      %Variation{
        id: :current_run,
        attributes: %{
          schedule:
            ScheduleDetailPage.sample_schedule(%{
              activation_state: :enabled,
              activation_label: "Enabled",
              activation_tone: :success,
              runtime_state: :running,
              runtime_label: "Running",
              effective_enabled?: true,
              in_flight_run_id: "run_8f3a2c",
              current_run_label: "run_8f3a2c"
            }),
          occurrence_preview: ScheduleDetailPage.sample_occurrences(),
          occurrence_error: nil,
          active_view: :overview,
          loading: false,
          error: nil,
          nav_items: ScheduleDetailPage.nav_items()
        }
      },
      %Variation{
        id: :not_found,
        attributes: %{
          schedule: nil,
          occurrence_preview: [],
          occurrence_error: nil,
          active_view: :overview,
          loading: false,
          error: :not_found,
          nav_items: ScheduleDetailPage.nav_items()
        }
      },
      %Variation{
        id: :loading,
        attributes: %{
          schedule: nil,
          occurrence_preview: [],
          occurrence_error: nil,
          active_view: :overview,
          loading: true,
          error: nil,
          nav_items: ScheduleDetailPage.nav_items()
        }
      }
    ]
  end
end
