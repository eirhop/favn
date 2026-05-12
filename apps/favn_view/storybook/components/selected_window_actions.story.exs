defmodule FavnView.Storybook.Components.SelectedWindowActions do
  alias FavnView.Components.SelectedWindowActions

  use PhoenixStorybook.Story, :component

  def function, do: &SelectedWindowActions.selected_window_actions/1
  def layout, do: :one_column
  def render_source, do: :function

  def container, do: {:iframe, style: "width: 100%; height: 820px; border: 0;"}

  def template do
    """
    <div data-theme="favn-dark" class="favn-shell-bg flex min-h-[28rem] items-center p-12 text-base-content">
      <div class="w-full max-w-6xl">
        <.psb-variation/>
      </div>
    </div>
    """
  end

  def variations do
    [
      %Variation{
        id: :no_selection_full_refresh,
        attributes: %{
          selected_window: nil,
          has_data_windows?: false,
          run_config: run_config(nil, "", "")
        }
      },
      %Variation{
        id: :no_selection_windowed_refresh,
        attributes: %{
          selected_window: nil,
          has_data_windows?: true,
          active_timeline: :refresh,
          run_config: run_config(:refresh_timeline, :day, "2026-06-12")
        }
      },
      %Variation{
        id: :no_selection_refresh_editable_config,
        attributes: %{
          selected_window: nil,
          has_data_windows?: true,
          active_timeline: :refresh,
          run_config_open?: true,
          run_config: run_config(:refresh_timeline, :day, "2026-06-12")
        }
      },
      %Variation{
        id: :no_selection_data_window_editable_config,
        attributes: %{
          selected_window: nil,
          has_data_windows?: true,
          active_timeline: :data_coverage,
          run_config_open?: true,
          run_config: run_config(:data_coverage_timeline, :day, "2026-06-12")
        }
      },
      %Variation{
        id: :selected_refresh_period,
        attributes: %{
          selected_window: refresh_window(),
          has_data_windows?: true,
          active_timeline: :refresh,
          run_config: run_config(:refresh_timeline, :day, "2026-06-12")
        }
      },
      %Variation{
        id: :selected_data_window,
        attributes: %{
          selected_window: data_window(),
          has_data_windows?: true,
          active_timeline: :data_coverage,
          run_config: run_config(:data_coverage_timeline, :day, "2026-06-12")
        }
      },
      %Variation{
        id: :selected_fields_disabled,
        attributes: %{
          selected_window: data_window(),
          has_data_windows?: true,
          active_timeline: :data_coverage,
          run_config_open?: true,
          run_config: run_config(:data_coverage_timeline, :day, "2026-06-12")
        }
      },
      %Variation{
        id: :prefilled_latest_failed_run_config,
        attributes: %{
          selected_window: failed_refresh_window(),
          has_data_windows?: true,
          active_timeline: :refresh,
          run_config_open?: true,
          run_config: run_config(:refresh_timeline, :day, "2026-06-10", "none", "force_all")
        }
      },
      %Variation{
        id: :submitting,
        attributes: %{
          selected_window: refresh_window(),
          has_data_windows?: true,
          submitting_window_run?: true,
          run_config: run_config(:refresh_timeline, :day, "2026-06-12")
        }
      },
      %Variation{
        id: :success,
        attributes: %{
          selected_window: refresh_window(),
          has_data_windows?: true,
          submitted_run_id: "run_01HZ",
          run_config: run_config(:refresh_timeline, :day, "2026-06-12")
        }
      },
      %Variation{
        id: :error,
        attributes: %{
          selected_window: refresh_window(),
          has_data_windows?: true,
          selected_window_error: "Could not submit run.",
          run_config: run_config(:refresh_timeline, :day, "2026-06-12")
        }
      },
      %Variation{
        id: :not_runnable,
        attributes: %{
          selected_window:
            Map.merge(data_window(), %{run_enabled?: false, run_disabled_reason: :invalid_window}),
          has_data_windows?: true,
          active_timeline: :data_coverage,
          run_config: run_config(:data_coverage_timeline, :day, "2026-06-12")
        }
      }
    ]
  end

  defp refresh_window do
    timeline_window(
      "refresh:day:2026-06-12",
      :refresh_timeline,
      :day,
      "2026-06-12",
      "Jun 12, 2026",
      :success
    )
  end

  defp failed_refresh_window do
    timeline_window(
      "refresh:day:2026-06-10",
      :refresh_timeline,
      :day,
      "2026-06-10",
      "Jun 10, 2026",
      :error
    )
    |> Map.merge(%{
      latest_run_id: "run_failed_window",
      latest_run_status: :error,
      latest_run_config: run_config(:refresh_timeline, :day, "2026-06-10", "none", "force_all")
    })
  end

  defp data_window do
    timeline_window(
      "window:day:2026-06-12",
      :data_coverage_timeline,
      :day,
      "2026-06-12",
      "Jun 12, 2026",
      :success
    )
  end

  defp timeline_window(id, source, kind, value, label, status) do
    %{
      id: id,
      source: source,
      kind: kind,
      value: value,
      timezone: "Etc/UTC",
      label: label,
      date_label: label,
      range_label: label,
      status: status,
      status_label: status_label(status),
      run_enabled?: true,
      run_disabled_reason: nil,
      run_label: "Run asset",
      default_run_config: run_config(source, kind, value)
    }
  end

  defp run_config(nil, _kind, _value) do
    %{dependencies: "all", refresh: "auto", source: nil, kind: "", value: "", timezone: "Etc/UTC"}
  end

  defp run_config(source, kind, value, dependencies \\ "all", refresh \\ "auto") do
    %{
      dependencies: dependencies,
      refresh: refresh,
      source: Atom.to_string(source),
      kind: Atom.to_string(kind),
      value: value,
      timezone: "Etc/UTC"
    }
  end

  defp status_label(:success), do: "Fresh"
  defp status_label(:warning), do: "Running"
  defp status_label(:error), do: "Failed"
  defp status_label(:muted), do: "Missing"
end
