defmodule FavnView.Storybook.Components.WindowTimelinePanel do
  alias FavnView.Components.AssetDetailPage

  use PhoenixStorybook.Story, :component

  def function, do: &AssetDetailPage.window_timeline_panel/1
  def layout, do: :one_column
  def render_source, do: :function

  def template do
    """
    <div data-theme="favn-dark" class="favn-shell-bg min-h-[42rem] p-12">
      <.psb-variation/>
    </div>
    """
  end

  def variations do
    [
      %Variation{
        id: :full_refresh_only,
        attributes:
          base_attributes()
          |> Map.merge(%{
            has_data_windows?: false,
            refresh_timeline: refresh_timeline(),
            data_coverage_timeline: nil,
            selected_window: nil
          })
      },
      %Variation{
        id: :refresh_and_data_toggle,
        attributes: base_attributes()
      },
      %Variation{
        id: :active_data_coverage,
        attributes:
          base_attributes()
          |> Map.merge(%{
            active_timeline: :data_coverage,
            selected_window: nil
          })
      },
      %Variation{
        id: :no_selected_context_run_config,
        attributes:
          base_attributes()
          |> Map.merge(%{
            selected_window: nil,
            run_config_open?: true,
            run_config: run_config(:refresh_timeline, :day, "2026-06-12")
          })
      },
      %Variation{
        id: :selected_refresh_period,
        attributes:
          base_attributes()
          |> Map.merge(%{selected_window: List.last(refresh_timeline())})
      },
      %Variation{
        id: :selected_data_window,
        attributes:
          base_attributes()
          |> Map.merge(%{
            active_timeline: :data_coverage,
            selected_window: List.last(data_coverage_timeline())
          })
      },
      %Variation{
        id: :data_coverage_run_config_editable,
        attributes:
          base_attributes()
          |> Map.merge(%{
            active_timeline: :data_coverage,
            selected_window: nil,
            run_config_open?: true,
            run_config: run_config(:data_coverage_timeline, :day, "2026-06-12")
          })
      }
    ]
  end

  defp base_attributes do
    %{
      window_range: "May 14 - Jun 12",
      refresh_window_range: "May 14 - Jun 12",
      data_coverage_window_range: "May 14 - Jun 12",
      refresh_timeline_label: "Refresh timeline",
      refresh_cadence_label: "Daily refresh periods Etc/UTC",
      data_coverage_timeline_label: "Daily data windows",
      active_timeline: :refresh,
      has_data_windows?: true,
      can_run_asset?: true,
      refresh_timeline: refresh_timeline(),
      data_coverage_timeline: data_coverage_timeline(),
      freshness: AssetDetailPage.sample_freshness(:fresh),
      selected_window: nil,
      run_config_open?: false,
      run_config: run_config(:refresh_timeline, :day, "2026-06-12")
    }
  end

  defp refresh_timeline do
    [
      timeline_window(
        "refresh:day:2026-06-09",
        :refresh_timeline,
        :day,
        "2026-06-09",
        "Jun 9",
        :success
      ),
      timeline_window(
        "refresh:day:2026-06-10",
        :refresh_timeline,
        :day,
        "2026-06-10",
        "Jun 10",
        :error
      ),
      timeline_window(
        "refresh:day:2026-06-11",
        :refresh_timeline,
        :day,
        "2026-06-11",
        "Jun 11",
        :warning
      ),
      timeline_window(
        "refresh:day:2026-06-12",
        :refresh_timeline,
        :day,
        "2026-06-12",
        "Jun 12",
        :success
      )
    ]
  end

  defp data_coverage_timeline do
    [
      timeline_window(
        "window:day:2026-06-09",
        :data_coverage_timeline,
        :day,
        "2026-06-09",
        "Jun 9",
        :success
      ),
      timeline_window(
        "window:day:2026-06-10",
        :data_coverage_timeline,
        :day,
        "2026-06-10",
        "Jun 10",
        :error
      ),
      timeline_window(
        "window:day:2026-06-11",
        :data_coverage_timeline,
        :day,
        "2026-06-11",
        "Jun 11",
        :muted
      ),
      timeline_window(
        "window:day:2026-06-12",
        :data_coverage_timeline,
        :day,
        "2026-06-12",
        "Jun 12",
        :success
      )
    ]
  end

  defp timeline_window(id, source, kind, value, label, status) do
    %{
      id: id,
      source: source,
      kind: kind,
      value: value,
      timezone: "Etc/UTC",
      label: label,
      date_label: "#{label}, 2026",
      range_label: "#{label}, 2026",
      status: status,
      status_label: status_label(status),
      latest_run_id: if(status == :error, do: "run_failed_window"),
      latest_run_status: if(status == :error, do: :error),
      run_enabled?: true,
      run_disabled_reason: nil,
      run_label: "Run asset",
      default_run_config: run_config(source, kind, value),
      latest_run_config:
        if(status == :error, do: run_config(source, kind, value, "none", "force_all"))
    }
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
