defmodule FavnView.Storybook.Components.AssetDetailPage do
  alias FavnView.Components.AssetDetailPage

  use PhoenixStorybook.Story, :component

  def function, do: &AssetDetailPage.asset_detail_page/1
  def layout, do: :one_column
  def render_source, do: :function

  def container,
    do: {:iframe, style: "width: 100%; height: 900px; border: 0;", allowfullscreen: true}

  def template do
    """
    <div data-theme="favn-dark" class="relative min-h-screen">
      <button
        type="button"
        class="btn btn-primary btn-sm fixed right-4 top-4 z-50 shadow-xl"
        onclick="document.documentElement.requestFullscreen && document.documentElement.requestFullscreen()"
      >
        Fullscreen
      </button>
      <.psb-variation/>
    </div>
    """
  end

  def variations do
    [
      %Variation{
        id: :refresh_timeline_default_no_selection,
        attributes: base_attributes()
      },
      %Variation{
        id: :full_refresh_asset_no_data_windows,
        attributes:
          base_attributes()
          |> Map.merge(%{
            title: "stg_payments",
            status: "Unknown",
            status_tone: :neutral,
            has_data_windows?: false,
            data_coverage_timeline: nil,
            data_coverage_window_range: "No windows",
            selected_window: nil,
            freshness: AssetDetailPage.sample_freshness(:unknown)
          })
      },
      %Variation{
        id: :active_data_coverage_timeline,
        attributes:
          base_attributes()
          |> Map.merge(%{
            active_timeline: :data_coverage,
            selected_window: nil
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
        id: :refresh_run_config_open_editable,
        attributes:
          base_attributes()
          |> Map.merge(%{
            selected_window: nil,
            run_config_open?: true,
            run_config: run_config(:refresh_timeline, :day, "2026-06-12")
          })
      },
      %Variation{
        id: :data_coverage_run_config_open_editable,
        attributes:
          base_attributes()
          |> Map.merge(%{
            active_timeline: :data_coverage,
            selected_window: nil,
            run_config_open?: true,
            run_config: run_config(:data_coverage_timeline, :day, "2026-06-12")
          })
      },
      %Variation{
        id: :prefilled_failed_run_config,
        attributes:
          base_attributes()
          |> Map.merge(%{
            selected_window: failed_refresh_window(),
            run_config_open?: true,
            run_config: run_config(:refresh_timeline, :day, "2026-06-10", "none", "force_all")
          })
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
        id: :submit_success,
        attributes:
          base_attributes()
          |> Map.merge(%{
            selected_window: List.last(refresh_timeline()),
            submitted_run_id: "run_01HZ"
          })
      },
      %Variation{
        id: :submit_error,
        attributes:
          base_attributes()
          |> Map.merge(%{
            selected_window: List.last(refresh_timeline()),
            selected_window_error: "Could not submit run."
          })
      },
      %Variation{
        id: :selected_non_runnable_window,
        attributes:
          base_attributes()
          |> Map.merge(%{
            active_timeline: :data_coverage,
            selected_window:
              data_window("2026-06-12", "Jun 12", :muted)
              |> Map.merge(%{run_enabled?: false, run_disabled_reason: :invalid_window})
          })
      },
      %Variation{
        id: :placeholder_mode,
        attributes:
          base_attributes()
          |> Map.merge(%{active_mode: :runs, selected_window: List.last(refresh_timeline())})
      }
    ]
  end

  defp base_attributes do
    %{
      title: "customer_orders_daily",
      status: "Healthy",
      status_tone: :success,
      window_range: "May 14 - Jun 12",
      refresh_window_range: "May 14 - Jun 12",
      data_coverage_window_range: "May 14 - Jun 12",
      refresh_timeline_label: "Refresh timeline",
      refresh_cadence_label: "Daily refresh periods Etc/UTC",
      data_coverage_timeline_label: "Daily data windows",
      active_timeline: :refresh,
      has_data_windows?: true,
      can_run_asset?: true,
      nav_items: AssetDetailPage.sample_nav_items(),
      refresh_timeline: refresh_timeline(),
      data_coverage_timeline: data_coverage_timeline(),
      freshness: AssetDetailPage.sample_freshness(:fresh),
      active_mode: :timeline,
      selected_window: nil,
      run_config_open?: false,
      run_config: run_config(:refresh_timeline, :day, "2026-06-12")
    }
  end

  defp freshness_attributes(state) do
    base_attributes()
    |> Map.merge(%{
      active_mode: :details,
      selected_window: List.last(refresh_timeline()),
      freshness: AssetDetailPage.sample_freshness(state)
    })
  end

  defp refresh_timeline do
    [
      refresh_window("2026-06-09", "Jun 9", :success),
      failed_refresh_window(),
      refresh_window("2026-06-11", "Jun 11", :warning),
      refresh_window("2026-06-12", "Jun 12", :success)
    ]
  end

  defp data_coverage_timeline do
    [
      data_window("2026-06-09", "Jun 9", :success),
      data_window("2026-06-10", "Jun 10", :error),
      data_window("2026-06-11", "Jun 11", :muted),
      data_window("2026-06-12", "Jun 12", :success)
    ]
  end

  defp refresh_window(value, label, status) do
    timeline_window("refresh:day:#{value}", :refresh_timeline, :day, value, label, status)
  end

  defp failed_refresh_window do
    refresh_window("2026-06-10", "Jun 10", :error)
    |> Map.merge(%{
      latest_run_id: "run_failed_window",
      latest_run_status: :error,
      latest_run_config: run_config(:refresh_timeline, :day, "2026-06-10", "none", "force_all")
    })
  end

  defp data_window(value, label, status) do
    timeline_window("window:day:#{value}", :data_coverage_timeline, :day, value, label, status)
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
      default_run_config: run_config(source, kind, value)
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
