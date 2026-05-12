defmodule FavnView.Storybook.Components.TimelineWindow do
  alias FavnView.Components.AssetDetailPage

  use PhoenixStorybook.Story, :component

  def function, do: &AssetDetailPage.timeline_window/1
  def render_source, do: :function

  def template do
    """
    <div data-theme="favn-dark" class="favn-shell-bg flex min-h-[18rem] items-center justify-center p-12">
      <.psb-variation/>
    </div>
    """
  end

  def variations do
    [
      %Variation{
        id: :fresh_refresh_period,
        attributes: %{window: timeline_window("refresh:day:2026-06-12", "Jun 12", :success)}
      },
      %Variation{
        id: :running_refresh_period,
        attributes: %{window: timeline_window("refresh:day:2026-06-13", "Jun 13", :warning)}
      },
      %Variation{
        id: :failed_data_window,
        attributes: %{window: timeline_window("window:day:2026-06-10", "Jun 10", :error)}
      },
      %Variation{
        id: :missing_data_window,
        attributes: %{window: timeline_window("window:day:2026-06-11", "Jun 11", :muted)}
      },
      %Variation{
        id: :selected_month_window,
        attributes: %{
          selected: true,
          window:
            timeline_window("window:month:2026-06", "Jun 2026", :success)
            |> Map.merge(%{date_label: "June 2026", range_label: "June 2026"})
        }
      }
    ]
  end

  defp timeline_window(id, label, status) do
    %{
      id: id,
      label: label,
      date_label: label,
      range_label: label,
      status: status,
      status_label: status_label(status),
      run_enabled?: true,
      run_disabled_reason: nil,
      run_label: "Run asset"
    }
  end

  defp status_label(:success), do: "Fresh"
  defp status_label(:warning), do: "Running"
  defp status_label(:error), do: "Failed"
  defp status_label(:muted), do: "Unknown"
end
