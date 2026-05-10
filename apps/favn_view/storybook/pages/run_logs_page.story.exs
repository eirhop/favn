defmodule FavnView.Storybook.Pages.RunLogsPage do
  alias Favn.Log.Entry
  alias Favn.Log.Filter
  alias FavnView.Components.AssetCataloguePage
  alias FavnView.Components.LogPages
  alias FavnView.LogsViewModel

  use PhoenixStorybook.Story, :component

  def function, do: &LogPages.run_logs_page/1
  def layout, do: :one_column
  def render_source, do: :function
  def container, do: {:iframe, style: "width: 100%; height: 920px; border: 0;"}

  def variations do
    [
      %Variation{
        id: :run_logs_page,
        attributes:
          page_attrs(%{
            title: "Run logs",
            subtitle: "run_2026_06_12 · customer_orders_daily",
            status: "Running",
            status_tone: :info,
            nav_items: AssetCataloguePage.nav_items(:runs),
            back_href: "/runs/run_2026_06_12",
            back_label: "Back to run",
            facts: [%{label: "Started", value: "09:41:12"}, %{label: "Duration", value: "02:14"}],
            scope: :run,
            filter: %Filter{run_id: "run_2026_06_12"},
            empty_state: "No logs recorded for this run yet."
          })
      }
    ]
  end

  defp page_attrs(attrs) do
    logs = sample_logs()

    Map.merge(
      %{
        logs: logs,
        visible_logs: LogsViewModel.entries(logs),
        logs_status: :ready,
        live?: true,
        live_tail?: true,
        wrap?: true,
        search_query: "",
        selected_level: "all",
        selected_source: "all",
        next_cursor: nil,
        stream_warning: nil,
        context_note: nil
      },
      attrs
    )
  end

  defp sample_logs do
    [
      entry(1, :info, :runner, "Starting asset customer_orders_daily"),
      entry(
        2,
        :info,
        :adapter,
        "Running SQL:\nSELECT customer_id, order_id, total_amount\nFROM raw.orders"
      ),
      entry(3, :warning, :adapter, "Query exceeded warehouse threshold")
    ]
  end

  defp entry(sequence, level, source, message) do
    %Entry{
      id: "run-page-log-#{sequence}",
      global_sequence: sequence,
      run_id: "run_2026_06_12",
      occurred_at:
        DateTime.new!(~D[2026-06-12], ~T[09:41:12], "Etc/UTC")
        |> DateTime.add(sequence * 8, :second),
      level: level,
      source: source,
      message: message
    }
  end
end
