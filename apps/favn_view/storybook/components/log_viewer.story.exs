defmodule FavnView.Storybook.Components.LogViewer do
  alias Favn.Log.Entry
  alias FavnView.Components.LogViewer
  alias FavnView.LogsViewModel

  use PhoenixStorybook.Story, :component

  def function, do: &LogViewer.log_viewer/1
  def layout, do: :one_column
  def render_source, do: :function
  def container, do: {:iframe, style: "width: 100%; height: 860px; border: 0;"}

  def variations do
    [
      variation(:global_mixed, "Logs", "Live system and run logs", sample_logs(), true, true),
      variation(
        :running_live_stream,
        "Logs",
        "Listening to live backend logs",
        sample_logs(:streaming),
        true,
        true
      ),
      variation(:empty_logs, "Logs", "Live system and run logs", [], true, true),
      variation(:multi_line_sql, "Logs", "SQL runtime output", sample_logs(:sql), true, true),
      variation(:long_stacktrace, "Logs", "Error output", sample_logs(:stacktrace), true, true),
      variation(:wrap_on, "Logs", "Wrapped long messages", sample_logs(:long), true, true),
      variation(
        :wrap_off,
        "Logs",
        "Horizontal overflow for long messages",
        sample_logs(:long),
        true,
        false
      ),
      variation(
        :truncated_log,
        "Logs",
        "Persisted truncated output",
        sample_logs(:truncated),
        true,
        true
      ),
      variation(
        :asset_step_scoped,
        "customer_orders_daily",
        "Run r_2026_06_12 · Asset step 03",
        sample_logs(:asset),
        true,
        true
      )
    ]
  end

  defp variation(id, title, subtitle, logs, live?, wrap?) do
    view_logs = LogsViewModel.entries(logs)

    %Variation{
      id: id,
      attributes: %{
        logs: logs,
        visible_logs: view_logs,
        title: title,
        subtitle: subtitle,
        status: :ready,
        live?: live?,
        live_tail?: true,
        wrap?: wrap?,
        search_query: "",
        selected_level: "all",
        selected_source: "all",
        empty_state: "No logs yet.",
        facts: [
          %{label: "Started", value: "09:41:12"},
          %{label: "Duration", value: "02:14"},
          %{label: "Attempt", value: "1/3"}
        ]
      }
    }
  end

  defp sample_logs(kind \\ :mixed)
  defp sample_logs(:mixed), do: base_logs()

  defp sample_logs(:streaming),
    do: base_logs() ++ [entry(5, :info, :runner, "Streaming new log lines...")]

  defp sample_logs(:sql),
    do: [
      entry(
        1,
        :info,
        :adapter,
        "Running SQL:\nSELECT customer_id, order_id, total_amount\nFROM raw.orders\nWHERE order_date >= '2026-06-12'"
      )
    ]

  defp sample_logs(:stacktrace),
    do: [
      entry(
        1,
        :error,
        :user_code,
        "Warehouse timeout\nquery_id=01bc...\n    at Favn.Asset.run/2\n    at FavnRunner.Worker.perform/1"
      )
    ]

  defp sample_logs(:long),
    do: [
      entry(
        1,
        :info,
        :runner,
        String.duplicate(
          "customer_orders_daily emitted a very long diagnostic message with many columns and partitions ",
          4
        )
      )
    ]

  defp sample_logs(:truncated),
    do: [
      entry(1, :warning, :system, "Output exceeded maximum persisted log size\n[TRUNCATED]", true)
    ]

  defp sample_logs(:asset),
    do: base_logs() |> Enum.map(&%{&1 | run_id: "r_2026_06_12", asset_step_id: "03"})

  defp base_logs do
    [
      entry(1, :info, :runner, "Starting asset customer_orders_daily"),
      entry(
        2,
        :info,
        :adapter,
        "Running SQL:\nSELECT customer_id, order_id, total_amount\nFROM raw.orders\nWHERE order_date >= '2026-06-12'"
      ),
      entry(3, :warning, :adapter, "Query exceeded warehouse threshold"),
      entry(4, :error, :adapter, "Warehouse timeout\nquery_id=01bc...")
    ]
  end

  defp entry(sequence, level, source, message, truncated \\ false) do
    %Entry{
      id: "story-log-#{sequence}",
      global_sequence: sequence,
      occurred_at:
        DateTime.new!(~D[2026-06-12], ~T[09:41:12], "Etc/UTC")
        |> DateTime.add(sequence * 8, :second),
      level: level,
      source: source,
      message: message,
      metadata: %{story: true},
      truncated: truncated
    }
  end
end
