defmodule FavnView.Storybook.Components.OutputMetadata do
  alias FavnView.Components.OutputMetadata

  use PhoenixStorybook.Story, :component

  def function, do: &OutputMetadata.output_metadata/1
  def layout, do: :one_column
  def render_source, do: :function
  def container, do: {:iframe, style: "width: 100%; height: 720px; border: 0;"}

  def variations do
    [
      %Variation{
        id: :operational_metadata,
        attributes: %{
          id: "output-metadata-operational",
          status: :ok,
          metadata: %{
            rows_written: 0,
            rows_read: 1800,
            relation: "raw.mercatus.reporting_baseline_feeding",
            mode: :monthly_replace,
            partition_month: "2026-04",
            endpoint: "vReportingBaselineFeeding",
            loaded_at: ~U[2026-05-20 18:06:44Z],
            source: %{system: :mercatus}
          }
        }
      },
      %Variation{
        id: :empty_success,
        attributes: %{id: "output-metadata-empty-success", status: :ok, metadata: %{}}
      },
      %Variation{
        id: :failed_before_output,
        attributes: %{id: "output-metadata-failed", status: :error, metadata: %{}}
      },
      %Variation{
        id: :nested_and_large,
        attributes: %{
          id: "output-metadata-nested-large",
          status: :ok,
          initial_rows: 6,
          metadata: large_metadata()
        }
      }
    ]
  end

  defp large_metadata do
    Map.merge(
      %{
        rows_inserted: 42,
        rows_updated: 0,
        rows_deleted: 0,
        source: %{system: :warehouse, endpoint: "/v1/orders", empty: []},
        window: %{kind: :month, start_at: "2026-04-01", end_at: "2026-05-01"},
        flags: %{dry_run: false, warnings: []}
      },
      Map.new(1..16, fn index -> {"diagnostic_#{index}", "value #{index}"} end)
    )
  end
end
