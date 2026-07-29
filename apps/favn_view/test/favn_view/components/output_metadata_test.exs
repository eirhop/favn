defmodule FavnView.Components.OutputMetadataTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias FavnView.Components.OutputMetadata

  doctest OutputMetadata

  # The shape a SQL asset actually reports, taken from a real run rather than
  # guessed: no row count, the relation nested under `materialized`, and
  # `write_outcome` spelled "written".
  @sql_metadata %{
    "quality_status" => "passed",
    "write_outcome" => "written",
    "rows_affected" => nil,
    "command" => "sql",
    "connection" => "warehouse",
    "materialized" => %{
      "catalog" => nil,
      "connection" => "warehouse",
      "name" => "pipeline_daily",
      "schema" => "mart"
    },
    "check_results" => [
      %{"name" => "row_count_positive", "origin" => "contract", "outcome" => "passed"}
    ]
  }

  describe "outcome/2" do
    test "a SQL materialisation reports where it wrote and that its checks passed" do
      assert %{headline: "Table written", target: "mart.pipeline_daily", tone: :success} =
               outcome = OutputMetadata.outcome(@sql_metadata, :ok)

      assert outcome.checks == %{label: "1 check passed", tone: :success}
    end

    test "a row count wins over the generic written headline" do
      metadata = Map.put(@sql_metadata, "rows_written", 1_204)

      assert %{headline: "Wrote 1,204 rows"} = OutputMetadata.outcome(metadata, :ok)
    end

    test "rows_affected counts when rows_written is absent" do
      metadata = Map.put(@sql_metadata, "rows_affected", 7)

      assert %{headline: "Wrote 7 rows"} = OutputMetadata.outcome(metadata, :ok)
    end

    test "one row is not plural" do
      metadata = Map.put(@sql_metadata, "rows_written", 1)

      assert %{headline: "Wrote 1 row"} = OutputMetadata.outcome(metadata, :ok)
    end

    test "a failed attempt tones the same write as an error" do
      assert %{tone: :error} = OutputMetadata.outcome(@sql_metadata, :error)
    end

    test "an unknown transaction outcome is never reported as a success" do
      metadata = Map.put(@sql_metadata, "write_outcome", "unknown")

      assert %{headline: "Write outcome unknown", tone: :error} =
               OutputMetadata.outcome(metadata, :error)
    end

    test "a failed check is named in the verdict" do
      metadata =
        Map.put(@sql_metadata, "check_results", [
          %{"name" => "a", "outcome" => "passed"},
          %{"name" => "b", "outcome" => "failed"}
        ])

      assert %{checks: %{label: "1 of 2 checks failed", tone: :error}} =
               OutputMetadata.outcome(metadata, :error)
    end

    test "metadata that says nothing about a write yields no verdict line" do
      assert OutputMetadata.outcome(%{"command" => "sql"}, :ok) == nil
      assert OutputMetadata.outcome(%{}, :ok) == nil
      assert OutputMetadata.outcome(nil, :ok) == nil
    end
  end

  describe "output_metadata/1" do
    test "keeps every field behind one disclosure rather than promoting twelve" do
      html = render_component(&OutputMetadata.output_metadata/1, metadata: @sql_metadata)

      assert html =~ "metadata fields"
      assert html =~ "mart"
    end

    test "says nothing about checks that all passed" do
      html =
        render_component(&OutputMetadata.output_metadata/1,
          metadata: @sql_metadata,
          status: :ok
        )

      refute html =~ ~s(data-testid="sql-check-summary")
      refute html =~ ~s(data-testid="sql-check-result")
    end

    test "a no-op write still reports itself, because keeping a table is a decision" do
      metadata = Map.put(@sql_metadata, "write_outcome", "no_op")

      html =
        render_component(&OutputMetadata.output_metadata/1, metadata: metadata, status: :ok)

      assert html =~ ~s(data-testid="sql-check-summary")
      assert html =~ "No-op write"
    end

    test "lists the individual checks once one has failed" do
      metadata =
        Map.put(@sql_metadata, "check_results", [
          %{"name" => "revenue_not_negative", "outcome" => "failed", "message" => "3 rows"}
        ])

      html =
        render_component(&OutputMetadata.output_metadata/1, metadata: metadata, status: :error)

      assert html =~ ~s(data-testid="sql-check-result")
      assert html =~ "revenue_not_negative"
    end

    test "says why there is no metadata rather than rendering an empty list" do
      html = render_component(&OutputMetadata.output_metadata/1, metadata: %{}, status: :error)

      assert html =~ "failed before completion"
    end
  end
end
