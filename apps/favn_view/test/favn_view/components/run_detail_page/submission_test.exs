defmodule FavnView.Components.RunDetailPage.SubmissionTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias FavnView.Components.RunDetailPage.Submission

  test "renders a failed preparation with an actionable runner diagnostics link" do
    html =
      render_component(&Submission.submission_panel/1,
        run: %{
          raw_status: :failed,
          updated_at: "Jul 31, 2026 12:00:00 UTC",
          failure: %{
            title: "Run preparation failed",
            message: "Favn could not inspect a physical relation required by this run.",
            remediation: "Configure the DuckDB ADBC driver and retry.",
            code: "physical_inspection_unavailable"
          }
        }
      )

    assert html =~ "Run preparation failed"
    assert html =~ "physical_inspection_unavailable"
    assert html =~ "Configure the DuckDB ADBC driver"
    assert html =~ ~s(href="/runners")
  end

  test "renders queued work as durable instead of missing" do
    html =
      render_component(&Submission.submission_panel/1,
        run: %{raw_status: :queued, failure: nil}
      )

    assert html =~ "Run request queued"
    assert html =~ "request is durable"
    refute html =~ "Run not found"
  end

  test "renders cancelled work as terminal without promising execution" do
    html =
      render_component(&Submission.submission_panel/1,
        run: %{raw_status: :cancelled, failure: nil}
      )

    assert html =~ "Run request cancelled"
    assert html =~ "Execution did not start"
    refute html =~ "page will update when execution starts"
  end
end
