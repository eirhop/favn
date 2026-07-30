defmodule FavnView.Components.LogViewerTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias FavnView.Components.LogPages
  alias FavnView.Components.LogViewer
  alias FavnView.Dev.DesignSystem.Fixtures.Logs

  test "level is a tag as well as a colour, so colour is never the only signal" do
    html = render_component(&LogViewer.log_viewer/1, Logs.viewer_attrs())

    assert html =~ ~s(data-log-level="info")
    assert html =~ ~s(data-log-level="warning")
    assert html =~ ~s(data-log-level="error")
    assert html =~ "INF"
    assert html =~ "WRN"
    assert html =~ "ERR"
  end

  test "the error jump control appears exactly when the stream holds errors" do
    with_error = render_component(&LogViewer.log_viewer/1, Logs.viewer_attrs())

    assert with_error =~ ~s(data-testid="log-error-jump")
    assert with_error =~ "1 error"

    without_error =
      render_component(&LogViewer.log_viewer/1, Logs.viewer_attrs(%{logs: Logs.global()}))

    refute without_error =~ ~s(data-testid="log-error-jump")
  end

  test "a multi-line message keeps its line breaks" do
    html = render_component(&LogViewer.log_viewer/1, Logs.viewer_attrs(%{logs: Logs.sql_only()}))

    assert html =~ "SELECT customer_id, order_id, total_amount\nFROM raw.orders"
  end

  test "entry identity lives in the disclosure, not on the line" do
    html =
      render_component(
        &LogViewer.log_viewer/1,
        Logs.viewer_attrs(%{logs: Logs.for_run("run_2026_06_12")})
      )

    assert html =~ ~s(data-testid="log-details-panel")
    assert html =~ "run_2026_06_12"
    assert html =~ ~s(data-testid="log-row-copy-button")
  end

  test "loading, failed, and empty states each say what is happening" do
    for {status, marker} <- [
          {:loading, "log-loading-state"},
          {:error, "log-error-state"},
          {:ready, "log-empty-state"}
        ] do
      html =
        render_component(
          &LogViewer.log_viewer/1,
          Logs.viewer_attrs(%{logs: [], status: status})
        )

      assert html =~ ~s(data-testid="#{marker}")
    end
  end

  test "the step strip links every step to its own log view and names the failure" do
    html =
      render_component(&LogPages.step_strip/1, run_id: "run_2026_06_12", steps: Logs.run_steps())

    assert html =~ ~s(href="/runs/run_2026_06_12/assets/03/logs")
    assert length(Regex.scan(~r/data-testid="log-run-step"/, html)) == 3
    assert html =~ "customer_orders_daily"
    assert html =~ "Failed"
  end
end
