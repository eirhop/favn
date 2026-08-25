defmodule FavnView.Components.PipelineDetailPageTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest, only: [render_component: 2]

  alias FavnView.Components.PipelineDetailPage

  test "a windowed pipeline keeps latest-window run available" do
    pipeline =
      PipelineDetailPage.sample_pipeline()
      |> Map.put(:window, %{kind: "month", timezone: "Etc/UTC", allow_full_load: false})

    html =
      render_component(&PipelineDetailPage.pipeline_detail_page/1,
        pipeline: pipeline,
        nav_items: [],
        backfill_config: %{
          from: "",
          to: "",
          kind: "month",
          timezone: "Etc/UTC",
          refresh: "missing",
          combine_windows: false
        },
        can_submit_runs?: true,
        flash: %{}
      )

    assert html =~ ~s(data-testid="pipeline-latest-window-help")
    assert html =~ "latest complete window"
    assert [button] = Regex.run(~r/<button[^>]*data-testid="run-pipeline-button"[^>]*>/, html)
    refute button =~ "disabled"
  end

  test "a windowed pipeline that allows full loads still explains latest-window runs" do
    pipeline =
      PipelineDetailPage.sample_pipeline()
      |> Map.put(:can_run_without_window?, true)
      |> Map.put(:window, %{kind: "month", timezone: "Etc/UTC", allow_full_load: true})

    html =
      render_component(&PipelineDetailPage.pipeline_detail_page/1,
        pipeline: pipeline,
        nav_items: [],
        backfill_config: %{
          from: "",
          to: "",
          kind: "month",
          timezone: "Etc/UTC",
          refresh: "missing",
          combine_windows: false
        },
        can_submit_runs?: true,
        flash: %{}
      )

    assert html =~ ~s(data-testid="pipeline-latest-window-help")
    assert html =~ "latest complete window"
  end
end
