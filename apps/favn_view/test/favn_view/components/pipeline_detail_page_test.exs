defmodule FavnView.Components.PipelineDetailPageTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest, only: [render_component: 2]

  alias FavnView.Components.PipelineDetailPage
  alias FavnView.PipelineRunConfig

  doctest FavnView.Components.PipelineRunDialog

  describe "the page" do
    test "states what the pipeline declares instead of repeating its name" do
      html = render(PipelineDetailPage.sample_pipeline())

      assert html =~ "Month · Etc/UTC"
      assert html =~ "The last complete month"
      assert html =~ "Include deps"
      assert html =~ ~s(data-testid="open-run-dialog")
    end

    test "an unwindowed pipeline says so and offers no period" do
      html = render(PipelineDetailPage.sample_unwindowed_pipeline())

      assert html =~ "Not windowed"
      assert html =~ "The whole relation"
      refute html =~ "Backfill"
    end

    test "a long selection collapses behind a disclosure" do
      assets = Enum.map(1..30, &"asset_#{&1}")

      html =
        render(%{
          PipelineDetailPage.sample_pipeline()
          | selected_assets: assets,
            asset_count: length(assets)
        })

      assert html =~ ~s(data-testid="pipeline-assets-disclosure")
      assert html =~ "Show all 30"
      assert html =~ "asset_30"
    end

    test "a pipeline that has never run says so rather than showing an empty table" do
      html = render(%{PipelineDetailPage.sample_pipeline() | runs: []})

      assert html =~ ~s(data-testid="pipeline-history-empty-state")
      assert html =~ "No runs yet"
      refute html =~ ~s(id="pipeline-runs-table")
    end

    test "run history renders as a table and as cards" do
      html = render(PipelineDetailPage.sample_pipeline())

      assert html =~ ~s(id="pipeline-runs-table")
      assert html =~ ~s(data-testid="pipeline-run-row")
      assert html =~ ~s(data-testid="pipeline-runs-card-list")
    end
  end

  describe "the run dialog" do
    test "opens on the declared configuration with nothing marked as changed" do
      html = render(PipelineDetailPage.sample_pipeline(), run_dialog_open?: true)

      assert html =~ ~s(data-testid="pipeline-run-dialog")
      assert html =~ "The last complete month, Etc/UTC"
      assert html =~ "2 assets, with what they read"
      assert html =~ "Obeyed — what is already current is skipped"
      assert html =~ "4 at a time, on the default pool"
      assert html =~ "Run the last complete month"
      refute html =~ ~s(data-testid="run-config-changed")
      refute html =~ ~s(data-testid="reset-run-config")
    end

    test "an unwindowed pipeline gets no period controls" do
      html =
        render(PipelineDetailPage.sample_unwindowed_pipeline(), run_dialog_open?: true)

      assert html =~ "The whole relation, every run"
      assert html =~ "Run pipeline"
      refute html =~ ~s(data-testid="pipeline-run-period")
      refute html =~ ~s(data-testid="pipeline-run-combine-windows")
    end

    test "a single period runs that window" do
      html = render_with_config(from: "2026-03")

      assert html =~ "Run 2026-03"
      assert html =~ ~s(data-testid="run-config-changed")
      assert html =~ ~s(data-testid="reset-run-config")
      refute html =~ ~s(data-testid="pipeline-run-combine-windows")
    end

    test "a range turns the submission into a backfill and offers combine windows" do
      html = render_with_config(from: "2026-01", to: "2026-08")

      assert html =~ "Backfill 2026-01 to 2026-08"
      assert html =~ ~s(data-testid="pipeline-run-combine-windows")
      assert html =~ "One child run per period"
      assert html =~ ~s(data-command-operation-present="pipeline_backfill_submit")
    end

    test "forcing states the blast radius" do
      html = render_with_config(from: "2026-01", to: "2026-08", refresh: "force_all")

      assert html =~ ~s(data-testid="pipeline-run-force-notice")
      assert html =~ "Forcing recomputes 2 assets for every month from 2026-01 to 2026-08"
    end

    test "a viewer is told why it cannot submit" do
      html =
        render(PipelineDetailPage.sample_pipeline(),
          run_dialog_open?: true,
          can_submit_runs?: false
        )

      assert html =~ ~s(data-testid="pipeline-run-not-permitted")
      assert html =~ "needs an operator account"
    end

    test "an invalid configuration is explained and cannot be submitted" do
      html =
        render(PipelineDetailPage.sample_pipeline(),
          run_dialog_open?: true,
          run_config_valid?: false,
          run_error: "A range needs a period to start from."
        )

      assert html =~ ~s(data-testid="pipeline-run-error")
      assert [button] = Regex.run(~r/<button[^>]*data-testid="submit-pipeline-run"[^>]*>/, html)
      assert button =~ "disabled"
    end
  end

  defp render_with_config(overrides) do
    pipeline = PipelineDetailPage.sample_pipeline()
    config = Enum.into(overrides, PipelineRunConfig.default(pipeline))

    render(pipeline, run_dialog_open?: true, run_config: config)
  end

  defp render(pipeline, overrides \\ []) do
    attrs =
      Enum.into(overrides, %{
        pipeline: pipeline,
        nav_items: [],
        run_config: PipelineRunConfig.default(pipeline),
        run_config_defaults: PipelineRunConfig.default(pipeline),
        can_submit_runs?: true,
        flash: %{}
      })

    render_component(&PipelineDetailPage.pipeline_detail_page/1, attrs)
  end
end
