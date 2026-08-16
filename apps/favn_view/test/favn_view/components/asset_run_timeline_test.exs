defmodule FavnView.Components.AssetRunTimelineTest do
  @moduledoc """
  Covers the Runs sub-page: the spine that selects a run and the panel that explains
  one.

  The two halves are tested together because the contract between them is a URL, not
  a function call. The spine emits `patch` targets and the panel renders whatever
  `handle_params` loaded for the id in the address bar, so a test that exercised
  either alone would pass while the pair disagreed.
  """

  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias FavnView.Components.AssetDetailPage
  alias FavnView.UI.Data

  test "the spine names each day once and marks the run in the address bar" do
    html =
      render_component(&Data.run_timeline/1,
        runs: [
          run("run-c", "Aug 3", "10:25", :success),
          run("run-b", "Aug 3", "04:25", :success),
          run("run-a", "Aug 2", "10:25", :error)
        ],
        selected_id: "run-b"
      )

    # Three runs across two days, so the earlier day heading appears once rather
    # than on the row: a day with several runs has to read as one day.
    assert html |> String.split("Aug 3") |> length() == 2
    assert html =~ "Aug 2"

    assert html =~ ~s(aria-current="true")
    assert html =~ "/assets/orders/runs/run-b"
    assert html =~ "10:25"
  end

  test "the spine says so rather than rendering an empty rail" do
    html =
      render_component(&Data.run_timeline/1,
        runs: [],
        empty_label: "This asset has not run yet."
      )

    assert html =~ "This asset has not run yet."
  end

  test "a run panel leads with the outcome and compares the contract it was given" do
    html =
      render_component(&AssetDetailPage.run_detail_panel/1,
        timezone: "Etc/UTC",
        asset_id: "orders",
        run: %{
          run_id: "run-b",
          status: :ok,
          submit_kind: :schedule,
          started_at: ~U[2026-08-03 10:25:04Z],
          duration_ms: 4_210,
          window: %{label: "Aug 3", kind: :day, value: "2026-08-03"},
          error: nil,
          asset_result: %{
            status: :ok,
            stage: 0,
            started_at: ~U[2026-08-03 10:25:04Z],
            finished_at: ~U[2026-08-03 10:25:08Z],
            duration_ms: 4_210,
            attempt_count: 1,
            max_attempts: 3,
            error: nil,
            meta: %{"rows_written" => 1_284, "relation" => "mart.orders"}
          },
          assurance: %{
            quality_status: :passed,
            write_outcome: :written,
            latest_run_id: "run-b",
            contract_validation: nil,
            checks: [
              %{
                name: :orders_present,
                origin: :asset,
                claim_id: nil,
                phase: :after_materialize,
                when: nil,
                on_violation: :fail,
                message: nil,
                latest_result: %{outcome: :passed, metrics: %{actual: 1_284}}
              }
            ],
            contract: %{grain: nil, unique_keys: [], columns: [], row_counts: []}
          },
          runtime_inputs: []
        }
      )

    assert html =~ ~s(data-testid="asset-run-detail")
    assert html =~ "run-b"
    assert html =~ "Wrote 1,284 rows"
    assert html =~ "mart.orders"
    assert html =~ "1 of 3"
    assert html =~ "Aug 3"
    assert html =~ "orders_present"
    refute html =~ ~s(data-testid="asset-run-failure")
  end

  test "a failed run leads with the failure and a way out of the panel" do
    html =
      render_component(&AssetDetailPage.run_detail_panel/1,
        timezone: "Etc/UTC",
        asset_id: "orders",
        run: %{
          run_id: "run-a",
          status: :error,
          submit_kind: :manual,
          started_at: ~U[2026-08-02 10:25:04Z],
          duration_ms: 900,
          window: nil,
          error: %{message: "relation mart.orders does not exist"},
          asset_result: nil,
          assurance: nil,
          runtime_inputs: []
        }
      )

    assert html =~ ~s(data-testid="asset-run-failure")
    assert html =~ "relation mart.orders does not exist"
    assert html =~ "/runs/run-a"
    assert html =~ "Full refresh"
  end

  test "resolved inputs report which payload was selected and never its values" do
    html =
      render_component(&AssetDetailPage.run_detail_panel/1,
        timezone: "Etc/UTC",
        asset_id: "orders",
        run: %{
          run_id: "run-b",
          status: :ok,
          submit_kind: :pipeline,
          started_at: ~U[2026-08-03 10:25:04Z],
          duration_ms: 4_210,
          window: nil,
          error: nil,
          asset_result: nil,
          assurance: nil,
          runtime_inputs: [
            %{
              node_key: {{MyApp.Orders, :asset}, nil},
              resolver: MyApp.Resolvers.LandedFile,
              input_identity: "landing/orders/2026-08-03.csv",
              payload_fingerprint: String.duplicate("a", 64),
              source_run_id: "run-a",
              source_node_key: nil,
              source_payload_fingerprint: nil
            }
          ]
        }
      )

    assert html =~ ~s(data-testid="asset-run-inputs")
    assert html =~ "MyApp.Resolvers.LandedFile"
    assert html =~ "landing/orders/2026-08-03.csv"
    assert html =~ "/assets/orders/runs/run-a"
  end

  test "the rail patches between sub-pages and hides coverage for a full refresh" do
    windowed = AssetDetailPage.detail_modes("orders", true)
    full_refresh = AssetDetailPage.detail_modes("orders", false)

    assert Enum.map(windowed, & &1.id) == [:overview, :runs, :coverage, :docs, :diagnostics]

    # Coverage asks whether every expected period has data. A full-refresh asset
    # replaces its whole relation every run, so the question does not apply and the
    # destination is absent rather than empty. Every other destination is there for
    # both, because every asset has runs, documentation and a configuration.
    assert Enum.map(full_refresh, & &1.id) == [:overview, :runs, :docs, :diagnostics]

    # Every destination is a URL. The modes used to be an assign, so a refresh lost
    # the tab and the back button walked out of the asset entirely.
    assert Enum.all?(windowed, &is_binary(&1.patch))
    assert Enum.find(windowed, &(&1.id == :runs)).patch == "/assets/orders/runs"
  end

  defp run(id, day_label, time_label, tone) do
    %{
      id: id,
      patch: "/assets/orders/runs/#{id}",
      status: :ok,
      status_tone: tone,
      status_label: "Succeeded",
      trigger_label: "Schedule",
      started_at: ~U[2026-08-03 10:25:04Z],
      day_label: day_label,
      time_label: time_label,
      duration_label: "4.2s",
      # Deliberately unlike the day heading, so counting the heading counts headings.
      window_label: "day #{id}"
    }
  end
end
