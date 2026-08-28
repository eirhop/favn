defmodule FavnView.Components.RunDetailPage.WindowSemanticsTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias FavnView.Components.RunDetailPage
  alias FavnView.Components.RunDetailPage.WindowRail
  alias FavnView.Dev.DesignSystem.Fixtures.Runs
  alias FavnView.RunWindowRail

  defp render_page(run, opts \\ []) do
    render_component(
      &RunDetailPage.run_detail_page/1,
      Keyword.merge(
        [
          run: run,
          run_id: run.id,
          current_scope: %{workspace_configuration: %{default_timezone: "Etc/UTC"}}
        ],
        opts
      )
    )
  end

  test "Flow draws one exact run as lanes on its own axis" do
    html = render_page(Runs.single_window())

    assert html =~ ~s(data-testid="asset-progress")
    assert html =~ ~s(data-testid="run-flow")
    assert html =~ ~s(data-testid="run-timeline")
    assert html =~ ~s(data-testid="run-timeline-bar")
    assert html =~ "Orders"

    # The chart is the default reading, so the table is not also rendered.
    refute html =~ ~s(data-testid="run-asset-row")
    refute html =~ "Output metadata"
    refute html =~ ~s(data-testid="window-progress")
  end

  test "Flow keeps the lean asset list one click away" do
    html = render_page(Runs.single_window(), flow_view: :table)

    assert html =~ ~s(data-testid="run-asset-row")
    assert html =~ "Orders"
    assert html =~ "Started"
    assert html =~ "Finished"
    refute html =~ ~s(data-testid="run-timeline")
  end

  test "a run outside a backfill shows no rail and no window controls" do
    html = render_page(Runs.single_window())

    refute html =~ ~s(data-testid="window-rail")
    refute html =~ ~s(data-testid="load-run-windows")
    refute html =~ ~s(data-testid="run-window-selector")
  end

  test "sibling window runs render as one selectable calendar rail" do
    run = Runs.single_window()

    rail =
      rail(
        [
          window("run_daily_orders_2026_07_22", ~U[2026-07-22 00:00:00Z]),
          window(run.id, ~U[2026-07-23 00:00:00Z]),
          window("run_daily_orders_2026_07_24", ~U[2026-07-24 00:00:00Z])
        ],
        run.id
      )

    html = render_page(run, rail: rail)

    assert html =~ ~s(data-testid="window-rail")
    assert count(html, ~s(data-testid="window-rail-cell")) == 3

    # Every cell is selectable, including the one already open: the rail is a
    # calendar, not a list of somewhere-else links.
    assert count(html, ~s(phx-click="select_window")) == 3
    assert html =~ ~s(aria-current="true")
    refute html =~ ~s(data-testid="window-rail-buckets")
  end

  test "the rail says the set is still growing while the backfill runs" do
    run = Runs.single_window()
    windows = [window(run.id, ~U[2026-07-23 00:00:00Z])]

    running = render_page(run, rail: rail(windows, run.id, backfill_status: :running))
    assert running =~ ~s(data-testid="window-rail-in-progress")

    completed = render_page(run, rail: rail(windows, run.id, backfill_status: :completed))
    refute completed =~ ~s(data-testid="window-rail-in-progress")
  end

  test "a truncated window read says so without claiming a total" do
    run = Runs.single_window()
    windows = [window(run.id, ~U[2026-07-23 00:00:00Z])]

    html = render_page(run, rail: rail(windows, run.id, truncated?: true))

    assert html =~ ~s(data-testid="window-rail-truncated")
    assert html =~ "older windows exist"
  end

  test "a bounded Flow slice is named precisely" do
    run = Map.put(Runs.single_window(), :asset_attempts_truncated?, true)
    html = render_page(run)

    assert html =~ "run-detail-truncated-warning"
    assert html =~ "first 1,000 in stable order"
  end

  test "a backfill parent explains its grouping role instead of claiming there is no work" do
    run =
      Runs.single_window()
      |> Map.merge(%{
        backfill_parent?: true,
        window: nil,
        assets: [],
        total_windows: 3,
        completed_windows: 2,
        failed_windows: 1,
        total_asset_attempts: 3,
        completed_asset_attempts: 2,
        succeeded_asset_attempts: 1,
        failed_asset_attempts: 1,
        running_asset_attempts: 1,
        queued_asset_attempts: 0,
        planned_asset_attempts: 0
      })

    # Three windows that produced runs, so the rail can offer them and the
    # explanation is the true thing to say.
    children = [
      window("run-child-one", ~U[2026-07-01 00:00:00Z]),
      window("run-child-two", ~U[2026-07-02 00:00:00Z]),
      window("run-child-three", ~U[2026-07-03 00:00:00Z])
    ]

    html = render_page(run, rail: rail(children, run.id))

    assert html =~ ~s(data-testid="backfill-parent-explanation")
    assert html =~ ~s(data-testid="window-progress")
    assert html =~ "Asset work runs in the windows"
    refute html =~ "No asset work yet"
    refute html =~ ~s(data-testid="backfill-parent-no-window-runs")
  end

  test "a backfill parent whose windows produced no run says so instead of offering one" do
    # Every window failed before a run existed, so there is nothing to open. The
    # page used to instruct the operator to open a window run anyway, with an
    # empty rail and an empty chart under it.
    run =
      Runs.single_window()
      |> Map.merge(%{
        backfill_parent?: true,
        window: nil,
        assets: [],
        active?: false,
        total_windows: 31,
        completed_windows: 0,
        failed_windows: 31
      })

    html = render_page(run, rail: nil)

    assert html =~ ~s(data-testid="backfill-parent-no-window-runs")
    assert html =~ "None of the 31 windows produced a run"
    refute html =~ ~s(data-testid="backfill-parent-explanation")

    # The chart's empty state said the same thing a second time, in the largest
    # type on the screen.
    assert html =~ ~s(data-testid="backfill-parent-no-work")
    refute html =~ "Open a window run to inspect"
  end

  test "a flat rail names the period its bare cell labels sit inside" do
    run = Map.merge(Runs.single_window(), %{backfill_parent?: true, window: nil})

    children = [
      window("run-child-one", ~U[2026-03-01 00:00:00Z]),
      window("run-child-two", ~U[2026-03-02 00:00:00Z]),
      window("run-child-three", ~U[2026-03-03 00:00:00Z])
    ]

    html = render_page(run, rail: rail(children, run.id))

    # A day cell is labelled by its day number alone, so three March windows read
    # "1 2 3" and a flat rail has no band header to say which month. The coverage
    # is named by the days it contains, so the exclusive Mar 4 bound — a day the
    # rail holds no cell for — is never printed.
    assert html =~ "Covering Mar 1, 2026 – Mar 3, 2026"
  end

  test "a banded rail states the period in its bands rather than twice" do
    run = Map.merge(Runs.single_window(), %{backfill_parent?: true, window: nil})

    html = render_page(run, rail: Runs.rail(:banded))

    assert html =~ ~s(data-testid="window-rail-buckets")
    refute html =~ "Covering"
  end

  describe "why the windows failed" do
    defp failing_parent(failed \\ 31) do
      Runs.single_window()
      |> Map.merge(%{
        backfill_parent?: true,
        window: nil,
        assets: [],
        active?: false,
        total_windows: failed,
        completed_windows: 0,
        failed_windows: failed
      })
    end

    defp group(opts) do
      %FavnView.WindowFailures.Group{
        reason: Keyword.get(opts, :reason, "invalid_backfill_pipeline_identity"),
        detail: Keyword.get(opts, :detail),
        window_count: Keyword.get(opts, :window_count, 31),
        run_count: Keyword.get(opts, :run_count, 0),
        span: Keyword.get(opts, :span, "Jan 1 00:00 – Feb 1 00:00, 2026"),
        first_window: Keyword.get(opts, :first_window),
        attempts: Keyword.get(opts, :attempts, 1),
        run_ids: Keyword.get(opts, :run_ids, [])
      }
    end

    test "the reason a backfill's windows failed is on the page, once per reason" do
      html = render_page(failing_parent(), rail: nil, window_failures: [group([])])

      assert html =~ ~s(data-testid="window-failures")
      assert html =~ "invalid_backfill_pipeline_identity"
      assert html =~ "31 windows"
      assert html =~ "Covering Jan 1 00:00 – Feb 1 00:00, 2026"
      assert count(html, ~s(data-testid="window-failure-row")) == 1
      assert html =~ ~s(data-testid="window-failure-no-runs")
    end

    test "the panel counts reasons, not windows, so a shared cause reads as one finding" do
      groups = [
        group(reason: "no_runner", window_count: 20),
        group(reason: "timed_out", window_count: 11)
      ]

      html = render_page(failing_parent(), rail: nil, window_failures: groups)

      assert count(html, ~s(data-testid="window-failure-row")) == 2
      assert html =~ "2 reasons"
      assert html =~ "31 windows failed"
    end

    test "a reason whose windows did start runs links to them" do
      groups = [group(run_count: 2, run_ids: ["run_abcdef012345678", "run_b"])]

      html = render_page(failing_parent(), rail: nil, window_failures: groups)

      assert html =~ ~s(data-testid="window-failure-runs")
      assert html =~ "2 of these started a run"
      assert html =~ ~s(href="/runs/run_abcdef012345678")

      # The link text is shortened; the full id is what the href carries.
      assert html =~ "run_abcdef01234"
      refute html =~ ~s(data-testid="window-failure-no-runs")

      # Two runs and two links leaves nothing unsaid.
      refute html =~ ~s(data-testid="window-failure-more-runs")
    end

    test "a group with more runs than links says how many it is not showing" do
      groups = [group(run_count: 9, run_ids: ["run_a", "run_b", "run_c"])]

      html = render_page(failing_parent(), rail: nil, window_failures: groups)

      assert html =~ ~s(data-testid="window-failure-more-runs")
      assert html =~ "and 6 more"
    end

    test "a message that adds to the reason is shown, and a repeat of it is not" do
      with_detail =
        render_page(failing_parent(),
          rail: nil,
          window_failures: [group(detail: "the runner pool was empty")]
        )

      assert with_detail =~ ~s(data-testid="window-failure-detail")
      assert with_detail =~ "the runner pool was empty"

      without = render_page(failing_parent(), rail: nil, window_failures: [group([])])
      refute without =~ ~s(data-testid="window-failure-detail")
    end

    test "a group of one names its window rather than a span" do
      groups = [group(window_count: 1, span: nil, first_window: "Jan 4, 2026")]

      html = render_page(failing_parent(1), rail: nil, window_failures: groups)

      assert html =~ ~s(data-testid="window-failure-window")
      assert html =~ "Jan 4, 2026"
      assert html =~ "1 window"
      refute html =~ ~s(data-testid="window-failure-span")
    end

    test "a bounded read counts what it read, never the backfill's own total" do
      # 900 windows failed; the read returned 500. Claiming the reasons cover the
      # earliest 900 would say they account for 400 windows nothing read.
      html =
        render_page(failing_parent(900),
          rail: nil,
          window_failures: [group(window_count: 500)],
          window_failures_overflow?: true
        )

      assert html =~ ~s(data-testid="window-failures-truncated")
      assert html =~ "the earliest 500 of 900 failed windows"
      assert html =~ "later windows were not read"
      refute html =~ "earliest 900"

      # The subtitle must not restate the read count as the failure count: 900
      # failed, and the meter above says so.
      refute html =~ "500 windows failed."
      assert html =~ "Grouped by the reason each window recorded."
    end

    test "the partial notice states no total it cannot stand behind" do
      # A truncated read whose failed-window count is not larger than what it
      # returned has no second number to report, so it reports one.
      html =
        render_page(failing_parent(0),
          rail: nil,
          window_failures: [group(window_count: 500)],
          window_failures_overflow?: true
        )

      assert html =~ "the earliest 500 failed windows"
      refute html =~ "of 0"
    end

    test "a truncation notice needs rows to be about, not just a marker" do
      # A truncated read whose next cycle failed drops its rows. The marker can
      # still be set, and a notice trusting it alone would read "the earliest 0
      # of 900 failed windows" above nothing at all.
      html =
        render_page(failing_parent(900),
          rail: nil,
          window_failures: nil,
          window_failures_overflow?: true,
          window_failures_error: "Why these windows failed could not be loaded."
        )

      refute html =~ ~s(data-testid="window-failures-truncated")
      refute html =~ "earliest 0"
      assert html =~ ~s(data-testid="window-failures-error")
    end

    test "a failed ledger read states that rather than implying nothing was recorded" do
      html =
        render_page(failing_parent(),
          rail: nil,
          window_failures: nil,
          window_failures_error: "Why these windows failed could not be loaded."
        )

      assert html =~ ~s(data-testid="window-failures")
      assert html =~ ~s(data-testid="window-failures-error")
      assert html =~ "could not be loaded"
      refute html =~ ~s(data-testid="window-failure-row")
    end

    test "the panel stays off a run that is not a backfill parent" do
      html = render_page(Runs.single_window(), window_failures: [group([])])

      refute html =~ ~s(data-testid="window-failures")
    end

    test "the panel stays off a backfill parent whose windows all succeeded" do
      run = Map.merge(Runs.single_window(), %{backfill_parent?: true, window: nil, assets: []})

      html = render_page(run, rail: nil, window_failures: [])

      refute html =~ ~s(data-testid="window-failures")
    end
  end

  test "a failed window read is not reported as a backfill that produced no run" do
    # Every window ran and its run failed, so all 31 have a page to open. One
    # window read then times out, which also leaves no rail — a different fact
    # the page must not confuse with "nothing was produced".
    run =
      Runs.single_window()
      |> Map.merge(%{
        backfill_parent?: true,
        window: nil,
        assets: [],
        active?: false,
        total_windows: 31,
        completed_windows: 31,
        failed_windows: 31
      })

    html =
      render_page(run,
        rail: nil,
        windows_error: "Window runs could not be loaded. The page will try again."
      )

    refute html =~ ~s(data-testid="backfill-parent-no-window-runs")
    refute html =~ "None of the 31 windows produced a run"

    # It keeps the claim that is true of every backfill parent, and the read
    # warning says why the list is missing.
    assert html =~ ~s(data-testid="backfill-parent-explanation")
    assert html =~ ~s(data-testid="window-read-warning")

    # The chart must not claim it either.
    refute html =~ ~s(data-testid="backfill-parent-no-work")
  end

  test "a backfill still creating windows is not reported as having produced none" do
    run =
      Runs.single_window()
      |> Map.merge(%{backfill_parent?: true, window: nil, assets: [], active?: true})

    html = render_page(run, rail: nil)

    assert html =~ ~s(data-testid="backfill-parent-explanation")
    refute html =~ ~s(data-testid="backfill-parent-no-window-runs")
  end

  test "the progress meter names what it counts under the pointer" do
    run =
      Runs.single_window()
      |> Map.merge(%{
        backfill_parent?: true,
        window: nil,
        assets: [],
        total_windows: 31,
        failed_windows: 31
      })

    html = render_page(run, rail: nil)

    # A bar that is entirely one tone is exactly where the legend below it is
    # least likely to be the thing the pointer is on.
    assert html =~ ~s(title="31 failed")
  end

  test "a backfill parent's rail offers its children with none of them current" do
    run = Map.merge(Runs.single_window(), %{backfill_parent?: true, window: nil})

    children = [
      window("run-child-one", ~U[2026-07-01 00:00:00Z]),
      window("run-child-two", ~U[2026-08-01 00:00:00Z])
    ]

    html = render_page(run, rail: rail(children, run.id))

    assert count(html, ~s(data-testid="window-rail-cell")) == 2
    assert html =~ ~s(phx-value-run_id="run-child-one")
    assert html =~ ~s(phx-value-run_id="run-child-two")

    # The parent is not one of its own windows, so no cell is current.
    refute html =~ ~s(aria-current="true")
  end

  test "the rail offers a comparison and picks windows instead of opening them" do
    run = Runs.single_window()
    windows = compare_windows(run.id)

    off = render_page(run, rail: rail(windows, run.id))
    assert off =~ ~s(data-testid="window-rail-compare-toggle")
    assert off =~ ~s(aria-pressed="false")
    assert count(off, ~s(phx-click="select_window")) == 3
    refute off =~ ~s(phx-click="toggle_compare_window")

    on =
      render_page(run,
        rail: rail(windows, run.id, compare_run_ids: [run.id, "run-later"]),
        compare?: true
      )

    # In compare mode a cell picks a window to draw, so nothing in the rail
    # navigates and every cell reports whether it is in the comparison.
    assert count(on, ~s(phx-click="toggle_compare_window")) == 3
    refute on =~ ~s(phx-click="select_window")
    assert count(on, ~s(data-compared="true")) == 2
    assert count(on, ~s(data-compared="false")) == 1
    assert on =~ ~s(data-track="1")
    assert on =~ ~s(data-track="2")
  end

  test "the rail explains a full comparison rather than silently ignoring a click" do
    run = Runs.single_window()
    windows = compare_windows(run.id)

    html =
      render_page(run,
        rail: rail(windows, run.id),
        compare?: true,
        compare_limit_reached?: true
      )

    assert html =~ ~s(data-testid="window-rail-compare-limit")
    assert html =~ "at most #{RunWindowRail.compare_limit()} windows"

    # The refusal is a response to a click, not a standing part of the rail.
    refute render_page(run, rail: rail(windows, run.id), compare?: true) =~
             ~s(data-testid="window-rail-compare-limit")
  end

  test "a comparison takes the chart's place rather than sitting beside it" do
    run = Runs.single_window()
    windows = compare_windows(run.id)

    html =
      render_page(Map.put(run, :comparison, Runs.comparison()),
        rail: rail(windows, run.id, compare_run_ids: [run.id, "run-later"]),
        compare?: true
      )

    assert html =~ ~s(data-testid="run-comparison")
    assert html =~ ~s(data-testid="run-comparison-track")

    # One chart at a time: the single-run timeline and its filters belong to a
    # single window and would narrow only one track of the comparison.
    refute html =~ ~s(data-testid="run-timeline")
    refute html =~ ~s(data-testid="run-flow-controls")
  end

  test "the rail is one surface whose cells light up inside it" do
    run = Runs.single_window()

    # Rendered alone, because the page also carries a mode rail built from the
    # same element and counting both would say nothing about this one.
    html =
      render_component(&WindowRail.window_rail/1, rail: rail(compare_windows(run.id), run.id))

    # A window run is a position on a calendar, so the calendar is the object on
    # screen. Cells that each carried their own border and status fill read as
    # three separate controls and drowned the one that was selected.
    assert html =~ ~s(favn-surface-rail)
    assert count(html, "favn-mode-item h-9") == 3
    assert count(html, "favn-mode-item-active") == 1
    refute html =~ "rounded-md border px-2 py-1 text-center"

    # Status still shows, on a dot, so six statuses cannot fight the selected
    # state for the same pixels.
    assert count(html, ~s(class="status status-xs)) == 3
  end

  test "a compared window is numbered, and the number is what the chart repeats" do
    run = Runs.single_window()

    html =
      render_page(Map.put(run, :comparison, Runs.comparison()),
        rail: rail(compare_windows(run.id), run.id, compare_run_ids: [run.id, "run-later"]),
        compare?: true
      )

    # The rail is the only list of compared windows: it numbers the two it
    # picked, and the chart repeats those numbers on every track row. A second
    # copy of the list inside the chart said nothing the rail did not.
    assert count(html, ~s(data-compared="true")) == 2
    assert count(html, ~s(class="favn-track-index")) == 2
    refute html =~ ~s(data-testid="run-comparison-legend")
    assert html =~ ~s(class="favn-comparison-index favn-text-subtle")

    # "T1" named a track in a vocabulary the page never introduced.
    refute html =~ "T1"
  end

  test "a combined run states its span in its header instead of offering a rail" do
    run = Runs.single_window()

    combined =
      Enum.map(0..4, fn index ->
        window("run-combined", DateTime.add(~U[2026-07-10 00:00:00Z], index, :day))
      end)

    rail = rail(combined, "run-combined", backfill_status: :completed)

    html =
      render_page(
        Map.put(run, :combined_window, %{
          label: "Jul 10, 2026 – Jul 14, 2026",
          window_count: 5,
          kind: :day
        }),
        rail: rail
      )

    # One run covering five windows has one place to navigate to, and it is this
    # page. A rail of one cell is not a calendar, so the span is a property of
    # the run and sits with the run's other properties.
    refute html =~ ~s(data-testid="window-rail")
    assert html =~ "Combined window"

    # Counted in the unit the windows were planned in: "5 days" can be checked
    # against the span beside it, and "5 windows" cannot.
    assert html =~ "Jul 10, 2026 – Jul 14, 2026 · 5 days"
  end

  test "a combined run of one window still counts in that window's unit" do
    run = Runs.single_window()

    html =
      render_page(
        Map.put(run, :combined_window, %{
          label: "Jul 2026",
          window_count: 1,
          kind: :month
        })
      )

    assert html =~ "Jul 2026 · 1 month"
  end

  test "a combined run whose windows have no decodable unit still says how many" do
    run = Runs.single_window()

    html =
      render_page(
        Map.put(run, :combined_window, %{
          label: "Jul 10 09:30 – Jul 14 11:15, 2026",
          window_count: 5,
          kind: nil
        })
      )

    # A window key too old to decode a kind leaves nothing truer to count in than
    # windows, which is what the page falls back to rather than guessing a unit.
    assert html =~ "· 5 windows"
  end

  test "a window's exact bounds are stated on the operator's clock" do
    run = Runs.single_window()

    # December keyed in UTC, read by an operator whose display timezone is Oslo:
    # the cell is labelled "Dec" because that is the month the window covers, and
    # its bounds are 01:00 to 01:00 because that is when they happen in Oslo.
    december = [
      %{
        run_id: "run-december",
        window_start_at: ~U[2024-12-01 00:00:00Z],
        window_end_at: ~U[2025-01-01 00:00:00Z],
        status: :succeeded,
        kind: :month,
        timezone: "Etc/UTC"
      }
    ]

    html =
      render_page(run,
        rail:
          RunWindowRail.build(december, "run-december", "Europe/Oslo",
            backfill_status: :completed
          )
      )

    assert html =~ "Dec 1, 2024 01:00:00 CET – Jan 1, 2025 01:00:00 CET"

    # Two clocks in one tooltip is a contradiction unless the second one is
    # named, so the zone the window was keyed in is stated beside the bounds.
    assert html =~ "Window timezone Etc/UTC"
  end

  test "a window keyed in the operator's own timezone says nothing about zones" do
    run = Runs.single_window()

    html =
      render_page(run,
        rail: RunWindowRail.build(compare_windows(run.id), run.id, "Etc/UTC")
      )

    refute html =~ "Window timezone"
  end

  test "two names for one clock are not reported as a difference" do
    run = Runs.single_window()

    # A workspace may spell its default timezone "UTC" while the window policy
    # spells it "Etc/UTC". Nothing on screen disagrees, so there is nothing to
    # explain, and a note here would invent a discrepancy to worry about.
    html =
      render_page(run,
        rail: RunWindowRail.build(compare_windows(run.id), run.id, "UTC")
      )

    refute html =~ "Window timezone"
  end

  test "a panel's title keeps its inset even when its body owns the spacing" do
    run = Runs.single_window()
    html = render_page(run, rail: rail(compare_windows(run.id), run.id))

    # `padding={:none}` says the body owns its spacing — a chart that wants the
    # width, a table with its own cells. It never meant the heading should sit
    # on the card's border.
    assert html =~ ~s(px-5 pt-5 sm:px-6 sm:pt-6)
  end

  test "compare is offered only where the chart it draws exists" do
    run = Runs.single_window()
    html = render_page(run, rail: rail(compare_windows(run.id), run.id))

    # Below `lg` the page shows the card list, so the toggle would enter a mode
    # that changes nothing. A comparison already open says where its chart went
    # rather than showing this window's rows as if they were the comparison.
    assert html =~ ~s(hidden lg:inline-flex)

    comparing =
      render_page(Map.put(run, :comparison, Runs.comparison()),
        rail: rail(compare_windows(run.id), run.id, compare_run_ids: [run.id, "run-later"]),
        compare?: true
      )

    assert comparing =~ ~s(data-testid="run-comparison-narrow")
    assert comparing =~ "needs a wider screen"
  end

  test "a failed window read is stated on the page, not just absent from it" do
    run = Runs.single_window()

    html =
      render_page(run,
        rail: nil,
        windows_error: "Window runs could not be loaded. The page will try again."
      )

    assert html =~ ~s(data-testid="window-read-warning")
    assert html =~ "could not be loaded"

    # The rail is gone and the rest of the run page is untouched.
    refute html =~ ~s(data-testid="window-rail")
    assert html =~ ~s(data-testid="run-flow")
  end

  test "a comparison that lost every window says so where the chart was" do
    run = Runs.single_window()

    html =
      render_page(run,
        compare_error: "No compared window could be read. Showing this window on its own."
      )

    assert html =~ ~s(data-testid="compare-warning")
    assert html =~ "Showing this window on its own"

    # It has fallen back, so the single-run chart is what is drawn.
    assert html =~ ~s(data-testid="run-timeline")
    refute html =~ ~s(data-testid="run-comparison")
  end

  defp compare_windows(run_id) do
    [
      window("run-earlier", ~U[2026-07-22 00:00:00Z]),
      window(run_id, ~U[2026-07-23 00:00:00Z]),
      window("run-later", ~U[2026-07-24 00:00:00Z])
    ]
  end

  defp rail(windows, selected_run_id, opts \\ []) do
    RunWindowRail.build(windows, selected_run_id, "Etc/UTC", opts)
  end

  defp window(run_id, start_at, opts \\ []) do
    %{
      run_id: run_id,
      window_start_at: start_at,
      window_end_at: DateTime.add(start_at, 1, :day),
      status: Keyword.get(opts, :status, :succeeded),
      kind: :day,
      timezone: "Etc/UTC"
    }
  end

  defp count(html, fragment), do: html |> String.split(fragment) |> length() |> Kernel.-(1)
end
