defmodule FavnView.AssetCoverageTest do
  @moduledoc """
  Covers the coverage page: the calendar layout and the panel that acts on it.

  The two are tested together because the contract between them is the derived
  calendar, and a panel test with a hand-written calendar would pass while the
  derivation produced a different shape.
  """

  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias FavnView.Components.AssetDetailPage
  alias FavnView.CoverageCalendar
  alias FavnView.UI.Data

  describe "the calendar" do
    test "draws every examined day and marks only the missing ones" do
      calendar = day_calendar(~w(2026-07-08 2026-07-15))

      assert calendar.layout == :grid
      assert calendar.columns == 7
      assert calendar.column_labels == ~w(Mon Tue Wed Thu Fri Sat Sun)
      assert [july] = calendar.groups
      assert july.label == "July 2026"
      assert length(july.cells) == 22

      # 1 July 2026 is a Wednesday, so the month opens two columns in. Without the
      # padding the whole grid would be shifted and every weekday reading wrong.
      assert july.blanks == 2

      missing = Enum.filter(july.cells, &(&1.state == :missing))
      assert Enum.map(missing, & &1.label) == ~w(8 15)
      assert Enum.map(missing, & &1.key) == coverage_keys(~w(2026-07-08 2026-07-15))
      assert Enum.all?(missing, &(&1.selected? == false))
    end

    test "a period past the examined range is absent rather than drawn as covered" do
      # Twenty-two days examined out of a thirty-one day month, so the last nine are
      # unknown. Drawing them plain would claim they hold data.
      calendar = day_calendar([])
      assert [july] = calendar.groups
      assert Enum.map(july.cells, & &1.label) |> List.last() == "22"
    end

    test "marks the picked days and counts them" do
      calendar = day_calendar(~w(2026-07-08 2026-07-15), ["day:Europe/Oslo:2026-07-08"])

      assert calendar.selected_count == 1
      assert [july] = calendar.groups
      selected = Enum.filter(july.cells, & &1.selected?)
      assert Enum.map(selected, & &1.label) == ["8"]
    end

    test "a monthly asset gets months in a year and no weekday columns" do
      calendar =
        CoverageCalendar.build(%{
          examined: %{
            kind: :month,
            timezone: "Europe/Oslo",
            from: ~U[2025-11-01 00:00:00Z],
            through: ~U[2026-02-01 00:00:00Z],
            count: 4
          },
          gaps: [
            %{
              window_key: "month:Europe/Oslo:2025-12",
              kind: :month,
              start_at: ~U[2025-12-01 00:00:00Z]
            }
          ]
        })

      assert calendar.column_labels == []
      assert Enum.map(calendar.groups, & &1.label) == ["2025", "2026"]
      assert Enum.map(calendar.groups, & &1.blanks) == [10, 0]

      cells = Enum.flat_map(calendar.groups, & &1.cells)
      assert Enum.map(cells, & &1.label) == ~w(Nov Dec Jan Feb)
      assert Enum.map(cells, & &1.state) == [:covered, :missing, :covered, :covered]
    end

    test "hours are listed rather than gridded, grouped by the day they fall on" do
      calendar =
        CoverageCalendar.build(%{
          examined: %{
            kind: :hour,
            timezone: "Europe/Oslo",
            from: ~U[2026-07-08 00:00:00Z],
            through: ~U[2026-07-09 23:00:00Z],
            count: 48
          },
          gaps: [
            %{
              window_key: "hour:x:2026-07-09T04",
              kind: :hour,
              start_at: ~U[2026-07-09 04:00:00Z]
            },
            %{window_key: "hour:x:2026-07-08T03", kind: :hour, start_at: ~U[2026-07-08 03:00:00Z]}
          ]
        })

      # Stepping hour by hour across a daylight-saving boundary is timezone
      # arithmetic, and 48 hour cells is not a calendar. Only the gaps are listed.
      assert calendar.layout == :list

      assert Enum.map(calendar.groups, & &1.label) == [
               "Wednesday 8 July 2026",
               "Thursday 9 July 2026"
             ]

      assert Enum.map(calendar.groups, &Enum.map(&1.cells, fn cell -> cell.label end)) ==
               [["03:00"], ["04:00"]]
    end

    test "no examined range means no calendar at all" do
      assert CoverageCalendar.build(%{}).layout == :empty
      assert CoverageCalendar.build(%{}).groups == []
    end
  end

  describe "the calendar element" do
    test "missing days are buttons carrying their window key, covered days are not" do
      html =
        render_component(&Data.coverage_calendar/1,
          layout: :grid,
          columns: 7,
          column_labels: ~w(Mon Tue Wed Thu Fri Sat Sun),
          groups: day_calendar(~w(2026-07-08)).groups,
          on_select: "toggle_coverage_window"
        )

      assert html =~ ~s(phx-click="toggle_coverage_window")
      assert html =~ ~s(phx-value-key="day:Europe/Oslo:2026-07-08")
      assert html =~ "Wednesday 8 July 2026"

      # One button for the one missing day. A covered day is not a control, because
      # there is nothing to do with it.
      assert html |> String.split(~s(phx-click="toggle_coverage_window")) |> length() == 2
    end

    test "without an event nothing is clickable" do
      html =
        render_component(&Data.coverage_calendar/1,
          groups: day_calendar(~w(2026-07-08)).groups,
          column_labels: ~w(Mon Tue Wed Thu Fri Sat Sun)
        )

      refute html =~ "phx-click"
      assert html =~ "Wednesday 8 July 2026"
    end
  end

  describe "the coverage panel" do
    test "answers in one sentence and names the days it drew" do
      html =
        render_component(&AssetDetailPage.coverage_panel/1,
          command_resource: "asset:orders",
          can_plan?: true,
          coverage: %{
            status: :incomplete,
            expected_count: 22,
            covered_count: 20,
            missing_count: 2
          },
          calendar: day_calendar(~w(2026-07-08 2026-07-15))
        )

      assert html =~ ~s(data-testid="asset-coverage")
      assert html =~ "2 of 22 days have no data."
      assert html =~ "Showing 22 days, 1 July 2026 to 22 July 2026, in Europe/Oslo."
      assert html =~ "Backfill all 2 missing days"

      # The window key was the whole answer on the old screen. It survives only as the
      # click payload — once, in `phx-value-key` — and never as something to read.
      assert html |> String.split("day:Europe/Oslo:2026-07-08") |> length() == 2
      assert html =~ ~s(phx-value-key="day:Europe/Oslo:2026-07-08")
      assert html =~ "Wednesday 8 July 2026"
    end

    test "a selection replaces the offer to backfill everything" do
      html =
        render_component(&AssetDetailPage.coverage_panel/1,
          command_resource: "asset:orders",
          can_plan?: true,
          coverage: %{
            status: :incomplete,
            expected_count: 22,
            covered_count: 20,
            missing_count: 2
          },
          calendar: day_calendar(~w(2026-07-08 2026-07-15), ["day:Europe/Oslo:2026-07-08"])
        )

      assert html =~ "Backfill 1 selected day"
      assert html =~ ~s(data-testid="clear-coverage-selection")
      refute html =~ "Backfill all"
    end

    test "complete coverage offers no backfill and no pagination" do
      html =
        render_component(&AssetDetailPage.coverage_panel/1,
          command_resource: "asset:orders",
          can_plan?: true,
          coverage: %{
            status: :complete,
            expected_count: 22,
            covered_count: 22,
            missing_count: 0
          },
          calendar: day_calendar([])
        )

      assert html =~ "All 22 days that should hold data do."
      refute html =~ ~s(data-testid="plan-missing-coverage")
      refute html =~ ~s(data-testid="coverage-gap-pagination")
    end

    test "unknown coverage says why in plain words and draws no grid" do
      html =
        render_component(&AssetDetailPage.coverage_panel/1,
          command_resource: "asset:orders",
          coverage: %{status: :unknown, unknown_reason: :coverage_not_declared},
          calendar: CoverageCalendar.build(%{})
        )

      assert html =~ "Unknown"
      assert html =~ "does not say which periods it should cover"

      # An empty grid reads as a complete one, so there is no grid.
      refute html =~ ~s(data-testid="asset-coverage-calendar")
      refute html =~ ~s(data-testid="plan-missing-coverage")
    end

    test "a plan under review names each day in words before it can be submitted" do
      gaps = [
        %{
          window_key: "day:Europe/Oslo:2026-07-08",
          kind: :day,
          start_at: ~U[2026-07-08 00:00:00Z]
        }
      ]

      html =
        render_component(&AssetDetailPage.coverage_panel/1,
          command_resource: "asset:orders",
          can_plan?: true,
          coverage: %{
            status: :incomplete,
            expected_count: 22,
            covered_count: 21,
            missing_count: 1
          },
          calendar: day_calendar(~w(2026-07-08)),
          plan: %{plan_hash: String.duplicate("a", 64), window_count: 1, windows: gaps}
        )

      assert html =~ ~s(data-testid="coverage-plan-review")
      assert html =~ "Ready to backfill 1 day"
      assert html =~ "8 July 2026"
      assert html =~ ~s(data-testid="submit-missing-coverage")

      # The plan is the thing being confirmed, so the button that would build a
      # different one is gone rather than sitting beside it.
      refute html =~ ~s(data-testid="plan-missing-coverage")
    end

    test "a viewer sees the gaps but is told why they cannot fill them" do
      html =
        render_component(&AssetDetailPage.coverage_panel/1,
          command_resource: "asset:orders",
          can_plan?: false,
          coverage: %{
            status: :incomplete,
            expected_count: 22,
            covered_count: 20,
            missing_count: 2
          },
          calendar: day_calendar(~w(2026-07-08 2026-07-15))
        )

      assert html =~ "needs an operator account"
      assert html =~ ~s(disabled)
      refute html =~ ~s(phx-click="toggle_coverage_window")
    end

    test "a truncated page says later periods follow" do
      html =
        render_component(&AssetDetailPage.coverage_panel/1,
          command_resource: "asset:orders",
          coverage: %{
            status: :incomplete,
            expected_count: 400,
            covered_count: 398,
            missing_count: 2
          },
          calendar: day_calendar(~w(2026-07-08 2026-07-15)),
          pagination: %{limit: 366, has_more: true, next_cursor: "opaque-cursor"}
        )

      assert html =~ "Later periods are on the next page."
      assert html =~ ~s(data-testid="next-coverage-gap-page")
      assert html =~ ~s(data-testid="previous-coverage-gap-page")
    end
  end

  defp day_calendar(missing_dates, selected \\ []) do
    CoverageCalendar.build(%{
      examined: %{
        kind: :day,
        timezone: "Europe/Oslo",
        from: ~U[2026-07-01 00:00:00Z],
        through: ~U[2026-07-22 00:00:00Z],
        count: 22
      },
      gaps: Enum.map(missing_dates, &gap/1),
      selected: selected
    })
  end

  defp coverage_keys(dates), do: Enum.map(dates, &("day:Europe/Oslo:" <> &1))

  defp gap(date) do
    {:ok, start_at, _offset} = DateTime.from_iso8601(date <> "T00:00:00Z")

    %{
      window_key: "day:Europe/Oslo:" <> date,
      kind: :day,
      timezone: "Europe/Oslo",
      start_at: start_at,
      end_at: DateTime.add(start_at, 1, :day)
    }
  end
end
