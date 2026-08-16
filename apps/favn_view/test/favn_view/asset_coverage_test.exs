defmodule FavnView.AssetCoverageTest do
  @moduledoc """
  Covers the coverage page: the calendar layout, the navigator, and the panel.

  They are tested together because the contract between them is the derived calendar,
  and a panel test with a hand-written one would pass while the derivation produced a
  different shape.
  """

  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias FavnView.CoverageCalendar, as: Subject

  doctest Subject, import: true

  alias FavnView.Components.AssetDetailPage
  alias FavnView.UI.Data

  defmodule CoverageCalendar do
    defdelegate build(attrs), to: FavnView.CoverageCalendar
    defdelegate unit_bounds(kind, date), to: FavnView.CoverageCalendar

    def navigation(attrs),
      do: FavnView.CoverageCalendar.navigation(Map.put_new(attrs, :timezone, "Etc/UTC"))

    def opening_date(attrs),
      do: FavnView.CoverageCalendar.opening_date(Map.put_new(attrs, :timezone, "Etc/UTC"))

    def jump_target(attrs, params),
      do:
        FavnView.CoverageCalendar.jump_target(
          Map.put_new(attrs, :timezone, "Etc/UTC"),
          params
        )
  end

  describe "the shape of one screen" do
    test "a daily asset shows one month, aligned to its weekdays" do
      calendar = day_calendar([8, 15])

      assert calendar.layout == :grid
      assert calendar.columns == 7
      assert calendar.column_labels == ~w(Mon Tue Wed Thu Fri Sat Sun)
      assert calendar.unit_label == "July 2026"
      assert calendar.period_count == 31

      # 1 July 2026 is a Wednesday, so the month opens two columns in. Without the
      # padding every weekday on screen would be wrong.
      assert calendar.blanks == 2

      missing = Enum.filter(calendar.cells, &(&1.state == :missing))
      assert Enum.map(missing, & &1.label) == ~w(8 15)
      assert Enum.map(missing, & &1.key) == window_keys([8, 15])
    end

    test "an hourly asset shows one day of hours, however many the day has" do
      # Twenty-three, because the clock went forward. The view is told the hours rather
      # than counting to 24, which is the whole reason it is told them.
      windows =
        Enum.zip([0, 1, 3, 4, 5], [23, 0, 1, 2, 3])
        |> Enum.map(fn {local_hour, utc_hour} ->
          date = if utc_hour == 23, do: ~D[2026-03-28], else: ~D[2026-03-29]

          window(
            :hour,
            "hour:Europe/Oslo:2026-03-29T#{pad(local_hour)}",
            DateTime.new!(date, Time.new!(utc_hour, 0, 0), "Etc/UTC"),
            local_hour != 4
          )
        end)

      calendar = CoverageCalendar.build(%{kind: :hour, timezone: "Europe/Oslo", windows: windows})

      assert calendar.unit_label == "Sunday 29 March 2026"
      assert calendar.column_labels == []
      assert calendar.blanks == 0
      assert calendar.period_count == 5
      assert Enum.map(calendar.cells, & &1.label) == ~w(00:00 01:00 03:00 04:00 05:00)
      assert Enum.map(calendar.cells, & &1.state) |> Enum.count(&(&1 == :missing)) == 1
    end

    test "a monthly asset shows one year of months, with no weekday columns" do
      windows =
        Enum.map(1..12, fn month ->
          window(:month, "month:Europe/Oslo:2026-#{pad(month)}", month_at(month), month != 2)
        end)

      calendar =
        CoverageCalendar.build(%{kind: :month, timezone: "Europe/Oslo", windows: windows})

      assert calendar.unit_label == "2026"
      assert calendar.column_labels == []
      assert Enum.map(calendar.cells, & &1.label) |> Enum.take(3) == ~w(Jan Feb Mar)
      assert Enum.at(calendar.cells, 1).state == :missing
    end

    test "a yearly asset shows every year at once and names the span" do
      windows =
        Enum.map(2024..2026, fn year ->
          window(:year, "year:Europe/Oslo:#{year}", year_at(year), year != 2025)
        end)

      calendar = CoverageCalendar.build(%{kind: :year, timezone: "Europe/Oslo", windows: windows})

      assert calendar.unit_label == "2024 to 2026"
      assert Enum.map(calendar.cells, & &1.label) == ~w(2024 2025 2026)
    end

    test "a covered period carries no key, because there is nothing to select" do
      calendar = day_calendar([8])
      covered = Enum.find(calendar.cells, &(&1.state == :covered))

      assert covered.key == nil
      assert covered.selected? == false
    end

    test "no windows means no calendar at all" do
      assert CoverageCalendar.build(%{}).layout == :empty
      assert CoverageCalendar.build(%{}).cells == []
      assert CoverageCalendar.build(%{}).unit_label == nil
    end
  end

  describe "where the navigator can go" do
    test "a daily asset steps by month and jumps by year and month" do
      navigation =
        CoverageCalendar.navigation(%{
          kind: :day,
          at: ~U[2026-07-01 00:00:00Z],
          first_expected_at: ~U[2025-11-14 00:00:00Z],
          last_expected_at: ~U[2026-09-30 00:00:00Z]
        })

      assert navigation.previous == "2026-06-01"
      assert navigation.next == "2026-08-01"
      assert Enum.map(navigation.jumps, & &1.name) == ~w(year month)

      year = Enum.find(navigation.jumps, &(&1.name == "year"))
      assert Enum.map(year.options, &elem(&1, 1)) == ~w(2025 2026)

      # Coverage runs to September, so December is offered for 2026 only because the
      # month select is scoped to the selected year. Here that year ends in September.
      month = Enum.find(navigation.jumps, &(&1.name == "month"))
      assert Enum.map(month.options, &elem(&1, 1)) == ~w(1 2 3 4 5 6 7 8 9)
    end

    test "there is no step past either end of coverage" do
      first =
        CoverageCalendar.navigation(%{
          kind: :day,
          at: ~U[2026-07-05 00:00:00Z],
          first_expected_at: ~U[2026-07-01 00:00:00Z],
          last_expected_at: ~U[2026-09-30 00:00:00Z]
        })

      # A control that cannot go anywhere is absent rather than disabled, so the
      # operator can tell they have reached the start.
      assert first.previous == nil
      assert first.next == "2026-08-01"

      last =
        CoverageCalendar.navigation(%{
          kind: :day,
          at: ~U[2026-09-14 00:00:00Z],
          first_expected_at: ~U[2026-07-01 00:00:00Z],
          last_expected_at: ~U[2026-09-30 00:00:00Z]
        })

      assert last.previous == "2026-08-01"
      assert last.next == nil
    end

    test "an hourly asset steps by day and offers a day select" do
      navigation =
        CoverageCalendar.navigation(%{
          kind: :hour,
          at: ~U[2026-07-08 00:00:00Z],
          first_expected_at: ~U[2026-07-01 00:00:00Z],
          last_expected_at: ~U[2026-08-20 23:00:00Z]
        })

      assert navigation.previous == "2026-07-07"
      assert navigation.next == "2026-07-09"

      # One year, so no year select. Two months and a whole month of days, so both of
      # those ship.
      assert Enum.map(navigation.jumps, & &1.name) == ~w(month day)

      day = Enum.find(navigation.jumps, &(&1.name == "day"))
      assert Enum.map(day.options, &elem(&1, 1)) == Enum.map(1..31, &Integer.to_string/1)
    end

    test "a day select stops at the last day coverage expects" do
      navigation =
        CoverageCalendar.navigation(%{
          kind: :hour,
          at: ~U[2026-08-14 00:00:00Z],
          first_expected_at: ~U[2026-07-01 00:00:00Z],
          last_expected_at: ~U[2026-08-20 23:00:00Z]
        })

      # August is the last month, so its days stop on the 20th rather than the 31st.
      day = Enum.find(navigation.jumps, &(&1.name == "day"))
      assert Enum.map(day.options, &elem(&1, 1)) == Enum.map(1..20, &Integer.to_string/1)
    end

    test "a yearly asset has nothing to navigate" do
      navigation =
        CoverageCalendar.navigation(%{
          kind: :year,
          at: ~U[2024-01-01 00:00:00Z],
          first_expected_at: ~U[2019-01-01 00:00:00Z],
          last_expected_at: ~U[2026-01-01 00:00:00Z]
        })

      # Every year is already on screen, so a navigator would be chrome.
      assert navigation == %{previous: nil, next: nil, jumps: []}
    end

    test "a range that fits one screen offers no jump" do
      navigation =
        CoverageCalendar.navigation(%{
          kind: :day,
          at: ~U[2026-07-01 00:00:00Z],
          first_expected_at: ~U[2026-07-03 00:00:00Z],
          last_expected_at: ~U[2026-07-28 00:00:00Z]
        })

      # One month, so both selects would hold one option each and neither ships.
      assert navigation.jumps == []
      assert navigation.previous == nil
      assert navigation.next == nil
    end
  end

  describe "which unit opens first" do
    test "uses the workspace timezone when an instant crosses a local date boundary" do
      assert Subject.opening_date(%{
               kind: :hour,
               timezone: "Europe/Oslo",
               first_expected_at: ~U[2026-03-28 23:00:00Z],
               last_expected_at: ~U[2026-03-28 23:00:00Z]
             }) == ~D[2026-03-29]
    end

    test "the newest one, where there is a unit above the grain" do
      assert CoverageCalendar.opening_date(%{
               kind: :day,
               first_expected_at: ~U[2026-01-05 00:00:00Z],
               last_expected_at: ~U[2026-07-19 00:00:00Z]
             }) == ~D[2026-07-01]

      assert CoverageCalendar.opening_date(%{
               kind: :hour,
               first_expected_at: ~U[2026-07-01 00:00:00Z],
               last_expected_at: ~U[2026-07-19 13:00:00Z]
             }) == ~D[2026-07-19]

      assert CoverageCalendar.opening_date(%{
               kind: :month,
               first_expected_at: ~U[2024-03-01 00:00:00Z],
               last_expected_at: ~U[2026-07-01 00:00:00Z]
             }) == ~D[2026-01-01]
    end

    test "the start of the range, where the whole range is one screen" do
      # A year grain has no unit above it, so every year is already on screen and the
      # screen has to begin where coverage does. Opening on "the unit holding the last
      # year" drew that one year and nothing else — four expected, one cell.
      assert CoverageCalendar.opening_date(%{
               kind: :year,
               first_expected_at: ~U[2022-01-01 00:00:00Z],
               last_expected_at: ~U[2025-01-01 00:00:00Z]
             }) == ~D[2022-01-01]
    end

    test "nothing to open when nothing is expected" do
      assert CoverageCalendar.opening_date(%{}) == nil
      assert CoverageCalendar.opening_date(%{kind: :day, first_expected_at: nil}) == nil
    end
  end

  describe "the range one screen asks for" do
    # The screen used to ask by count — 31 for day grain — and a count cannot name a
    # calendar unit. February came back with three days of March on it, drawn under a
    # February heading and selectable for backfill.
    test "is the unit's own bounds, whatever that unit's length" do
      assert CoverageCalendar.unit_bounds(:day, ~D[2026-02-17]) ==
               {~D[2026-02-01], ~D[2026-03-01]}

      assert CoverageCalendar.unit_bounds(:day, ~D[2026-04-30]) ==
               {~D[2026-04-01], ~D[2026-05-01]}

      assert CoverageCalendar.unit_bounds(:day, ~D[2026-12-01]) ==
               {~D[2026-12-01], ~D[2027-01-01]}
    end

    test "is one day for hourly coverage, including the days a clock change shortens" do
      assert CoverageCalendar.unit_bounds(:hour, ~D[2026-03-29]) ==
               {~D[2026-03-29], ~D[2026-03-30]}
    end

    test "is one year for monthly coverage" do
      assert CoverageCalendar.unit_bounds(:month, ~D[2026-07-08]) ==
               {~D[2026-01-01], ~D[2027-01-01]}
    end

    # Yearly coverage has no unit above it, so the screen is every year in range. An
    # open upper bound says "to the end of coverage" rather than naming a length.
    test "is open-ended where the whole range is one screen" do
      assert CoverageCalendar.unit_bounds(:year, ~D[2026-07-08]) == {~D[2026-07-08], nil}
      assert CoverageCalendar.unit_bounds(nil, ~D[2026-07-08]) == {~D[2026-07-08], nil}
    end

    test "is nothing to ask for when nothing is expected" do
      assert CoverageCalendar.unit_bounds(:day, nil) == {nil, nil}
    end
  end

  describe "resolving a jump" do
    test "keeps the selects the operator did not touch" do
      view = %{
        kind: :day,
        at: ~U[2026-07-01 00:00:00Z],
        first_expected_at: ~U[2025-01-01 00:00:00Z],
        last_expected_at: ~U[2026-12-31 00:00:00Z]
      }

      # Only the year changed, so the month stays July rather than resetting to January.
      assert CoverageCalendar.jump_target(view, %{"year" => "2025", "month" => "7"}) ==
               ~D[2025-07-01]
    end

    test "clamps a combination that falls outside coverage" do
      view = %{
        kind: :day,
        at: ~U[2026-07-01 00:00:00Z],
        first_expected_at: ~U[2026-06-01 00:00:00Z],
        last_expected_at: ~U[2026-08-31 00:00:00Z]
      }

      assert CoverageCalendar.jump_target(view, %{"year" => "2026", "month" => "1"}) ==
               ~D[2026-06-01]

      assert CoverageCalendar.jump_target(view, %{"year" => "2026", "month" => "12"}) ==
               ~D[2026-08-01]
    end

    test "an hourly jump keeps the day within the month it landed on" do
      view = %{
        kind: :hour,
        at: ~U[2026-01-31 00:00:00Z],
        first_expected_at: ~U[2026-01-01 00:00:00Z],
        last_expected_at: ~U[2026-12-31 00:00:00Z]
      }

      # There is no 31 February, so the day gives way rather than the jump failing.
      assert CoverageCalendar.jump_target(view, %{"month" => "2", "day" => "31"}) ==
               ~D[2026-02-28]
    end

    test "an untracked asset resolves nothing" do
      assert CoverageCalendar.jump_target(%{}, %{"year" => "2026"}) == nil
    end
  end

  describe "the calendar element" do
    test "missing periods are buttons carrying their window key, covered ones are not" do
      calendar = day_calendar([8])

      html =
        render_component(&Data.coverage_calendar/1,
          layout: calendar.layout,
          cells: calendar.cells,
          blanks: calendar.blanks,
          columns: calendar.columns,
          column_labels: calendar.column_labels,
          on_select: "toggle_coverage_window"
        )

      assert html =~ ~s(phx-value-key="day:Europe/Oslo:2026-07-08")
      assert html =~ "8 July 2026"

      # One button for the one missing day. A covered day is not a control, because
      # there is nothing to do with it.
      assert html |> String.split(~s(phx-click="toggle_coverage_window")) |> length() == 2
    end

    test "without an event nothing is clickable" do
      calendar = day_calendar([8])

      html =
        render_component(&Data.coverage_calendar/1,
          cells: calendar.cells,
          blanks: calendar.blanks,
          column_labels: calendar.column_labels
        )

      refute html =~ "phx-click"
      assert html =~ "8 July 2026"
    end
  end

  describe "the navigator element" do
    test "renders a step only where there is somewhere to go" do
      html =
        render_component(&Data.calendar_navigator/1,
          label: "July 2026",
          next: "2026-08-01",
          on_step: "show_coverage_period"
        )

      assert html =~ "July 2026"
      assert html =~ ~s(data-testid="coverage-step-next")
      assert html =~ ~s(phx-value-at="2026-08-01")
      refute html =~ ~s(data-testid="coverage-step-previous")
    end

    test "a navigator with nowhere to go and nothing to pick is absent" do
      html =
        render_component(&Data.calendar_navigator/1,
          label: "2026",
          on_step: "show_coverage_period"
        )

      refute html =~ "2026"
    end

    test "renders one select per jump, with the current value chosen" do
      html =
        render_component(&Data.calendar_navigator/1,
          label: "July 2026",
          previous: "2026-06-01",
          on_step: "show_coverage_period",
          on_jump: "jump_coverage_period",
          jumps: [
            %{
              name: "year",
              label: "Year",
              value: "2026",
              options: [{"2025", "2025"}, {"2026", "2026"}]
            }
          ]
        )

      assert html =~ ~s(phx-change="jump_coverage_period")
      assert html =~ ~s(data-testid="coverage-jump-year")
      assert html =~ ~s(<option value="2026" selected)
    end
  end

  describe "the coverage panel" do
    test "answers in one sentence and names the grain it drew" do
      html =
        coverage_panel(
          %{status: :incomplete, expected_count: 300, covered_count: 298, missing_count: 2},
          day_calendar([8, 15])
        )

      assert html =~ ~s(data-testid="asset-coverage")
      assert html =~ "2 of 300 days have no data."

      # The count is the whole range and the calendar is one month of it, so the
      # caption has to say what is on screen rather than repeating the total.
      assert html =~ "31 days, in Europe/Oslo."
      assert html =~ "July 2026"
      assert html =~ "Backfill all 2 missing days"
    end

    test "a selection replaces the offer to backfill everything" do
      html =
        coverage_panel(
          %{status: :incomplete, expected_count: 31, covered_count: 29, missing_count: 2},
          day_calendar([8, 15], window_keys([8]))
        )

      assert html =~ "Backfill 1 selected day"
      assert html =~ ~s(data-testid="clear-coverage-selection")
      refute html =~ "Backfill all"
    end

    test "complete coverage offers no backfill" do
      html =
        coverage_panel(
          %{status: :complete, expected_count: 31, covered_count: 31, missing_count: 0},
          day_calendar([])
        )

      assert html =~ "All 31 days that should hold data do."
      refute html =~ ~s(data-testid="plan-missing-coverage")
    end

    test "unknown coverage says why in plain words and draws no calendar" do
      html =
        coverage_panel(
          %{status: :unknown, unknown_reason: :coverage_not_declared},
          CoverageCalendar.build(%{})
        )

      assert html =~ "Unknown"
      assert html =~ "does not say which periods it should cover"

      # An empty grid reads as a complete one, so there is no grid and no navigator.
      refute html =~ ~s(data-testid="asset-coverage-calendar")
      refute html =~ ~s(data-testid="coverage-navigator")
      refute html =~ ~s(data-testid="plan-missing-coverage")
    end

    test "a plan under review names each period in words before it can be submitted" do
      windows = [window(:day, "day:Europe/Oslo:2026-07-08", day_at(8), false)]

      html =
        render_component(&AssetDetailPage.coverage_panel/1,
          timezone: "Europe/Oslo",
          command_resource: "asset:orders",
          can_plan?: true,
          coverage: %{
            status: :incomplete,
            expected_count: 31,
            covered_count: 30,
            missing_count: 1
          },
          calendar: day_calendar([8]),
          plan: %{plan_hash: String.duplicate("a", 64), window_count: 1, windows: windows}
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
          timezone: "Europe/Oslo",
          command_resource: "asset:orders",
          can_plan?: false,
          coverage: %{
            status: :incomplete,
            expected_count: 31,
            covered_count: 29,
            missing_count: 2
          },
          calendar: day_calendar([8, 15])
        )

      assert html =~ "needs an operator account"
      refute html =~ ~s(phx-click="toggle_coverage_window")
    end
  end

  defp coverage_panel(coverage, calendar) do
    render_component(&AssetDetailPage.coverage_panel/1,
      timezone: "Europe/Oslo",
      command_resource: "asset:orders",
      can_plan?: true,
      coverage: coverage,
      calendar: calendar,
      navigation: %{previous: "2026-06-01", next: "2026-08-01", jumps: []}
    )
  end

  defp day_calendar(missing_days, selected \\ []) do
    windows =
      Enum.map(1..31, fn day ->
        window(
          :day,
          "day:Europe/Oslo:2026-07-#{pad(day)}",
          day_at(day),
          day not in missing_days
        )
      end)

    CoverageCalendar.build(%{
      kind: :day,
      timezone: "Europe/Oslo",
      windows: windows,
      selected: selected
    })
  end

  defp window(kind, key, start_at, covered?) do
    %{
      window_key: key,
      kind: kind,
      timezone: "Europe/Oslo",
      start_at: start_at,
      end_at: start_at,
      covered?: covered?
    }
  end

  defp window_keys(days), do: Enum.map(days, &"day:Europe/Oslo:2026-07-#{pad(&1)}")

  defp day_at(day), do: DateTime.new!(Date.new!(2026, 7, day), ~T[00:00:00], "Etc/UTC")
  defp month_at(month), do: DateTime.new!(Date.new!(2026, month, 1), ~T[00:00:00], "Etc/UTC")
  defp year_at(year), do: DateTime.new!(Date.new!(year, 1, 1), ~T[00:00:00], "Etc/UTC")
  defp pad(value), do: String.pad_leading("#{value}", 2, "0")
end
