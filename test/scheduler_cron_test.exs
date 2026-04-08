defmodule Favn.Scheduler.CronTest do
  use ExUnit.Case, async: true

  alias Favn.Scheduler.Cron

  test "uses OR semantics when both day-of-month and day-of-week are restricted" do
    cron = "0 9 10 * 1"

    monday_not_10th = DateTime.from_naive!(~N[2026-03-09 09:00:00], "Etc/UTC")
    tenth_not_monday = DateTime.from_naive!(~N[2026-03-10 09:00:00], "Etc/UTC")
    neither = DateTime.from_naive!(~N[2026-03-11 09:00:00], "Etc/UTC")

    assert Cron.matches?(cron, monday_not_10th)
    assert Cron.matches?(cron, tenth_not_monday)
    refute Cron.matches?(cron, neither)
  end

  test "requires weekday when day-of-month is unrestricted" do
    cron = "0 9 * * 1"

    monday = DateTime.from_naive!(~N[2026-03-09 09:00:00], "Etc/UTC")
    tuesday = DateTime.from_naive!(~N[2026-03-10 09:00:00], "Etc/UTC")

    assert Cron.matches?(cron, monday)
    refute Cron.matches?(cron, tuesday)
  end

  test "requires day-of-month when weekday is unrestricted" do
    cron = "0 9 10 * *"

    tenth = DateTime.from_naive!(~N[2026-03-10 09:00:00], "Etc/UTC")
    eleventh = DateTime.from_naive!(~N[2026-03-11 09:00:00], "Etc/UTC")

    assert Cron.matches?(cron, tenth)
    refute Cron.matches?(cron, eleventh)
  end

  test "treats wildcard-step day-of-month as unrestricted" do
    cron = "0 9 */1 * 1"

    monday = DateTime.from_naive!(~N[2026-03-09 09:00:00], "Etc/UTC")
    tuesday = DateTime.from_naive!(~N[2026-03-10 09:00:00], "Etc/UTC")

    assert Cron.matches?(cron, monday)
    refute Cron.matches?(cron, tuesday)
  end

  test "treats wildcard-step day-of-week as unrestricted" do
    cron = "0 9 10 * */1"

    tenth = DateTime.from_naive!(~N[2026-03-10 09:00:00], "Etc/UTC")
    eleventh = DateTime.from_naive!(~N[2026-03-11 09:00:00], "Etc/UTC")

    assert Cron.matches?(cron, tenth)
    refute Cron.matches?(cron, eleventh)
  end
end
