defmodule FavnOrchestrator.Scheduler.CronTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Scheduler.Cron

  test "five-field cron keeps minute-level latest due and range semantics" do
    assert Cron.latest_due("*/15 * * * *", "Etc/UTC", ~U[2026-04-27 12:34:56.123456Z]) ==
             ~U[2026-04-27 12:30:00Z]

    assert Cron.occurrences_between(
             "*/15 * * * *",
             "Etc/UTC",
             ~U[2026-04-27 12:00:00Z],
             ~U[2026-04-27 13:00:00Z]
           ) == [
             ~U[2026-04-27 12:15:00Z],
             ~U[2026-04-27 12:30:00Z],
             ~U[2026-04-27 12:45:00Z],
             ~U[2026-04-27 13:00:00Z]
           ]
  end

  test "six-field cron matches fixed seconds" do
    assert Cron.matches?("15 * * * * *", ~U[2026-04-27 12:00:15Z])
    refute Cron.matches?("15 * * * * *", ~U[2026-04-27 12:00:16Z])

    assert Cron.latest_due("15 * * * * *", "Etc/UTC", ~U[2026-04-27 12:00:20.123456Z]) ==
             ~U[2026-04-27 12:00:15Z]
  end

  test "six-field latest due recovers sparse schedules beyond one day" do
    assert Cron.latest_due("0 0 0 * * 1", "Etc/UTC", ~U[2026-04-29 12:00:00Z]) ==
             ~U[2026-04-27 00:00:00Z]

    assert Cron.latest_due("0 0 0 1 * *", "Etc/UTC", ~U[2026-04-10 12:00:00Z]) ==
             ~U[2026-04-01 00:00:00Z]
  end

  test "six-field range helpers return occurrences after last due through latest due" do
    cron = "15 0 0 * * *"
    from = ~U[2026-04-01 00:00:15Z]
    to = ~U[2026-04-04 00:00:15Z]

    assert Cron.first_occurrence_between(cron, "Etc/UTC", from, to) == ~U[2026-04-02 00:00:15Z]
    assert Cron.last_occurrence_between(cron, "Etc/UTC", from, to) == ~U[2026-04-04 00:00:15Z]

    assert Cron.occurrences_between(cron, "Etc/UTC", from, to) == [
             ~U[2026-04-02 00:00:15Z],
             ~U[2026-04-03 00:00:15Z],
             ~U[2026-04-04 00:00:15Z]
           ]
  end
end
