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

  test "occurrences_between can cap high-frequency six-field schedules" do
    assert Cron.occurrences_between(
             "* * * * * *",
             "Etc/UTC",
             ~U[2026-04-27 12:00:00Z],
             ~U[2026-04-27 12:00:10Z],
             limit: 3
           ) == [
             ~U[2026-04-27 12:00:01Z],
             ~U[2026-04-27 12:00:02Z],
             ~U[2026-04-27 12:00:03Z]
           ]
  end

  test "capped high-frequency occurrence generation does not need a full day of candidates" do
    assert Cron.occurrences_between(
             "* * * * * *",
             "Etc/UTC",
             ~U[2026-04-27 00:00:00Z],
             ~U[2026-04-28 00:00:00Z],
             limit: 3
           ) == [
             ~U[2026-04-27 00:00:01Z],
             ~U[2026-04-27 00:00:02Z],
             ~U[2026-04-27 00:00:03Z]
           ]
  end

  test "spring-forward gap skips nonexistent local scheduled times" do
    cron = "30 2 * * *"
    timezone = "Europe/Oslo"
    before_gap = ~U[2026-03-28 01:30:00Z]
    after_gap = ~U[2026-03-30 00:30:00Z]

    assert Cron.latest_due(cron, timezone, ~U[2026-03-29 01:30:00Z]) == before_gap
    assert Cron.first_occurrence_between(cron, timezone, before_gap, after_gap) == after_gap
    assert Cron.last_occurrence_between(cron, timezone, before_gap, after_gap) == after_gap
    assert Cron.occurrences_between(cron, timezone, before_gap, after_gap) == [after_gap]
  end

  test "autumn ambiguous hour includes both local occurrences in deterministic order" do
    cron = "30 2 * * *"
    timezone = "Europe/Oslo"
    previous_day = ~U[2026-10-24 00:30:00Z]
    first_ambiguous = ~U[2026-10-25 00:30:00Z]
    second_ambiguous = ~U[2026-10-25 01:30:00Z]

    assert Cron.latest_due(cron, timezone, ~U[2026-10-25 02:45:00Z]) == second_ambiguous

    assert Cron.first_occurrence_between(cron, timezone, previous_day, second_ambiguous) ==
             first_ambiguous

    assert Cron.last_occurrence_between(cron, timezone, previous_day, second_ambiguous) ==
             second_ambiguous

    assert Cron.occurrences_between(cron, timezone, previous_day, second_ambiguous) == [
             first_ambiguous,
             second_ambiguous
           ]
  end
end
