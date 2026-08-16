defmodule FavnView.TimeTest do
  use ExUnit.Case, async: true

  alias FavnView.Time

  test "formats instants in an explicit workspace timezone" do
    assert Time.format(~U[2026-06-30 22:00:00Z], "%b %Y", "Europe/Oslo") == "Jul 2026"

    assert Time.format(
             ~U[2026-07-01 06:45:56Z],
             "%Y-%m-%d %H:%M:%S %Z",
             "Europe/Oslo"
           ) ==
             "2026-07-01 08:45:56 CEST"
  end

  test "uses the correct local offset on both sides of daylight saving time" do
    assert Time.format(~U[2026-01-15 10:00:00Z], "%H:%M %Z", "Europe/Oslo") == "11:00 CET"
    assert Time.format(~U[2026-07-15 10:00:00Z], "%H:%M %Z", "Europe/Oslo") == "12:00 CEST"

    assert Time.beginning_of_day(~D[2026-03-29], "Europe/Oslo") == ~U[2026-03-28 23:00:00Z]
    assert Time.beginning_of_day(~D[2026-03-30], "Europe/Oslo") == ~U[2026-03-29 22:00:00Z]
  end

  test "keeps simultaneous workspaces isolated" do
    instant = ~U[2026-07-01 06:45:56Z]

    assert Time.format(instant, "%H:%M %Z", "Europe/Oslo") == "08:45 CEST"
    assert Time.format(instant, "%H:%M %Z", "America/New_York") == "02:45 EDT"
  end
end
