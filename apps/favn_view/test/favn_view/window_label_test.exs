defmodule FavnView.WindowLabelTest do
  use ExUnit.Case, async: true

  alias FavnView.WindowLabel

  doctest WindowLabel

  describe "whole periods" do
    test "names a day, a month, and a year by the period rather than by its bounds" do
      assert compact(~U[2026-07-17 00:00:00Z], ~U[2026-07-18 00:00:00Z]) == "Jul 17, 2026"
      assert compact(~U[2026-07-01 00:00:00Z], ~U[2026-08-01 00:00:00Z]) == "Jul 2026"
      assert compact(~U[2026-01-01 00:00:00Z], ~U[2027-01-01 00:00:00Z]) == "2026"
    end

    test "carries the month across a year boundary" do
      assert compact(~U[2026-12-01 00:00:00Z], ~U[2027-01-01 00:00:00Z]) == "Dec 2026"
    end

    test "an hour keeps the day, because an hour alone names nothing" do
      assert compact(~U[2026-07-17 14:00:00Z], ~U[2026-07-17 15:00:00Z]) == "Jul 17, 14:00"
    end
  end

  describe "the display timezone decides what a period is" do
    test "a day is the local day, not the UTC one it starts inside" do
      # Midnight in Oslo is 22:00 UTC the day before, so a reader who trusted
      # the UTC fields would name this window Jul 16.
      assert compact(~U[2026-07-16 22:00:00Z], ~U[2026-07-17 22:00:00Z], "Europe/Oslo") ==
               "Jul 17, 2026"
    end

    test "a 23-hour spring-forward day is still one day" do
      # Oslo loses an hour on 2026-03-29, so this window is 82,800 seconds long.
      assert compact(~U[2026-03-28 23:00:00Z], ~U[2026-03-29 22:00:00Z], "Europe/Oslo") ==
               "Mar 29, 2026"
    end

    test "a 25-hour autumn day is still one day" do
      # And gains one on 2026-10-25, making the same window 90,000 seconds.
      assert compact(~U[2026-10-24 22:00:00Z], ~U[2026-10-25 23:00:00Z], "Europe/Oslo") ==
               "Oct 25, 2026"
    end

    test "the same instants are not a whole day in a timezone they do not align with" do
      assert compact(~U[2026-07-16 22:00:00Z], ~U[2026-07-17 22:00:00Z], "Etc/UTC") ==
               "Jul 16 22:00 – Jul 17 22:00, 2026"
    end
  end

  describe "partial windows" do
    test "keep the range, saying once whatever both ends share" do
      assert compact(~U[2026-07-17 09:30:00Z], ~U[2026-07-17 11:15:00Z]) ==
               "Jul 17, 2026 09:30 – 11:15"

      assert compact(~U[2026-07-17 09:30:00Z], ~U[2026-07-19 11:15:00Z]) ==
               "Jul 17 09:30 – Jul 19 11:15, 2026"

      assert compact(~U[2026-12-31 09:30:00Z], ~U[2027-01-02 11:15:00Z]) ==
               "Dec 31, 2026 09:30 – Jan 2, 2027 11:15"
    end

    test "two whole days are a range, not one day" do
      assert compact(~U[2026-07-17 00:00:00Z], ~U[2026-07-19 00:00:00Z]) ==
               "Jul 17 00:00 – Jul 19 00:00, 2026"
    end
  end

  describe "the full form" do
    test "states both bounds with their zone, for the tooltip behind a compact label" do
      assert WindowLabel.full(~U[2026-07-17 00:00:00Z], ~U[2026-07-18 00:00:00Z], "Etc/UTC") ==
               "Jul 17, 2026 00:00:00 UTC – Jul 18, 2026 00:00:00 UTC"
    end

    test "an absent window has no name in either form" do
      assert compact(nil, nil) == nil
      assert WindowLabel.full(nil, nil, "Etc/UTC") == nil
    end
  end

  defp compact(start_at, end_at, timezone \\ "Etc/UTC"),
    do: WindowLabel.compact(start_at, end_at, timezone)
end
