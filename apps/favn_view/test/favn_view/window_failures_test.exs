defmodule FavnView.WindowFailuresTest do
  use ExUnit.Case, async: true

  alias FavnView.WindowFailures

  doctest FavnView.WindowFailures

  defp window(opts) do
    day = Keyword.fetch!(opts, :day)

    %{
      status: Keyword.get(opts, :status, :failed),
      window_start: DateTime.new!(Date.new!(2026, 1, day), ~T[00:00:00], "Etc/UTC"),
      window_end: DateTime.new!(Date.new!(2026, 1, day + 1), ~T[00:00:00], "Etc/UTC"),
      run_id: Keyword.get(opts, :run_id),
      attempt_count: Keyword.get(opts, :attempt_count, 1),
      last_error: Keyword.get(opts, :last_error)
    }
  end

  defp inspected(reason), do: %{"reason" => ~s(%{"reason" => "#{reason}"})}

  describe "grouping" do
    test "one reason shared by every window is one row that names the coverage it cost" do
      windows = for day <- 1..30, do: window(day: day, last_error: inspected("bad_identity"))

      assert [group] = WindowFailures.group(windows, "Etc/UTC")
      assert group.reason == "bad_identity"
      assert group.window_count == 30
      assert group.run_count == 0
      assert group.span == "Jan 1 00:00 – Jan 31 00:00, 2026"
      assert is_nil(group.first_window)
    end

    test "a group of one names its single window instead of a span" do
      windows = [window(day: 4, last_error: inspected("no_runner"))]

      assert [group] = WindowFailures.group(windows, "Etc/UTC")
      assert group.first_window == "Jan 4, 2026"
      assert is_nil(group.span)
    end

    test "distinct reasons are separate rows, worst coverage first" do
      windows =
        [window(day: 1, last_error: inspected("rare"))] ++
          for day <- 2..5, do: window(day: day, last_error: inspected("common"))

      assert [common, rare] = WindowFailures.group(windows, "Etc/UTC")
      assert {common.reason, common.window_count} == {"common", 4}
      assert {rare.reason, rare.window_count} == {"rare", 1}
    end

    test "reasons that tie are ordered by name, so the list is stable across reads" do
      windows = [
        window(day: 1, last_error: inspected("zeta")),
        window(day: 2, last_error: inspected("alpha"))
      ]

      assert ["alpha", "zeta"] ==
               windows |> WindowFailures.group("Etc/UTC") |> Enum.map(& &1.reason)

      assert ["alpha", "zeta"] ==
               windows
               |> Enum.reverse()
               |> WindowFailures.group("Etc/UTC")
               |> Enum.map(& &1.reason)
    end

    test "a window that is not failed is never counted as a failure" do
      windows = [
        window(day: 1, status: :succeeded, last_error: inspected("stale")),
        window(day: 2, status: :running),
        window(day: 3, last_error: inspected("real"))
      ]

      assert [%{reason: "real", window_count: 1}] = WindowFailures.group(windows, "Etc/UTC")
    end

    test "windows that did start a run are counted and linked, bounded to three" do
      windows =
        for day <- 1..5,
            do: window(day: day, run_id: "run_#{day}", last_error: inspected("lease_lost"))

      assert [group] = WindowFailures.group(windows, "Etc/UTC")
      assert group.run_count == 5
      assert group.run_ids == ["run_1", "run_2", "run_3"]
    end

    test "the highest attempt count in the group is the one reported" do
      windows = [
        window(day: 1, attempt_count: 1, last_error: inspected("flaky")),
        window(day: 2, attempt_count: 4, last_error: inspected("flaky"))
      ]

      assert [%{attempts: 4}] = WindowFailures.group(windows, "Etc/UTC")
    end

    test "an empty list groups to nothing rather than to an unknown-reason row" do
      assert WindowFailures.group([], "Etc/UTC") == []
    end
  end

  describe "reading a stored reason" do
    test "a binary reason survives unchanged" do
      assert WindowFailures.reason(%{last_error: %{"reason" => "window_lease_lost"}}) ==
               "window_lease_lost"
    end

    test "an inspected single-entry map is unwrapped to the reason inside it" do
      assert WindowFailures.reason(%{last_error: inspected("invalid_pipeline_identity")}) ==
               "invalid_pipeline_identity"
    end

    test "an inspected atom-keyed map is unwrapped too" do
      assert WindowFailures.reason(%{last_error: %{"reason" => "%{reason: :timed_out}"}}) ==
               "timed_out"
    end

    test "a map with more than a reason in it is shown whole" do
      stored = ~s(%{"reason" => "x", "detail" => "y"})

      assert WindowFailures.reason(%{last_error: %{"reason" => stored}}) == stored
    end

    test "the message is read when no reason was recorded" do
      assert WindowFailures.reason(%{last_error: %{"message" => "runner rejected the window"}}) ==
               "runner rejected the window"
    end

    test "a blank reason falls through to the message rather than showing empty" do
      error = %{"reason" => "   ", "message" => "connection closed"}

      assert WindowFailures.reason(%{last_error: error}) == "connection closed"
    end

    test "a window with no error at all says so rather than rendering nil" do
      assert WindowFailures.reason(%{last_error: nil}) == WindowFailures.unknown_reason()
      assert WindowFailures.reason(%{}) == WindowFailures.unknown_reason()
    end

    test "string keys and atom keys both reach the error" do
      assert WindowFailures.reason(%{"last_error" => %{"reason" => "boom"}}) == "boom"
    end
  end

  describe "the detail line" do
    test "a message that repeats the reason is not shown twice" do
      windows = [window(day: 1, last_error: %{"reason" => "boom", "message" => "boom"})]

      assert [%{detail: nil}] = WindowFailures.group(windows, "Etc/UTC")
    end

    test "a message that says more than the reason is kept" do
      error = %{"reason" => "boom", "message" => "the runner pool was empty"}

      assert [%{detail: "the runner pool was empty"}] =
               WindowFailures.group([window(day: 1, last_error: error)], "Etc/UTC")
    end
  end

  describe "timezone" do
    test "a span is named in the display timezone, not in UTC" do
      windows = [
        %{
          status: :failed,
          window_start: ~U[2026-06-30 22:00:00Z],
          window_end: ~U[2026-07-01 22:00:00Z],
          run_id: nil,
          attempt_count: 1,
          last_error: %{"reason" => "boom"}
        },
        %{
          status: :failed,
          window_start: ~U[2026-07-01 22:00:00Z],
          window_end: ~U[2026-07-02 22:00:00Z],
          run_id: nil,
          attempt_count: 1,
          last_error: %{"reason" => "boom"}
        }
      ]

      assert [%{span: "Jul 1 00:00 – Jul 3 00:00, 2026"}] =
               WindowFailures.group(windows, "Europe/Oslo")
    end
  end
end
