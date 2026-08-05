defmodule FavnView.TimeTest do
  use ExUnit.Case, async: false

  alias FavnView.Time

  setup do
    previous = Application.get_env(:favn, :default_timezone)

    on_exit(fn ->
      if is_nil(previous) do
        Application.delete_env(:favn, :default_timezone)
      else
        Application.put_env(:favn, :default_timezone, previous)
      end
    end)

    :ok
  end

  test "formats instants in the configured default timezone" do
    Application.put_env(:favn, :default_timezone, "Europe/Oslo")

    assert Time.format(~U[2026-06-30 22:00:00Z], "%b %Y") == "Jul 2026"

    assert Time.format(~U[2026-07-01 06:45:56Z], "%Y-%m-%d %H:%M:%S %Z") ==
             "2026-07-01 08:45:56 CEST"
  end

  test "uses the correct local offset on both sides of daylight saving time" do
    Application.put_env(:favn, :default_timezone, "Europe/Oslo")

    assert Time.format(~U[2026-01-15 10:00:00Z], "%H:%M %Z") == "11:00 CET"
    assert Time.format(~U[2026-07-15 10:00:00Z], "%H:%M %Z") == "12:00 CEST"

    assert Time.beginning_of_day(~D[2026-03-29]) == ~U[2026-03-28 23:00:00Z]
    assert Time.beginning_of_day(~D[2026-03-30]) == ~U[2026-03-29 22:00:00Z]
  end

  test "falls back to UTC when no default is configured" do
    Application.delete_env(:favn, :default_timezone)

    assert Time.format(~U[2026-07-01 06:45:56Z], "%H:%M %Z") == "06:45 UTC"
  end
end
