defmodule Favn.Freshness.KeyTest do
  use ExUnit.Case, async: true

  alias Favn.Freshness.Key, as: FreshnessKey
  alias Favn.Window.Key, as: WindowKey

  test "builds latest freshness key" do
    assert FreshnessKey.latest() == "latest"
    assert FreshnessKey.encode("latest") == {:ok, "latest"}
    assert FreshnessKey.decode("latest") == {:ok, "latest"}
  end

  test "wraps window keys using canonical window encoding" do
    window_key = WindowKey.new!(:day, ~U[2026-05-08 00:00:00Z], "Etc/UTC")

    assert FreshnessKey.window(window_key) ==
             {:ok, "window:#{WindowKey.encode(window_key)}"}

    assert FreshnessKey.decode("window:#{WindowKey.encode(window_key)}") ==
             {:ok, "window:#{WindowKey.encode(window_key)}"}
  end

  test "combines exact window and calendar refresh identities" do
    window_key = WindowKey.new!(:month, ~U[2026-06-01 00:00:00Z], "Etc/UTC")

    expected =
      "window:#{WindowKey.encode(window_key)}|calendar:day:Europe/Oslo:2026-07-17"

    assert FreshnessKey.window_refresh(
             window_key,
             :day,
             "Europe/Oslo",
             ~D[2026-07-17]
           ) == {:ok, expected}

    assert FreshnessKey.decode(expected) == {:ok, expected}

    assert FreshnessKey.parse(expected) ==
             {:ok, {:window_refresh, window_key, :day, "Europe/Oslo", "2026-07-17"}}
  end

  test "builds calendar day keys with local date and timezone" do
    assert FreshnessKey.calendar(:daily, "Europe/Oslo", ~D[2026-05-08]) ==
             {:ok, "calendar:day:Europe/Oslo:2026-05-08"}

    assert FreshnessKey.decode("calendar:day:Europe/Oslo:2026-05-08") ==
             {:ok, "calendar:day:Europe/Oslo:2026-05-08"}

    assert FreshnessKey.parse("calendar:day:Europe/Oslo:2026-05-08") ==
             {:ok, {:calendar, :day, "Europe/Oslo", "2026-05-08"}}
  end

  test "builds deterministic calendar keys for hour month and year" do
    assert FreshnessKey.calendar(:hour, "Europe/Oslo", ~N[2026-05-08 07:30:00]) ==
             {:ok, "calendar:hour:Europe/Oslo:2026-05-08T07"}

    assert FreshnessKey.calendar(:month, "Europe/Oslo", ~D[2026-05-08]) ==
             {:ok, "calendar:month:Europe/Oslo:2026-05"}

    assert FreshnessKey.calendar(:year, "Europe/Oslo", ~D[2026-05-08]) ==
             {:ok, "calendar:year:Europe/Oslo:2026"}
  end

  test "offset-qualified hourly keys distinguish repeated daylight-saving hours" do
    first = ~U[2026-10-25 00:00:00Z]
    second = ~U[2026-10-25 01:00:00Z]

    assert FreshnessKey.calendar(:hour, "Europe/Oslo", first) ==
             {:ok, "calendar:hour:Europe/Oslo:2026-10-25T02+02:00"}

    assert FreshnessKey.calendar(:hour, "Europe/Oslo", second) ==
             {:ok, "calendar:hour:Europe/Oslo:2026-10-25T02+01:00"}

    assert FreshnessKey.decode("calendar:hour:Europe/Oslo:2026-10-25T02+02:00") ==
             {:ok, "calendar:hour:Europe/Oslo:2026-10-25T02+02:00"}

    assert FreshnessKey.decode("calendar:hour:Europe/Oslo:2026-10-25T02") ==
             {:ok, "calendar:hour:Europe/Oslo:2026-10-25T02"}
  end

  test "rejects invalid kind and timezone" do
    assert FreshnessKey.calendar(:week, "Europe/Oslo", ~D[2026-05-08]) ==
             {:error, {:invalid_window_policy_kind, :week}}

    assert FreshnessKey.calendar(:day, "Definitely/NotAZone", ~D[2026-05-08]) ==
             {:error, {:invalid_timezone, "Definitely/NotAZone"}}
  end

  test "rejects invalid decoded keys" do
    assert FreshnessKey.decode("unknown") == {:error, {:invalid_freshness_key, "unknown"}}

    assert FreshnessKey.decode("calendar:day:Europe/Oslo:not-a-date") ==
             {:error, {:invalid_calendar_period_start, :day, "not-a-date"}}

    assert FreshnessKey.decode("window:not-a-window-key") ==
             {:error, {:invalid_encoded_key, "not-a-window-key"}}

    window_key = WindowKey.new!(:month, ~U[2026-06-01 00:00:00Z], "Etc/UTC")

    assert FreshnessKey.decode(
             "window:#{WindowKey.encode(window_key)}|calendar:day:Europe/Oslo:not-a-date"
           ) == {:error, {:invalid_calendar_period_start, :day, "not-a-date"}}
  end
end
