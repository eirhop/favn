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

  test "builds calendar day keys with local date and timezone" do
    assert FreshnessKey.calendar(:daily, "Europe/Oslo", ~D[2026-05-08]) ==
             {:ok, "calendar:day:Europe/Oslo:2026-05-08"}

    assert FreshnessKey.decode("calendar:day:Europe/Oslo:2026-05-08") ==
             {:ok, "calendar:day:Europe/Oslo:2026-05-08"}
  end

  test "builds deterministic calendar keys for hour month and year" do
    assert FreshnessKey.calendar(:hour, "Europe/Oslo", ~N[2026-05-08 07:30:00]) ==
             {:ok, "calendar:hour:Europe/Oslo:2026-05-08T07"}

    assert FreshnessKey.calendar(:month, "Europe/Oslo", ~D[2026-05-08]) ==
             {:ok, "calendar:month:Europe/Oslo:2026-05"}

    assert FreshnessKey.calendar(:year, "Europe/Oslo", ~D[2026-05-08]) ==
             {:ok, "calendar:year:Europe/Oslo:2026"}
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
  end
end
