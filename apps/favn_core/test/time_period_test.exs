defmodule Favn.TimePeriodTest do
  use ExUnit.Case, async: true

  alias Favn.TimePeriod

  test "normalizes kind aliases" do
    assert TimePeriod.normalize_kind(:hourly) == {:ok, :hour}
    assert TimePeriod.normalize_kind(:daily) == {:ok, :day}
    assert TimePeriod.normalize_kind(:monthly) == {:ok, :month}
    assert TimePeriod.normalize_kind(:yearly) == {:ok, :year}
    assert TimePeriod.normalize_kind(:day) == {:ok, :day}

    assert {:error, {:invalid_period_kind, :weekly}} = TimePeriod.normalize_kind(:weekly)
  end

  test "validates kind and timezone" do
    assert {:error, {:invalid_period_kind, :week}} =
             TimePeriod.floor(~U[2026-04-27 12:30:00Z], :week, "Etc/UTC")

    assert {:error, {:invalid_timezone, "Definitely/NotAZone"}} =
             TimePeriod.floor(~U[2026-04-27 12:30:00Z], :day, "Definitely/NotAZone")
  end

  test "floors datetimes in the requested timezone" do
    now = ~U[2026-04-27 22:30:45.123456Z]

    assert {:ok, hour} = TimePeriod.floor(now, :hour, "Europe/Oslo")
    assert hour == local!(~N[2026-04-28 00:00:00], "Europe/Oslo")

    assert {:ok, day} = TimePeriod.floor(now, :day, "Europe/Oslo")
    assert day == local!(~N[2026-04-28 00:00:00], "Europe/Oslo")

    assert {:ok, month} = TimePeriod.floor(now, :month, "Europe/Oslo")
    assert month == local!(~N[2026-04-01 00:00:00], "Europe/Oslo")

    assert {:ok, year} = TimePeriod.floor(now, :year, "Europe/Oslo")
    assert year == local!(~N[2026-01-01 00:00:00], "Europe/Oslo")
  end

  test "shifts period boundaries" do
    assert {:ok, hour} = TimePeriod.shift(~U[2026-04-27 12:00:00Z], :hour, 2)
    assert hour == ~U[2026-04-27 14:00:00Z]

    start_at = local!(~N[2026-01-31 00:00:00], "Europe/Oslo")

    assert {:ok, day} = TimePeriod.shift(start_at, :day, 1)
    assert day == local!(~N[2026-02-01 00:00:00], "Europe/Oslo")

    assert {:ok, month} = TimePeriod.shift(start_at, :month, 1)
    assert month == local!(~N[2026-02-01 00:00:00], "Europe/Oslo")

    assert {:ok, year} = TimePeriod.shift(start_at, :year, -1)
    assert year == local!(~N[2025-01-01 00:00:00], "Europe/Oslo")

    assert {:error, {:invalid_period_count, 1.5}} = TimePeriod.shift(start_at, :day, 1.5)
  end

  test "builds bounds from window request values" do
    assert {:ok, hour} = TimePeriod.bounds(:hour, "2026-04-27T13", "Etc/UTC")

    assert hour == %TimePeriod{
             kind: :hour,
             start_at: ~U[2026-04-27 13:00:00Z],
             end_at: ~U[2026-04-27 14:00:00Z],
             timezone: "Etc/UTC"
           }

    assert {:ok, day} = TimePeriod.bounds(:daily, "2026-04-27", "Etc/UTC")
    assert day.start_at == ~U[2026-04-27 00:00:00Z]
    assert day.end_at == ~U[2026-04-28 00:00:00Z]

    assert {:ok, month} = TimePeriod.bounds(:monthly, "2026-04", "Etc/UTC")
    assert month.start_at == ~U[2026-04-01 00:00:00Z]
    assert month.end_at == ~U[2026-05-01 00:00:00Z]

    assert {:ok, year} = TimePeriod.bounds(:yearly, "2026", "Etc/UTC")
    assert year.start_at == ~U[2026-01-01 00:00:00Z]
    assert year.end_at == ~U[2027-01-01 00:00:00Z]

    assert {:error, {:invalid_window_value, :hour, "2026-04-27T24"}} =
             TimePeriod.bounds(:hour, "2026-04-27T24", "Etc/UTC")
  end

  test "returns current and previous complete bounds" do
    now = ~U[2026-04-27 12:30:00Z]

    assert {:ok, current} = TimePeriod.current(:day, now, "Etc/UTC")
    assert current.start_at == ~U[2026-04-27 00:00:00Z]
    assert current.end_at == ~U[2026-04-28 00:00:00Z]

    assert {:ok, previous} = TimePeriod.previous_complete(:day, now, "Etc/UTC")
    assert previous.start_at == ~U[2026-04-26 00:00:00Z]
    assert previous.end_at == ~U[2026-04-27 00:00:00Z]

    assert {:ok, boundary_previous} =
             TimePeriod.previous_complete(:month, ~U[2026-05-01 00:00:00Z], "Etc/UTC")

    assert boundary_previous.start_at == ~U[2026-04-01 00:00:00Z]
    assert boundary_previous.end_at == ~U[2026-05-01 00:00:00Z]
  end

  test "expands ranges into contiguous exclusive-end period bounds" do
    assert {:ok, periods} =
             TimePeriod.expand_range(
               :day,
               ~U[2026-04-01 10:00:00Z],
               ~U[2026-04-04 00:00:00Z],
               "Etc/UTC"
             )

    assert Enum.map(periods, & &1.start_at) == [
             ~U[2026-04-01 00:00:00Z],
             ~U[2026-04-02 00:00:00Z],
             ~U[2026-04-03 00:00:00Z]
           ]

    assert Enum.map(periods, & &1.end_at) == [
             ~U[2026-04-02 00:00:00Z],
             ~U[2026-04-03 00:00:00Z],
             ~U[2026-04-04 00:00:00Z]
           ]

    assert {:ok, [_april]} =
             TimePeriod.expand_range(
               :month,
               ~U[2026-04-01 00:00:00Z],
               ~U[2026-05-15 12:00:00Z],
               "Etc/UTC"
             )

    assert {:error, :invalid_period_bounds} =
             TimePeriod.expand_range(
               :day,
               ~U[2026-04-02 00:00:00Z],
               ~U[2026-04-01 00:00:00Z],
               "Etc/UTC"
             )
  end

  test "daily periods use Europe/Oslo calendar midnights across DST" do
    assert {:ok, period} = TimePeriod.bounds(:day, "2026-03-29", "Europe/Oslo")

    assert period.start_at == local!(~N[2026-03-29 00:00:00], "Europe/Oslo")
    assert period.end_at == local!(~N[2026-03-30 00:00:00], "Europe/Oslo")
    assert DateTime.diff(period.end_at, period.start_at, :hour) == 23

    assert {:ok, next_day} = TimePeriod.shift(period.start_at, :day, 1)
    assert next_day == period.end_at
  end

  test "hourly periods use the configured timezone database across repeated DST hours" do
    assert {:ok, first} =
             TimePeriod.current(:hour, ~U[2026-10-25 00:30:00Z], "Europe/Oslo")

    assert first.start_at == oslo!(~U[2026-10-25 00:00:00Z])
    assert first.end_at == oslo!(~U[2026-10-25 01:00:00Z])
    assert first.start_at.hour == 2
    assert first.end_at.hour == 2
    assert first.start_at.utc_offset + first.start_at.std_offset == 7200
    assert first.end_at.utc_offset + first.end_at.std_offset == 3600

    assert {:ok, second} =
             TimePeriod.current(:hour, ~U[2026-10-25 01:30:00Z], "Europe/Oslo")

    assert second.start_at == first.end_at
    assert second.end_at == oslo!(~U[2026-10-25 02:00:00Z])
  end

  defp local!(naive, timezone),
    do: DateTime.from_naive!(naive, timezone, Favn.Timezone.database!())

  defp oslo!(datetime),
    do: DateTime.shift_zone!(datetime, "Europe/Oslo", Favn.Timezone.database!())
end
