defmodule Favn.WindowTest do
  use ExUnit.Case, async: true

  alias Favn.Window.Anchor
  alias Favn.Window.Key
  alias Favn.Window.Policy
  alias Favn.Window.Request
  alias Favn.Window.Runtime
  alias Favn.Window.Spec

  test "builds canonical window structs and deterministic keys" do
    assert {:ok, %Spec{kind: :hour, timezone: nil}} = Spec.new(:hour)
    assert {:ok, %Spec{kind: :day}} = Spec.new(:day)
    assert {:ok, %Spec{kind: :month, refresh_from: :day}} = Spec.new(:month, refresh_from: :day)
    assert {:ok, %Spec{kind: :year, required: true}} = Spec.new(:year, required: true)

    start_at = ~U[2026-01-01 00:00:00Z]
    end_at = ~U[2026-01-02 00:00:00Z]

    assert {:ok, anchor} = Anchor.new(:day, start_at, end_at)
    assert :ok = Anchor.validate(anchor)

    assert anchor.key == %{
             kind: :day,
             start_at_us: DateTime.to_unix(start_at, :microsecond),
             timezone: "Etc/UTC"
           }

    assert {:ok, runtime} = Runtime.new(:day, start_at, end_at, anchor.key)
    assert :ok = Runtime.validate(runtime)
    assert runtime.key == anchor.key
    assert runtime.anchor_key == anchor.key
  end

  test "normalizes schema 12 persisted window specs" do
    assert {:ok, %Spec{kind: :month, refresh_from: :day, timezone: nil}} =
             Spec.from_value(%{"kind" => "month", "refresh_from" => "day"})

    assert {:ok, %Spec{kind: :hour, timezone: "Europe/Oslo"}} =
             Spec.from_value(%{kind: :hourly, timezone: "Europe/Oslo"})

    assert {:error, {:unknown_window_spec_fields, ["lookback"]}} =
             Spec.from_value(%{"kind" => "day", "lookback" => 1})

    assert {:error, {:unknown_window_spec_fields, unknown}} = Spec.from_value(%Policy{})
    assert :lookback in unknown
    assert :anchor in unknown
  end

  test "encodes and decodes canonical keys" do
    key = Key.new!(:month, ~U[2026-04-01 00:00:00Z], "Etc/UTC")

    encoded = Key.encode(key)

    assert {:ok, decoded} = Key.decode(encoded)
    assert decoded == key
    assert :ok = Key.validate(key)
  end

  test "encodes and decodes physical range keys without colliding with the first window" do
    exact = Key.new!(:month, ~U[2026-04-01 00:00:00Z], "Etc/UTC")

    range =
      Key.new_range!(
        :month,
        ~U[2026-04-01 00:00:00Z],
        ~U[2026-07-01 00:00:00Z],
        "Etc/UTC"
      )

    refute range == exact
    assert Key.range?(range)
    assert {:ok, ^range} = range |> Key.encode() |> Key.decode()

    assert {:ok, runtime} =
             Runtime.new_range(
               :month,
               ~U[2026-04-01 00:00:00Z],
               ~U[2026-07-01 00:00:00Z],
               exact,
               logical_window_count: 3
             )

    assert runtime.key == range
    assert runtime.logical_window_count == 3
    assert :ok = Runtime.validate(runtime)
  end

  test "window validation reports precise errors" do
    assert {:error, {:invalid_kind, :week}} = Spec.new(:week)
    assert {:error, {:unknown_opt, :lookback}} = Spec.new(:day, lookback: -1)
    assert {:error, {:invalid_lookback, -1}} = Policy.new(:daily, lookback: -1)

    assert {:error, {:invalid_boolean, :combine_windows, :yes}} =
             Policy.new(:daily, combine_windows: :yes)

    assert {:error, {:invalid_refresh_from, :day, :month}} = Spec.new(:day, refresh_from: :month)
    assert {:error, {:unknown_opt, :lookbak}} = Spec.new(:day, lookbak: 1)

    assert {:error, {:duplicate_opt, :timezone}} =
             Spec.new(:day, timezone: "Etc/UTC", timezone: "UTC")

    assert {:error, {:invalid_timezone, "Definitely/NotAZone"}} =
             Spec.new(:day, timezone: "Definitely/NotAZone")

    assert {:error, :invalid_window_bounds} =
             Anchor.new(:day, ~U[2026-04-02 00:00:00Z], ~U[2026-04-01 00:00:00Z])

    assert {:error, {:unknown_opt, :timezome}} =
             Anchor.new(:day, ~U[2026-04-01 00:00:00Z], ~U[2026-04-02 00:00:00Z],
               timezome: "Etc/UTC"
             )

    assert {:error, {:unknown_opt, :timezome}} =
             Runtime.new(
               :day,
               ~U[2026-04-01 00:00:00Z],
               ~U[2026-04-02 00:00:00Z],
               %{kind: :day, start_at_us: 1, timezone: "Etc/UTC"},
               timezome: "Etc/UTC"
             )

    assert {:error, {:invalid_encoded_key, "not-a-key"}} = Key.decode("not-a-key")
  end

  test "pipeline window policy defaults combine windows off and round trips an explicit value" do
    assert {:ok, %Policy{combine_windows: false}} = Policy.new(:monthly)

    assert {:ok, %Policy{combine_windows: true} = policy} =
             Policy.new(:monthly, combine_windows: true)

    assert {:ok, ^policy} = Policy.from_value(Map.from_struct(policy))
  end

  test "bang constructors raise on invalid input" do
    assert_raise ArgumentError, ~r/invalid window key/, fn ->
      Key.new!(:week, ~U[2026-04-01 00:00:00Z], "Etc/UTC")
    end

    assert_raise ArgumentError, ~r/invalid anchor window/, fn ->
      Anchor.new!(:day, ~U[2026-04-02 00:00:00Z], ~U[2026-04-01 00:00:00Z])
    end

    assert_raise ArgumentError, ~r/invalid runtime window/, fn ->
      Runtime.new!(
        :day,
        ~U[2026-04-02 00:00:00Z],
        ~U[2026-04-01 00:00:00Z],
        %{kind: :day, start_at_us: 1, timezone: "Etc/UTC"}
      )
    end

    assert_raise ArgumentError, ~r/invalid window spec/, fn ->
      Spec.new!(:week)
    end
  end

  test "expand_range generates contiguous anchors by kind" do
    assert {:ok, [anchor]} =
             Anchor.expand_range(:day, ~U[2026-04-01 00:00:00Z], ~U[2026-04-02 00:00:00Z])

    assert anchor.kind == :day
    assert anchor.start_at == ~U[2026-04-01 00:00:00Z]
    assert anchor.end_at == ~U[2026-04-02 00:00:00Z]

    assert {:ok, hourly_anchors} =
             Anchor.expand_range(:hour, ~U[2026-04-01 00:00:00Z], ~U[2026-04-01 03:00:00Z])

    assert length(hourly_anchors) == 3

    assert {:ok, monthly_anchors} =
             Anchor.expand_range(:month, ~U[2026-01-01 00:00:00Z], ~U[2026-04-01 00:00:00Z])

    assert length(monthly_anchors) == 3

    assert {:ok, yearly_anchors} =
             Anchor.expand_range(:year, ~U[2024-01-01 00:00:00Z], ~U[2026-01-01 00:00:00Z])

    assert length(yearly_anchors) == 2
  end

  test "pipeline window policies normalize aliases and resolve scheduled anchors" do
    assert {:ok, %Policy{kind: :month, anchor: :previous_complete_period}} =
             Policy.new(:monthly)

    due_at =
      DateTime.from_naive!(~N[2026-05-01 03:00:00], "Europe/Oslo", Favn.Timezone.database!())

    assert {:ok, anchor} =
             Policy.resolve_scheduled(
               Policy.new!(:monthly, timezone: "Europe/Oslo"),
               due_at,
               "Europe/Oslo"
             )

    assert anchor.kind == :month
    assert anchor.timezone == "Europe/Oslo"

    assert DateTime.compare(
             anchor.start_at,
             DateTime.from_naive!(
               ~N[2026-04-01 00:00:00],
               "Europe/Oslo",
               Favn.Timezone.database!()
             )
           ) == :eq

    assert DateTime.compare(
             anchor.end_at,
             DateTime.from_naive!(
               ~N[2026-05-01 00:00:00],
               "Europe/Oslo",
               Favn.Timezone.database!()
             )
           ) == :eq

    assert {:error, {:invalid_window_policy_kind, :weekly}} = Policy.new(:weekly)
  end

  test "current-period scheduled anchors include the incomplete named-timezone period" do
    policy =
      Policy.new!(:monthly,
        anchor: :current_period,
        timezone: "Europe/Oslo"
      )

    due_at =
      DateTime.from_naive!(~N[2026-07-17 02:00:00], "Europe/Oslo", Favn.Timezone.database!())

    assert {:ok, anchor} = Policy.resolve_scheduled(policy, due_at, nil)
    assert anchor.kind == :month
    assert anchor.timezone == "Europe/Oslo"
    assert anchor.start_at == oslo_datetime!(~N[2026-07-01 00:00:00])
    assert anchor.end_at == oslo_datetime!(~N[2026-08-01 00:00:00])

    assert {:ok, ^policy} =
             Policy.from_value(%{
               "kind" => "month",
               "anchor" => "current_period",
               "timezone" => "Europe/Oslo"
             })
  end

  test "current-period hourly anchors remain distinct across repeated DST hours" do
    policy =
      Policy.new!(:hourly,
        anchor: :current_period,
        timezone: "Europe/Oslo"
      )

    assert {:ok, first} =
             Policy.resolve_scheduled(policy, ~U[2026-10-25 00:30:00Z], nil)

    assert {:ok, second} =
             Policy.resolve_scheduled(policy, ~U[2026-10-25 01:30:00Z], nil)

    assert first.start_at == oslo_instant!(~U[2026-10-25 00:00:00Z])
    assert first.end_at == oslo_instant!(~U[2026-10-25 01:00:00Z])
    assert second.start_at == first.end_at
    assert second.end_at == oslo_instant!(~U[2026-10-25 02:00:00Z])
    refute first.key == second.key
  end

  test "scheduled anchors remain distinct at the first day of a local month" do
    due_at = oslo_datetime!(~N[2026-07-01 02:00:00])

    assert {:ok, previous} =
             Policy.resolve_scheduled(
               Policy.new!(:monthly,
                 anchor: :previous_complete_period,
                 timezone: "Europe/Oslo"
               ),
               due_at,
               "Europe/Oslo"
             )

    assert {:ok, current} =
             Policy.resolve_scheduled(
               Policy.new!(:monthly, anchor: :current_period, timezone: "Europe/Oslo"),
               due_at,
               "Europe/Oslo"
             )

    assert previous.start_at == oslo_datetime!(~N[2026-06-01 00:00:00])
    assert previous.end_at == oslo_datetime!(~N[2026-07-01 00:00:00])
    assert current.start_at == oslo_datetime!(~N[2026-07-01 00:00:00])
    assert current.end_at == oslo_datetime!(~N[2026-08-01 00:00:00])
  end

  test "manual window requests parse hour day month and year" do
    assert {:ok, %Request{kind: :hour, value: "2026-04-27T13"}} =
             Request.parse("hour:2026-04-27T13")

    assert {:ok, %Request{kind: :day, value: "2026-04-27"}} =
             Request.parse("day:2026-04-27")

    assert {:ok, %Request{kind: :month, value: "2026-04"}} =
             Request.parse("month:2026-04")

    assert {:ok, %Request{kind: :year, value: "2026"}} = Request.parse("year:2026")

    assert {:ok, request} = Request.parse("month:2026-03", timezone: "Europe/Oslo")
    assert {:ok, anchor} = Policy.resolve_manual(Policy.new!(:monthly), request)

    assert DateTime.compare(
             anchor.start_at,
             DateTime.from_naive!(
               ~N[2026-03-01 00:00:00],
               "Europe/Oslo",
               Favn.Timezone.database!()
             )
           ) == :eq

    assert DateTime.compare(
             anchor.end_at,
             DateTime.from_naive!(
               ~N[2026-04-01 00:00:00],
               "Europe/Oslo",
               Favn.Timezone.database!()
             )
           ) == :eq

    assert {:error, {:window_kind_mismatch, :month, :day}} =
             Policy.resolve_manual(
               Policy.new!(:monthly),
               Request.parse("day:2026-03-01") |> elem(1)
             )

    assert {:error, {:missing_window_request, :month}} =
             Policy.resolve_manual(Policy.new!(:monthly), nil)
  end

  test "manual defaults select the latest complete window at one delayed instant" do
    evaluated_at = oslo_datetime!(~N[2026-07-01 00:30:00])

    assert {:ok, selection} =
             Policy.select_latest_complete(
               Policy.new!(:daily,
                 anchor: :current_period,
                 timezone: "Europe/Oslo"
               ),
               evaluated_at,
               3_600
             )

    assert selection.intent == :manual
    assert selection.expansion == :none
    assert [anchor] = selection.requested_anchors
    assert anchor.start_at == oslo_datetime!(~N[2026-06-29 00:00:00])
    assert anchor.end_at == oslo_datetime!(~N[2026-06-30 00:00:00])

    assert {:ok, nil} = Policy.select_latest_complete(nil, evaluated_at)
  end

  test "day windows use local civil midnights across DST transitions" do
    assert {:ok, spring_request} = Request.parse("day:2026-03-29", timezone: "Europe/Oslo")
    assert {:ok, spring_anchor} = Policy.resolve_manual(Policy.new!(:daily), spring_request)

    assert DateTime.compare(
             spring_anchor.start_at,
             DateTime.from_naive!(
               ~N[2026-03-29 00:00:00],
               "Europe/Oslo",
               Favn.Timezone.database!()
             )
           ) == :eq

    assert DateTime.compare(
             spring_anchor.end_at,
             DateTime.from_naive!(
               ~N[2026-03-30 00:00:00],
               "Europe/Oslo",
               Favn.Timezone.database!()
             )
           ) == :eq

    assert DateTime.diff(spring_anchor.end_at, spring_anchor.start_at, :hour) == 23

    assert {:ok, autumn_request} = Request.parse("day:2026-10-25", timezone: "Europe/Oslo")
    assert {:ok, autumn_anchor} = Policy.resolve_manual(Policy.new!(:daily), autumn_request)

    assert DateTime.compare(
             autumn_anchor.end_at,
             DateTime.from_naive!(
               ~N[2026-10-26 00:00:00],
               "Europe/Oslo",
               Favn.Timezone.database!()
             )
           ) == :eq

    assert DateTime.diff(autumn_anchor.end_at, autumn_anchor.start_at, :hour) == 25
  end

  defp oslo_datetime!(naive) do
    DateTime.from_naive!(naive, "Europe/Oslo", Favn.Timezone.database!())
  end

  defp oslo_instant!(datetime) do
    DateTime.shift_zone!(datetime, "Europe/Oslo", Favn.Timezone.database!())
  end
end
