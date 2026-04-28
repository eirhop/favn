defmodule Favn.WindowTest do
  use ExUnit.Case, async: true

  alias Favn.Window.Anchor
  alias Favn.Window.Key
  alias Favn.Window.Policy
  alias Favn.Window.Request
  alias Favn.Window.Runtime
  alias Favn.Window.Spec

  test "builds canonical window structs and deterministic keys" do
    assert {:ok, %Spec{kind: :hour, lookback: 0, timezone: "Etc/UTC"}} = Spec.new(:hour)
    assert {:ok, %Spec{kind: :day, lookback: 2}} = Spec.new(:day, lookback: 2)
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

  test "encodes and decodes canonical keys" do
    key = Key.new!(:month, ~U[2026-04-01 00:00:00Z], "Etc/UTC")

    encoded = Key.encode(key)

    assert {:ok, decoded} = Key.decode(encoded)
    assert decoded == key
    assert :ok = Key.validate(key)
  end

  test "window validation reports precise errors" do
    assert {:error, {:invalid_kind, :week}} = Spec.new(:week)
    assert {:error, {:invalid_lookback, -1}} = Spec.new(:day, lookback: -1)
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
             Policy.resolve_scheduled(Policy.new!(:monthly), due_at, "Europe/Oslo")

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
end
