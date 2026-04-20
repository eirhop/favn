defmodule Favn.WindowAndScheduleTest do
  use ExUnit.Case, async: true

  alias Favn.Triggers.Schedule
  alias Favn.Triggers.Schedules
  alias Favn.Window.Anchor
  alias Favn.Window.Key
  alias Favn.Window.Runtime
  alias Favn.Window.Spec

  test "builds canonical window structs and deterministic keys" do
    assert {:ok, %Spec{kind: :hour, lookback: 0, timezone: "Etc/UTC"}} = Spec.new(:hour)
    assert {:ok, %Spec{kind: :day, lookback: 2}} = Spec.new(:day, lookback: 2)
    assert {:ok, %Spec{kind: :month, refresh_from: :day}} = Spec.new(:month, refresh_from: :day)

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
  end

  test "schedule constructors apply default timezone and validate refs" do
    assert {:ok, unresolved} = Schedule.new_inline(cron: "0 3 * * *")
    assert {:ok, resolved} = Schedule.apply_default_timezone(unresolved, "Etc/UTC")
    assert resolved.timezone == "Etc/UTC"

    assert {:error, :not_schedule_module} = Schedules.fetch(Favn.Window, :daily)

    module_name = Module.concat(__MODULE__, "NoSchedules#{System.unique_integer([:positive])}")

    Code.compile_string(
      """
      defmodule #{inspect(module_name)} do
        use Favn.Triggers.Schedules
      end
      """,
      "test/window_schedule_test.exs"
    )

    assert {:error, {:schedule_not_found, :missing}} = Schedules.fetch(module_name, :missing)
  end
end
