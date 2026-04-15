defmodule Favn.WindowTest do
  use ExUnit.Case, async: true

  alias Favn.Triggers.Schedules
  alias Favn.Window
  alias Favn.Window.{Anchor, Key, Runtime, Spec}

  test "builds canonical spec constructors" do
    assert %Spec{kind: :hour, lookback: 0, timezone: "Etc/UTC"} = Window.hourly()
    assert %Spec{kind: :day, lookback: 2} = Window.daily(lookback: 2)
    assert %Spec{kind: :month, refresh_from: :day} = Window.monthly(refresh_from: :day)
  end

  test "anchor and runtime carry deterministic keys" do
    start_at = ~U[2026-04-01 00:00:00Z]
    end_at = ~U[2026-04-02 00:00:00Z]

    assert %Anchor{} = anchor = Window.anchor(:day, start_at, end_at)

    assert anchor.key == %{
             kind: :day,
             start_at_us: DateTime.to_unix(start_at, :microsecond),
             timezone: "Etc/UTC"
           }

    assert %Runtime{} = runtime = Window.runtime(:day, start_at, end_at, anchor.key)
    assert runtime.key == anchor.key
    assert runtime.anchor_key == anchor.key
  end

  test "encodes and decodes window keys" do
    key = Key.new!(:month, ~U[2026-04-01 00:00:00Z], "Etc/UTC")

    encoded = Key.encode(key)

    assert {:ok, decoded} = Key.decode(encoded)
    assert decoded == key
    assert :ok = Key.validate(key)
  end

  test "Key.new! raises on invalid input" do
    assert_raise ArgumentError, ~r/invalid window key/, fn ->
      Key.new!(:week, ~U[2026-04-01 00:00:00Z], "Etc/UTC")
    end

    assert_raise ArgumentError, ~r/invalid window key/, fn ->
      Key.new!(:day, ~U[2026-04-01 00:00:00Z], "Invalid/Zone")
    end
  end

  test "Key.from_window extracts key from Anchor and Runtime" do
    start_at = ~U[2026-04-01 00:00:00Z]
    end_at = ~U[2026-04-02 00:00:00Z]

    anchor = Window.anchor(:day, start_at, end_at)
    assert Key.from_window(anchor) == anchor.key

    runtime = Window.runtime(:day, start_at, end_at, anchor.key)
    assert Key.from_window(runtime) == runtime.key
  end

  test "returns validation errors for invalid inputs" do
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

  test "Runtime.new! raises on invalid input" do
    assert_raise ArgumentError, "invalid runtime window: :invalid_window_bounds", fn ->
      Runtime.new!(
        :day,
        ~U[2026-04-02 00:00:00Z],
        ~U[2026-04-01 00:00:00Z],
        %{kind: :day, start_at_us: 1, timezone: "Etc/UTC"}
      )
    end
  end

  test "Runtime.validate/1 validates struct correctness" do
    start_at = ~U[2026-04-01 00:00:00Z]
    end_at = ~U[2026-04-02 00:00:00Z]
    anchor = Window.anchor(:day, start_at, end_at)

    runtime = Window.runtime(:day, start_at, end_at, anchor.key)

    assert :ok = Runtime.validate(runtime)
  end

  test "Runtime.validate/1 returns error for mismatched key" do
    start_at = ~U[2026-04-01 00:00:00Z]
    end_at = ~U[2026-04-02 00:00:00Z]
    anchor = Window.anchor(:day, start_at, end_at)

    runtime = %Runtime{
      kind: :day,
      start_at: start_at,
      end_at: end_at,
      timezone: "Etc/UTC",
      key: %{kind: :day, start_at_us: 123, timezone: "Etc/UTC"},
      anchor_key: anchor.key
    }

    assert {:error, :invalid_key} = Runtime.validate(runtime)
  end

  test "Runtime.validate/1 returns error for invalid kind" do
    start_at = ~U[2026-04-01 00:00:00Z]
    end_at = ~U[2026-04-02 00:00:00Z]
    anchor = Window.anchor(:day, start_at, end_at)

    runtime = %Runtime{
      kind: :week,
      start_at: start_at,
      end_at: end_at,
      timezone: "Etc/UTC",
      key: anchor.key,
      anchor_key: anchor.key
    }

    assert {:error, {:invalid_kind, :week}} = Runtime.validate(runtime)
  end

  test "Anchor.validate/1 validates struct correctness" do
    start_at = ~U[2026-04-01 00:00:00Z]
    end_at = ~U[2026-04-02 00:00:00Z]
    anchor = Window.anchor(:day, start_at, end_at)

    assert :ok = Anchor.validate(anchor)
  end

  test "Anchor.validate/1 returns error for invalid key" do
    anchor = %Anchor{
      kind: :day,
      start_at: ~U[2026-04-01 00:00:00Z],
      end_at: ~U[2026-04-02 00:00:00Z],
      timezone: "Etc/UTC",
      key: %{kind: :day, start_at_us: 999, timezone: "Etc/UTC"}
    }

    assert {:error, :invalid_key} = Anchor.validate(anchor)
  end

  test "Key.decode returns error for invalid kind" do
    assert {:error, {:invalid_kind, "invalid"}} =
             Key.decode("invalid:Etc/UTC:2026-04-01T00:00:00Z")
  end

  test "Key.decode returns error for invalid encoded key format" do
    assert {:error, {:invalid_kind, "a"}} = Key.decode("a:b:c:d")
  end

  test "Spec.validate/1 validates struct" do
    spec = Window.daily()
    assert :ok = Spec.validate(spec)
  end

  test "Spec.validate/1 returns error for invalid lookback" do
    spec = %Spec{kind: :day, lookback: -1, timezone: "Etc/UTC"}
    assert {:error, {:invalid_lookback, -1}} = Spec.validate(spec)
  end

  test "Spec.new! raises on invalid input" do
    assert_raise ArgumentError, ~r/invalid window spec/, fn ->
      Spec.new!(:week)
    end
  end

  test "Spec.new with different refresh_from options" do
    assert {:ok, %Spec{refresh_from: :hour}} = Spec.new(:hour, refresh_from: :hour)
    assert {:ok, %Spec{refresh_from: nil}} = Spec.new(:hour, refresh_from: nil)

    assert {:ok, %Spec{refresh_from: :hour}} = Spec.new(:day, refresh_from: :hour)
    assert {:ok, %Spec{refresh_from: :day}} = Spec.new(:day, refresh_from: :day)
    assert {:ok, %Spec{refresh_from: nil}} = Spec.new(:day, refresh_from: nil)

    assert {:ok, %Spec{refresh_from: :day}} = Spec.new(:month, refresh_from: :day)
    assert {:ok, %Spec{refresh_from: :month}} = Spec.new(:month, refresh_from: :month)
  end

  test "Spec.validate with different refresh_from values" do
    assert :ok = Spec.validate(%Spec{kind: :hour, refresh_from: :hour, timezone: "Etc/UTC"})
    assert :ok = Spec.validate(%Spec{kind: :hour, refresh_from: nil, timezone: "Etc/UTC"})

    assert {:error, {:invalid_refresh_from, :hour, :day}} =
             Spec.validate(%Spec{kind: :hour, refresh_from: :day, timezone: "Etc/UTC"})

    assert :ok = Spec.validate(%Spec{kind: :day, refresh_from: :hour, timezone: "Etc/UTC"})
    assert :ok = Spec.validate(%Spec{kind: :day, refresh_from: :day, timezone: "Etc/UTC"})

    assert {:error, {:invalid_refresh_from, :day, :month}} =
             Spec.validate(%Spec{kind: :day, refresh_from: :month, timezone: "Etc/UTC"})

    assert :ok = Spec.validate(%Spec{kind: :month, refresh_from: :day, timezone: "Etc/UTC"})
    assert :ok = Spec.validate(%Spec{kind: :month, refresh_from: :month, timezone: "Etc/UTC"})

    assert {:error, {:invalid_refresh_from, :month, :hour}} =
             Spec.validate(%Spec{kind: :month, refresh_from: :hour, timezone: "Etc/UTC"})
  end

  test "Schedules.fetch returns error for non-schedule module" do
    assert {:error, :not_schedule_module} = Schedules.fetch(Favn.Window, :daily)
  end

  test "Schedules.fetch returns error for schedule not found" do
    defmodule NoSchedules do
      use Favn.Triggers.Schedules
    end

    assert {:error, {:schedule_not_found, :nonexistent}} =
             Schedules.fetch(NoSchedules, :nonexistent)
  end

  test "Anchor.new! raises on invalid input" do
    assert_raise ArgumentError, ~r/invalid anchor window/, fn ->
      Anchor.new!(:day, ~U[2026-04-02 00:00:00Z], ~U[2026-04-01 00:00:00Z])
    end
  end

  test "Anchor.expand_range generates correct anchors for different kinds" do
    start = ~U[2026-04-01 00:00:00Z]
    ending = ~U[2026-04-02 00:00:00Z]

    assert {:ok, [anchor]} = Anchor.expand_range(:day, start, ending)
    assert anchor.kind == :day
    assert anchor.start_at == ~U[2026-04-01 00:00:00Z]
    assert anchor.end_at == ~U[2026-04-02 00:00:00Z]

    assert {:ok, [_, _, _]} =
             Anchor.expand_range(:day, ~U[2026-04-01 00:00:00Z], ~U[2026-04-04 00:00:00Z])
  end

  test "Anchor.expand_range with hourly windows" do
    start = ~U[2026-04-01 00:00:00Z]
    ending = ~U[2026-04-01 03:00:00Z]

    assert {:ok, anchors} = Anchor.expand_range(:hour, start, ending)
    assert length(anchors) == 3

    assert Enum.all?(anchors, fn a -> a.kind == :hour end)
  end

  test "Anchor.expand_range with monthly windows" do
    start = ~U[2026-01-01 00:00:00Z]
    ending = ~U[2026-04-01 00:00:00Z]

    assert {:ok, anchors} = Anchor.expand_range(:month, start, ending)
    assert length(anchors) == 3
  end
end
