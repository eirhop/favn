defmodule Favn.WindowTest do
  use ExUnit.Case, async: true

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
  end

  test "returns validation errors for invalid inputs" do
    assert {:error, {:invalid_kind, :week}} = Spec.new(:week)

    assert {:error, {:invalid_lookback, -1}} = Spec.new(:day, lookback: -1)

    assert {:error, :invalid_window_bounds} =
             Anchor.new(:day, ~U[2026-04-02 00:00:00Z], ~U[2026-04-01 00:00:00Z])

    assert {:error, {:invalid_encoded_key, "not-a-key"}} = Key.decode("not-a-key")
  end
end
