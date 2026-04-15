defmodule Favn.WindowAndScheduleTest do
  use ExUnit.Case, async: true

  alias Favn.Triggers.Schedule
  alias Favn.Window.Anchor
  alias Favn.Window.Runtime
  alias Favn.Window.Spec

  test "builds and validates window structures" do
    assert {:ok, spec} = Spec.new(:day, lookback: 1)
    assert :ok = Spec.validate(spec)

    start_at = ~U[2026-01-01 00:00:00Z]
    end_at = ~U[2026-01-02 00:00:00Z]

    assert {:ok, anchor} = Anchor.new(:day, start_at, end_at)
    assert :ok = Anchor.validate(anchor)

    assert {:ok, runtime} = Runtime.new(:day, start_at, end_at, anchor.key)
    assert :ok = Runtime.validate(runtime)
  end

  test "builds schedule and applies default timezone" do
    assert {:ok, unresolved} = Schedule.new_inline(cron: "0 3 * * *")
    assert {:ok, resolved} = Schedule.apply_default_timezone(unresolved, "Etc/UTC")
    assert resolved.timezone == "Etc/UTC"
  end
end
