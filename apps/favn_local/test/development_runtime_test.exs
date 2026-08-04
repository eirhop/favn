defmodule FavnLocal.DevelopmentRuntimeTest do
  use ExUnit.Case, async: true

  alias FavnLocal.DevelopmentRuntime

  test "the runner restart budget fails the stack after repeated fast exits" do
    now = 1_000_000
    recent = Enum.map(1..5, &(now - &1 * 1_000))

    assert {:fail, ^recent} = DevelopmentRuntime.runner_crash_budget_state(recent, now)
  end

  test "exits outside the crash window do not count against the budget" do
    now = 1_000_000
    old = Enum.map(1..5, &(now - 60_001 - &1))

    assert {:continue, [^now]} = DevelopmentRuntime.runner_crash_budget_state(old, now)
  end

  test "a first exit restarts and starts the window" do
    assert {:continue, [50]} = DevelopmentRuntime.runner_crash_budget_state([], 50)
  end
end
