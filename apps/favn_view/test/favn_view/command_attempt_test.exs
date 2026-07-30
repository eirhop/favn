defmodule FavnView.CommandAttemptTest do
  use ExUnit.Case, async: true

  alias FavnView.CommandAttempt

  doctest CommandAttempt

  test "each intent gets its own key" do
    enable = CommandAttempt.next(nil, "schedule_activation", {"s1", :enable})
    disable = CommandAttempt.next(enable, "schedule_activation", {"s1", :disable})
    other_schedule = CommandAttempt.next(enable, "schedule_activation", {"s2", :enable})

    assert Enum.uniq([enable.key, disable.key, other_schedule.key]) |> length() == 3
  end

  test "two attempts for the same intent never collide by accident" do
    keys =
      Enum.map(1..100, fn index ->
        CommandAttempt.next(nil, "rebuild_start", {"plan", index}).key
      end)

    assert length(Enum.uniq(keys)) == 100
  end

  test "the key names the command it belongs to" do
    assert CommandAttempt.next(nil, "backfill_submit", "target").key
           |> String.starts_with?("backfill_submit:")
  end

  test "a browser-supplied key survives a LiveView remount" do
    key = "run_cancel:browser:01234567-89ab-cdef-0123-456789abcdef"

    assert %CommandAttempt{key: ^key} =
             CommandAttempt.next(nil, "run_cancel", "run-1", %{"idempotency_key" => key})
  end
end
