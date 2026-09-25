defmodule FavnView.CommandAttemptTest do
  use ExUnit.Case, async: true

  alias FavnView.CommandAttempt

  alias Phoenix.LiveView.Socket
  alias Phoenix.LiveView.Utils

  doctest CommandAttempt

  test "invalid windows acknowledge the key but unconfirmed audit completion retains it" do
    attempt = CommandAttempt.next(nil, "pipeline_backfill_submit", "pipeline")
    socket = %Socket{assigns: %{__changed__: %{}}}
    reason = {:invalid_window_value, :month, "2021-31"}

    assert {rejected, nil} = CommandAttempt.settle_failure(socket, attempt, reason)

    assert Utils.get_push_events(rejected) ==
             [["operator-command-terminal", %{idempotency_key: attempt.key}]]

    assert {uncertain, ^attempt} =
             CommandAttempt.settle_failure(
               socket,
               attempt,
               {:operator_audit_incomplete, :unavailable}
             )

    assert Utils.get_push_events(uncertain) == []
  end

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
