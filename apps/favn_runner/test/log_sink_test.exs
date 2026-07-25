defmodule FavnRunner.LogSinkTest do
  use ExUnit.Case, async: true

  alias Favn.Log.Entry
  alias FavnRunner.LogSink

  test "normalizes and bounds entries before streaming them" do
    execution_id = "runner-execution"
    message = String.duplicate("x", Entry.max_message_bytes() + 100)

    assert :ok =
             LogSink.emit(self(), execution_id, %{
               source: :runner,
               level: :info,
               message: message,
               occurred_at: DateTime.utc_now()
             })

    assert_receive {:runner_log_entry, ^execution_id, %Entry{} = entry}
    assert byte_size(entry.message) == Entry.max_message_bytes()
    assert entry.truncated
  end
end
