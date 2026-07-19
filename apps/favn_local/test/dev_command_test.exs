defmodule Favn.Dev.CommandTest do
  use ExUnit.Case, async: true

  alias Favn.Dev.Command

  test "run/3 streams and preserves child output" do
    caller = self()
    elixir = System.find_executable("elixir") || flunk("elixir executable not found")
    writer = fn output -> send(caller, {:child_output, output}) end

    assert {"asset installer output\n", 0} =
             Command.run(elixir, ["-e", ~s|IO.puts("asset installer output")|],
               timeout_ms: 10_000,
               output_writer: writer,
               stderr_to_stdout: true
             )

    assert_received {:child_output, streamed_output}
    assert streamed_output =~ "asset installer output"
  end

  test "run/3 terminates a child at the timeout and keeps its output" do
    elixir = System.find_executable("elixir") || flunk("elixir executable not found")
    started_at = System.monotonic_time(:millisecond)

    assert {"started\n", :timeout} =
             Command.run(
               elixir,
               ["-e", ~s|IO.puts("started"); Process.sleep(30_000)|],
               timeout_ms: 2_000,
               output_writer: fn _output -> :ok end,
               stderr_to_stdout: true
             )

    elapsed_ms = System.monotonic_time(:millisecond) - started_at
    assert elapsed_ms < 5_000
  end
end
