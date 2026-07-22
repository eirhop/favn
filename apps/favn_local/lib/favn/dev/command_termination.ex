defmodule Favn.Dev.CommandTermination do
  @moduledoc false

  @spec stop(pos_integer(), timeout()) :: :ok
  def stop(pid, timeout_ms \\ 1_000)
      when is_integer(pid) and pid > 0 and is_integer(timeout_ms) and timeout_ms >= 0 do
    _ = signal(pid, "-TERM")
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    if wait_dead(pid, deadline) do
      :ok
    else
      _ = signal(pid, "-KILL")
      :ok
    end
  end

  defp wait_dead(pid, deadline) do
    cond do
      not alive?(pid) -> true
      System.monotonic_time(:millisecond) >= deadline -> false
      true ->
        Process.sleep(50)
        wait_dead(pid, deadline)
    end
  end

  defp alive?(pid) do
    case signal(pid, "-0") do
      {_output, 0} -> true
      _not_running -> false
    end
  end

  defp signal(pid, signal) do
    System.cmd(kill_executable(), [signal, Integer.to_string(pid)], stderr_to_stdout: true)
  end

  defp kill_executable, do: System.find_executable("kill") || "/bin/kill"
end
