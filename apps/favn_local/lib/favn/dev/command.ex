defmodule Favn.Dev.Command do
  @moduledoc false

  alias Favn.Dev.Process, as: DevProcess

  @default_timeout_ms 300_000
  @max_output_bytes 64 * 1024

  @type status :: non_neg_integer() | :timeout

  @spec run(Path.t(), [String.t()], keyword()) :: {String.t(), status()}
  def run(executable, args, opts)
      when is_binary(executable) and is_list(args) and is_list(opts) do
    timeout_ms = Keyword.get(opts, :timeout_ms, @default_timeout_ms)
    output_writer = Keyword.get(opts, :output_writer, &IO.binwrite/1)

    unless is_integer(timeout_ms) and timeout_ms > 0 do
      raise ArgumentError, "expected :timeout_ms to be a positive integer"
    end

    port = open_port(executable, args, opts)
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    await_exit(port, deadline, output_writer, "")
  rescue
    error -> {Exception.message(error), 127}
  catch
    kind, reason -> {Exception.format(kind, reason, __STACKTRACE__), 127}
  end

  defp open_port(executable, args, opts) do
    port_opts = [
      :binary,
      :exit_status,
      :use_stdio,
      {:args, args}
    ]

    port_opts =
      if Keyword.get(opts, :stderr_to_stdout, false),
        do: [:stderr_to_stdout | port_opts],
        else: port_opts

    port_opts =
      case Keyword.fetch(opts, :cd) do
        {:ok, path} -> [{:cd, String.to_charlist(path)} | port_opts]
        :error -> port_opts
      end

    port_opts =
      case Keyword.fetch(opts, :env) do
        {:ok, env} -> [{:env, encode_env(env)} | port_opts]
        :error -> port_opts
      end

    Port.open({:spawn_executable, String.to_charlist(executable)}, port_opts)
  end

  defp await_exit(port, deadline, output_writer, output) do
    timeout_ms = max(deadline - System.monotonic_time(:millisecond), 0)

    if timeout_ms == 0 do
      terminate(port)
      {output, :timeout}
    else
      receive do
        {^port, {:data, data}} ->
          output_writer.(data)
          await_exit(port, deadline, output_writer, append_output(output, data))

        {^port, {:exit_status, status}} ->
          {output, status}
      after
        timeout_ms ->
          terminate(port)
          {output, :timeout}
      end
    end
  end

  defp terminate(port) do
    case Port.info(port, :os_pid) do
      {:os_pid, pid} when is_integer(pid) and pid > 0 -> DevProcess.stop_pid(pid, 1_000)
      _other -> :ok
    end

    Port.close(port)
  rescue
    _error -> :ok
  catch
    _kind, _reason -> :ok
  end

  defp append_output(output, data) do
    combined = output <> data
    overflow = byte_size(combined) - @max_output_bytes

    if overflow > 0 do
      binary_part(combined, overflow, @max_output_bytes)
    else
      combined
    end
  end

  defp encode_env(env) do
    Enum.map(env, fn {key, value} ->
      {String.to_charlist(key), encode_env_value(value)}
    end)
  end

  defp encode_env_value(nil), do: false
  defp encode_env_value(value) when is_binary(value), do: String.to_charlist(value)
end
