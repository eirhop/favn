defmodule Favn.Dev.Process do
  @moduledoc false

  @type service_spec :: %{
          required(:name) => String.t(),
          required(:exec) => Path.t(),
          required(:args) => [String.t()],
          required(:cwd) => Path.t(),
          required(:log_path) => Path.t(),
          optional(:env) => %{optional(String.t()) => String.t()}
        }

  @spec start_service(service_spec()) :: {:ok, map()} | {:error, term()}
  def start_service(spec) when is_map(spec) do
    parent = self()
    pid = spawn_link(fn -> run_service(parent, spec) end)

    receive do
      {:service_started, ^pid, info} -> {:ok, Map.put(info, :wrapper_pid, pid)}
      {:service_start_failed, ^pid, reason} -> {:error, reason}
    after
      5_000 -> {:error, :service_start_timeout}
    end
  end

  @spec stop_pid(integer(), timeout()) :: :ok
  def stop_pid(pid, timeout_ms \\ 10_000)
      when is_integer(pid) and pid > 0 and is_integer(timeout_ms) do
    _ = send_terminate(pid)

    deadline = System.monotonic_time(:millisecond) + timeout_ms

    if wait_dead(pid, deadline) do
      :ok
    else
      _ = send_kill(pid)
      :ok
    end
  end

  @spec alive?(integer()) :: boolean()
  def alive?(pid) when is_integer(pid) and pid > 0 do
    case :os.type() do
      {:unix, _} -> unix_alive?(pid)
      {:win32, _} -> windows_alive?(pid)
    end
  end

  defp send_terminate(pid) do
    case :os.type() do
      {:unix, _} ->
        System.cmd(unix_kill(), ["-TERM", Integer.to_string(pid)], stderr_to_stdout: true)

      {:win32, _} ->
        System.cmd("taskkill", ["/PID", Integer.to_string(pid)], stderr_to_stdout: true)
    end
  end

  defp send_kill(pid) do
    case :os.type() do
      {:unix, _} ->
        System.cmd(unix_kill(), ["-KILL", Integer.to_string(pid)], stderr_to_stdout: true)

      {:win32, _} ->
        System.cmd("taskkill", ["/PID", Integer.to_string(pid), "/T", "/F"],
          stderr_to_stdout: true
        )
    end
  end

  defp unix_alive?(pid) do
    case System.cmd(unix_kill(), ["-0", Integer.to_string(pid)], stderr_to_stdout: true) do
      {_out, 0} -> true
      {_out, _status} -> false
    end
  end

  defp unix_kill do
    System.find_executable("kill") || "/bin/kill"
  end

  defp windows_alive?(pid) do
    case System.cmd("tasklist", ["/FI", "PID eq #{pid}", "/FO", "CSV", "/NH"],
           stderr_to_stdout: true
         ) do
      {output, 0} ->
        normalized = output |> String.trim() |> String.downcase()
        normalized != "" and not String.starts_with?(normalized, "info:")

      {_output, _status} ->
        false
    end
  end

  defp wait_dead(pid, deadline_ms) do
    cond do
      not alive?(pid) ->
        true

      System.monotonic_time(:millisecond) >= deadline_ms ->
        false

      true ->
        Process.sleep(100)
        wait_dead(pid, deadline_ms)
    end
  end

  defp run_service(parent, spec) do
    %{name: name, exec: exec, args: args, cwd: cwd, log_path: log_path} = spec
    env = Map.get(spec, :env, %{})

    try do
      case File.open(log_path, [:append, :binary]) do
        {:ok, io} ->
          port_opts = [
            :binary,
            :exit_status,
            :use_stdio,
            :stderr_to_stdout,
            :hide,
            {:args, args},
            {:cd, String.to_charlist(cwd)},
            {:env, encode_env(env)}
          ]

          port = Port.open({:spawn_executable, String.to_charlist(exec)}, port_opts)
          os_pid = port |> Port.info(:os_pid) |> normalize_os_pid()

          send(parent, {:service_started, self(), %{name: name, pid: os_pid, log_path: log_path}})
          service_loop(parent, name, port, io)

        {:error, reason} ->
          send(parent, {:service_start_failed, self(), {:log_open_failed, reason}})
      end
    rescue
      error in ErlangError ->
        send(parent, {:service_start_failed, self(), {:port_open_failed, error.original}})
    catch
      kind, reason ->
        send(parent, {:service_start_failed, self(), {:port_open_failed, {kind, reason}}})
    end
  end

  defp service_loop(parent, name, port, io) do
    receive do
      {^port, {:data, data}} ->
        :ok = IO.binwrite(io, data)
        service_loop(parent, name, port, io)

      {^port, {:exit_status, status}} ->
        _ = File.close(io)
        send(parent, {:service_exit, name, status})

      _other ->
        service_loop(parent, name, port, io)
    end
  end

  defp normalize_os_pid({:os_pid, pid}) when is_integer(pid), do: pid
  defp normalize_os_pid(_other), do: -1

  defp encode_env(env_map) when is_map(env_map) do
    Enum.map(env_map, fn {key, value} ->
      {String.to_charlist(key), String.to_charlist(value)}
    end)
  end
end
