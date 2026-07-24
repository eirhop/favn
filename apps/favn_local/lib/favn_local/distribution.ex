defmodule FavnLocal.Distribution do
  @moduledoc false

  @spec start(node(), String.t()) :: :ok | {:error, term()}
  def start(name, cookie) when is_atom(name) and is_binary(cookie) do
    with :ok <- ensure_epmd(),
         {:ok, _pid} <- Node.start(name, name_domain: :longnames) do
      Node.set_cookie(String.to_atom(cookie))
      :ok
    end
  end

  defp ensure_epmd do
    case epmd_executable() do
      nil ->
        {:error, :epmd_not_found}

      executable ->
        if epmd_running?(executable), do: :ok, else: start_epmd(executable)
    end
  end

  defp start_epmd(executable) do
    if match?({:win32, _}, :os.type()) do
      port =
        Port.open(
          {:spawn_executable, executable},
          [:binary, :exit_status, :stderr_to_stdout, args: []]
        )

      await_epmd(executable, port, 20)
    else
      case System.cmd(executable, ["-daemon"], stderr_to_stdout: true) do
        {_output, 0} -> await_epmd(executable, nil, 20)
        {output, status} -> {:error, {:epmd_start_failed, status, String.trim(output)}}
      end
    end
  end

  defp await_epmd(_executable, port, 0) do
    if is_port(port) and Port.info(port), do: Port.close(port)
    {:error, {:epmd_start_failed, :not_ready}}
  end

  defp await_epmd(executable, port, attempts) do
    if epmd_running?(executable) do
      :ok
    else
      Process.sleep(25)
      await_epmd(executable, port, attempts - 1)
    end
  end

  defp epmd_running?(executable) do
    match?({_output, 0}, System.cmd(executable, ["-names"], stderr_to_stdout: true))
  end

  defp epmd_executable do
    System.find_executable("epmd") || bundled_epmd()
  end

  defp bundled_epmd do
    [
      List.to_string(:code.root_dir()),
      "erts-#{:erlang.system_info(:version)}",
      "bin",
      epmd_filename()
    ]
    |> Path.join()
    |> existing_file()
  end

  defp epmd_filename do
    if match?({:win32, _}, :os.type()), do: "epmd.exe", else: "epmd"
  end

  defp existing_file(path) do
    if File.regular?(path), do: path
  end
end
