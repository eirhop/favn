defmodule FavnLocal.Distribution do
  @moduledoc false

  @local_address {127, 0, 0, 2}
  @local_host_alias "favn-local.test"
  @local_host_alias_chars ~c"favn-local.test"
  @resolver_config """
  {lookup, [file, native]}.
  {host, {127, 0, 0, 2}, ["favn-local.test"]}.
  """

  @doc false
  @spec local_host_alias() :: String.t()
  def local_host_alias, do: @local_host_alias

  @doc false
  @spec write_runner_resolver(Path.t()) :: {:ok, Path.t()} | {:error, term()}
  def write_runner_resolver(root_dir) do
    path = Path.join([Path.expand(root_dir), ".favn", "local", "inetrc"])

    with :ok <- File.mkdir_p(Path.dirname(path)),
         :ok <- File.write(path, @resolver_config) do
      {:ok, path}
    end
  end

  @spec start(node(), String.t()) :: :ok | {:error, term()}
  def start(name, cookie) when is_atom(name) and is_binary(cookie) do
    with :ok <- ensure_local_resolution(),
         :ok <- ensure_epmd(),
         {:ok, _pid} <- Node.start(name, name_domain: :longnames) do
      Node.set_cookie(String.to_atom(cookie))
      :ok
    end
  end

  @doc false
  @spec ensure_local_resolution() :: :ok | {:error, term()}
  def ensure_local_resolution do
    lookup = :inet_db.res_option(:lookup)

    with true <- is_list(lookup),
         :ok <- :inet_db.set_lookup(Enum.uniq([:file | lookup])),
         :ok <- :inet_db.add_host(@local_address, [@local_host_alias_chars]) do
      :ok
    else
      false -> {:error, :invalid_resolver_lookup}
      reason -> {:error, {:local_resolver_configuration_failed, reason}}
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
