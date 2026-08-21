defmodule FavnLocal.RunnerProcessLauncher do
  @moduledoc false

  alias FavnLocal.Config
  alias FavnLocal.Distribution

  @type child :: %{
          port: port(),
          node: node() | nil,
          release_id: String.t(),
          runner_instance_id: String.t()
        }

  @spec start(Config.t(), String.t()) :: {:ok, child()} | {:error, term()}
  def start(%Config{} = config, release_id) when is_binary(release_id) do
    with executable when is_binary(executable) <- System.find_executable("elixir"),
         {:ok, resolver_path} <- Distribution.write_runner_resolver(config.root_dir) do
      runner_instance_id = unique_runner_instance_id(release_id)
      runner_node = dynamic_runner_node(config.runner_node)

      args =
        [
          "--name",
          runner_node,
          "--cookie",
          config.distribution_cookie
        ] ++
          code_path_args() ++
          [
            "-e",
            "FavnLocal.RunnerProcess.run(#{inspect(config.root_dir)})"
          ]

      port =
        Port.open(
          {:spawn_executable, executable},
          [
            :binary,
            :exit_status,
            :stderr_to_stdout,
            args: args,
            cd: config.root_dir,
            env: [
              {~c"ERL_INETRC", String.to_charlist(resolver_path)},
              {~c"MIX_ENV", ~c"dev"},
              {~c"FAVN_LOCAL_OPERATOR_NODE",
               config.operator_node |> Atom.to_string() |> String.to_charlist()},
              {~c"FAVN_CONTROL_PLANE_NODE",
               config.operator_node |> Atom.to_string() |> String.to_charlist()},
              {~c"FAVN_RUNNER_POOL", ~c"default"},
              {~c"FAVN_RUNNER_LIFECYCLE_MODE", ~c"resident"},
              {~c"FAVN_RUNNER_INSTANCE_ID", String.to_charlist(runner_instance_id)},
              {~c"FAVN_RUNNER_RELEASE_ID", String.to_charlist(release_id)},
              {~c"FAVN_RUNNER_BUILD_PROFILE", ~c"source"},
              {~c"FAVN_LOG_LEVEL", config.log_level |> Atom.to_string() |> String.to_charlist()}
            ]
          ]
        )

      {:ok,
       %{
         port: port,
         node: nil,
         release_id: release_id,
         runner_instance_id: runner_instance_id
       }}
    else
      {:error, reason} -> {:error, {:runner_resolver_configuration_failed, reason}}
      _missing -> {:error, {:missing_tool, "elixir"}}
    end
  rescue
    error -> {:error, {:runner_start_failed, Exception.message(error)}}
  end

  @spec stop(child()) :: :ok
  def stop(%{node: runner_node}) when is_atom(runner_node) and not is_nil(runner_node) do
    if Node.ping(runner_node) == :pong do
      :erpc.cast(runner_node, FavnLocal.RunnerProcess, :stop, [])
    end

    :ok
  end

  def stop(%{node: nil, port: port}), do: close_port(port)

  @doc """
  Resolves the bounded OTP-assigned node atom from the registered runner PID.

  The launcher never creates an atom for a runner name. OTP allocates and
  reuses dynamic names on the stable host alias when the runner explicitly
  connects to the control plane.
  """
  @spec refresh_registration(child()) :: {:ok, child()} | :not_ready
  def refresh_registration(
        %{
          release_id: release_id,
          runner_instance_id: runner_instance_id
        } = child
      ) do
    with {:ok,
          %{
            required_runner_release_id: ^release_id,
            runner_pool: "default",
            lifecycle_mode: :resident,
            agent_pid: agent_pid
          }} <- FavnOrchestrator.RunnerRegistry.fetch(runner_instance_id) do
      {:ok, %{child | node: node(agent_pid)}}
    else
      _not_ready -> :not_ready
    end
  catch
    _kind, _reason -> :not_ready
  end

  defp unique_runner_instance_id(release_id) do
    suffix = System.unique_integer([:positive, :monotonic])
    "favn-local-#{String.slice(release_id, 0, 11)}-#{suffix}"
  end

  defp dynamic_runner_node(base_node) do
    [_base_name, host] = base_node |> Atom.to_string() |> String.split("@", parts: 2)
    "undefined@#{host}"
  end

  defp code_path_args do
    :code.get_path()
    |> Enum.map(&List.to_string/1)
    |> Enum.filter(&File.dir?/1)
    |> Enum.flat_map(&["-pa", &1])
  end

  defp close_port(port) when is_port(port) do
    _ = Port.close(port)
    :ok
  catch
    :error, :badarg -> :ok
  end
end
