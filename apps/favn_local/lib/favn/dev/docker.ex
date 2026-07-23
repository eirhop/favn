defmodule Favn.Dev.Docker do
  @moduledoc """
  Bounded Docker Engine and Compose v2 command boundary for local tooling.

  Authentication remains owned by Docker's configured credential store. Favn
  never accepts registry credentials and never adds secret values to command
  arguments.
  """

  alias Favn.Dev.{Command, ComposeDeployment, OutputRedactor}

  @default_timeout_ms 120_000
  @max_output_bytes 8_192
  @favn_compose_roles ~w(postgres control-plane-ops control-plane-verify runner control-plane)

  @type image :: %{
          id: String.t(),
          repo_digests: [String.t()],
          architecture: String.t(),
          os: String.t(),
          user: String.t(),
          labels: map()
        }

  @type opts :: keyword()

  @doc "Verifies a reachable Linux amd64 Docker Engine and Compose v2 plugin."
  @spec probe(opts()) :: {:ok, map()} | {:error, term()}
  def probe(opts \\ []) when is_list(opts) do
    with {:ok, engine} <- probe_engine(opts),
         {:ok, compose_version} <- compose_version(engine.executable, opts) do
      {:ok, Map.put(engine, :compose_version, compose_version)}
    end
  end

  @doc "Verifies only the Docker Engine target required for image installation."
  @spec probe_engine(opts()) :: {:ok, map()} | {:error, term()}
  def probe_engine(opts \\ []) when is_list(opts) do
    with {:ok, host} <- supported_host(opts),
         {:ok, executable} <- executable(opts),
         {:ok, server} <- docker_server(executable, opts),
         :ok <- supported_server(server) do
      {:ok,
       %{
         executable: executable,
         host_environment: host.environment,
         host_architecture: host.architecture,
         server_os: server["Os"],
         server_architecture: server["Arch"],
         server_version: server["Version"]
       }}
    end
  end

  @doc "Verifies only the Docker Compose v2 command required by local deployment."
  @spec probe_compose(opts()) :: :ok | {:error, term()}
  def probe_compose(opts \\ []) when is_list(opts) do
    with {:ok, executable} <- executable(opts),
         {:ok, _version} <- compose_version(executable, opts) do
      :ok
    end
  end

  @doc "Pulls one image reference through Docker's existing credential configuration."
  @spec pull(String.t(), opts()) :: :ok | {:error, term()}
  def pull(reference, opts \\ []) when is_binary(reference) and is_list(opts) do
    with {:ok, executable} <- executable(opts) do
      case command(executable, ["pull", reference], opts, @default_timeout_ms) do
        {_output, 0} -> :ok
        {output, status} -> pull_error(reference, status, output, opts)
      end
    end
  end

  @doc "Reads one exact local image record without exposing its environment."
  @spec inspect_image(String.t(), opts()) :: {:ok, image()} | {:error, term()}
  def inspect_image(reference, opts \\ []) when is_binary(reference) and is_list(opts) do
    with {:ok, executable} <- executable(opts),
         {output, 0} <-
           command(
             executable,
             ["image", "inspect", reference],
             opts,
             Keyword.get(opts, :docker_inspect_timeout_ms, 30_000)
           ),
         {:ok, [inspection]} <- JSON.decode(output),
         {:ok, image} <- normalize_image(inspection) do
      {:ok, image}
    else
      {_output, status} when is_integer(status) ->
        {:error, {:docker_image_unavailable, reference}}

      {:ok, _invalid} ->
        {:error, {:invalid_docker_image_inspection, reference}}

      {:error, _reason} ->
        {:error, {:invalid_docker_image_inspection, reference}}
    end
  end

  @doc false
  @spec inspect_container_state(String.t(), opts()) :: {:ok, map()} | {:error, term()}
  def inspect_container_state(container, opts \\ [])
      when is_binary(container) and container != "" and is_list(opts) do
    with {:ok, executable} <- executable(opts),
         {output, 0} <-
           command(
             executable,
             [
               "container",
               "inspect",
               "--format",
               ~s({"state":{{json .State}},"restart_count":{{.RestartCount}}}),
               container
             ],
             opts,
             Keyword.get(opts, :docker_inspect_timeout_ms, 30_000)
           ),
         {:ok, state} when is_map(state) <- JSON.decode(String.trim(output)) do
      {:ok, OutputRedactor.redact_term(state, opts)}
    else
      _invalid -> {:error, :container_state_unavailable}
    end
  end

  @doc false
  @spec project_role_containers(String.t(), opts()) :: {:ok, [map()]} | {:error, term()}
  def project_role_containers(project_name, opts \\ [])
      when is_binary(project_name) and project_name != "" and is_list(opts) do
    with {:ok, executable} <- executable(opts),
         {output, 0} <-
           command(
             executable,
             [
               "container",
               "ls",
               "--all",
               "--quiet",
               "--filter",
               "label=com.docker.compose.project=#{project_name}",
               "--filter",
               "label=io.favn.compose.contract-version=1",
               "--filter",
               "label=io.favn.compose.profile=local",
               "--filter",
               "label=io.favn.compose.role"
             ],
             opts,
             Keyword.get(opts, :docker_inspect_timeout_ms, 30_000)
           ),
         ids <- String.split(output, "\n", trim: true),
         {:ok, containers} <- inspect_project_role_containers(executable, project_name, ids, opts) do
      {:ok, containers}
    else
      {output, status} when is_integer(status) ->
        {:error, {:project_role_discovery_failed, status, safe_bounded(output, opts)}}

      {:error, _reason} = error ->
        error
    end
  end

  @doc false
  @spec stop_containers([map()], non_neg_integer(), opts()) :: :ok | {:error, term()}
  def stop_containers(containers, timeout_seconds, opts \\ [])
      when is_list(containers) and is_integer(timeout_seconds) and timeout_seconds >= 0 and
             is_list(opts) do
    ids =
      containers
      |> Enum.filter(& &1.running?)
      |> Enum.map(& &1.id)
      |> Enum.uniq()

    case {ids, executable(opts)} do
      {[], _result} ->
        :ok

      {ids, {:ok, executable}} ->
        case command(
               executable,
               ["container", "stop", "--time", Integer.to_string(timeout_seconds) | ids],
               opts,
               Keyword.get(opts, :compose_command_timeout_ms, @default_timeout_ms)
             ) do
          {_output, 0} ->
            :ok

          {output, status} ->
            {:error, {:project_role_stop_failed, status, safe_bounded(output, opts)}}
        end

      {_ids, {:error, _reason} = error} ->
        error
    end
  end

  @doc "Runs Docker Compose with one typed, validated deployment identity."
  @spec compose(ComposeDeployment.t(), [String.t()], opts()) ::
          {String.t(), non_neg_integer() | :timeout}
  def compose(%ComposeDeployment{} = deployment, args, opts \\ [])
      when is_list(args) and is_list(opts) do
    result =
      case executable(opts) do
        {:ok, executable} ->
          command(
            executable,
            [
              "compose",
              "--project-name",
              deployment.project_name,
              "--file",
              deployment.compose_file,
              "--env-file",
              deployment.env_file
              | args
            ],
            opts,
            Keyword.get(opts, :compose_command_timeout_ms, @default_timeout_ms)
          )

        {:error, reason} ->
          {inspect(reason), 127}
      end

    redact_result(result, opts)
  end

  @doc false
  @spec render_compose(String.t(), Path.t(), Path.t(), opts()) ::
          {String.t(), non_neg_integer() | :timeout}
  def render_compose(project_name, compose_file, env_file, opts \\ [])
      when is_binary(project_name) and is_binary(compose_file) and is_binary(env_file) and
             is_list(opts) do
    result =
      case executable(opts) do
        {:ok, executable} ->
          command(
            executable,
            [
              "compose",
              "--project-name",
              project_name,
              "--file",
              compose_file,
              "--env-file",
              env_file,
              "--profile",
              "*",
              "config",
              "--format",
              "json"
            ],
            opts,
            Keyword.get(opts, :compose_command_timeout_ms, @default_timeout_ms)
          )

        {:error, reason} ->
          {inspect(reason), 127}
      end

    redact_result(result, opts)
  end

  defp inspect_project_role_containers(_executable, _project_name, [], _opts), do: {:ok, []}

  defp inspect_project_role_containers(executable, project_name, ids, opts) do
    with {output, 0} <-
           command(
             executable,
             ["container", "inspect" | ids],
             opts,
             Keyword.get(opts, :docker_inspect_timeout_ms, 30_000)
           ),
         {:ok, inspections} when is_list(inspections) <- JSON.decode(output),
         {:ok, containers} <- normalize_project_role_containers(inspections, project_name, ids) do
      {:ok, containers}
    else
      {output, status} when is_integer(status) ->
        {:error, {:project_role_inspection_failed, status, safe_bounded(output, opts)}}

      _invalid ->
        {:error, :invalid_project_role_containers}
    end
  end

  defp normalize_project_role_containers(inspections, project_name, expected_ids) do
    Enum.reduce_while(inspections, {:ok, []}, fn inspection, {:ok, containers} ->
      case normalize_project_role_container(inspection, project_name) do
        {:ok, container} -> {:cont, {:ok, [container | containers]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, containers} ->
        actual_ids = Enum.map(containers, & &1.id)

        if length(Enum.uniq(actual_ids)) == length(expected_ids) and
             Enum.all?(expected_ids, fn expected ->
               Enum.any?(actual_ids, &String.starts_with?(&1, expected))
             end),
           do: {:ok, Enum.sort_by(containers, &{&1.role, &1.name})},
           else: {:error, :invalid_project_role_containers}

      {:error, _reason} = error ->
        error
    end
  end

  defp normalize_project_role_container(
         %{
           "Id" => id,
           "Name" => name,
           "Config" => %{"Labels" => labels},
           "State" => %{"Running" => running?}
         },
         project_name
       )
       when is_binary(id) and id != "" and is_binary(name) and is_map(labels) and
              is_boolean(running?) do
    role = labels["io.favn.compose.role"]

    if labels["com.docker.compose.project"] == project_name and
         labels["io.favn.compose.contract-version"] == "1" and
         labels["io.favn.compose.profile"] == "local" and role in @favn_compose_roles do
      {:ok,
       %{
         id: id,
         name: String.trim_leading(name, "/"),
         role: role,
         running?: running?
       }}
    else
      {:error, :invalid_project_role_containers}
    end
  end

  defp normalize_project_role_container(_inspection, _project_name),
    do: {:error, :invalid_project_role_containers}

  defp executable(opts) do
    executable =
      case Keyword.fetch(opts, :docker_executable) do
        {:ok, value} -> value
        :error -> System.find_executable("docker")
      end

    case executable do
      value when is_binary(value) and value != "" -> {:ok, value}
      _missing -> {:error, {:missing_tool, "docker"}}
    end
  end

  defp supported_host(opts) do
    host = host_platform(opts)

    if host.os == :linux and host.architecture == "amd64" do
      {:ok, host}
    else
      {:error, {:unsupported_docker_host, host.os, host.architecture}}
    end
  end

  defp host_platform(opts) do
    case {Mix.env(), Keyword.get(opts, :docker_host_platform)} do
      {:test, %{os: os, architecture: architecture, environment: environment}} ->
        %{os: os, architecture: architecture, environment: environment}

      _runtime ->
        {family, name} = :os.type()
        os = if family == :unix and name == :linux, do: :linux, else: name

        %{
          os: os,
          architecture: host_architecture(),
          environment: if(os == :linux and wsl2?(), do: :wsl2, else: os)
        }
    end
  end

  defp host_architecture do
    architecture = :erlang.system_info(:system_architecture) |> to_string() |> String.downcase()

    cond do
      String.starts_with?(architecture, "x86_64") -> "amd64"
      String.starts_with?(architecture, "amd64") -> "amd64"
      String.starts_with?(architecture, "aarch64") -> "arm64"
      String.starts_with?(architecture, "arm64") -> "arm64"
      true -> architecture
    end
  end

  defp wsl2? do
    is_binary(System.get_env("WSL_INTEROP")) or
      case File.read("/proc/sys/kernel/osrelease") do
        {:ok, release} -> release |> String.downcase() |> String.contains?("microsoft")
        {:error, _reason} -> false
      end
  end

  defp docker_server(executable, opts) do
    case command(
           executable,
           ["version", "--format", "{{json .Server}}"],
           opts,
           Keyword.get(opts, :docker_probe_timeout_ms, 15_000)
         ) do
      {output, 0} ->
        case JSON.decode(String.trim(output)) do
          {:ok, server} when is_map(server) -> {:ok, server}
          _invalid -> {:error, :invalid_docker_server_response}
        end

      {output, status} ->
        {:error, {:docker_engine_unavailable, status, safe_bounded(output, opts)}}
    end
  end

  defp supported_server(%{"Os" => "linux", "Arch" => "amd64"}), do: :ok

  defp supported_server(%{"Os" => os, "Arch" => arch}),
    do: {:error, {:unsupported_docker_server, os, arch}}

  defp supported_server(_server), do: {:error, :invalid_docker_server_response}

  defp compose_version(executable, opts) do
    case command(
           executable,
           ["compose", "version", "--short"],
           opts,
           Keyword.get(opts, :docker_probe_timeout_ms, 15_000)
         ) do
      {output, 0} ->
        version = output |> String.trim() |> String.trim_leading("v")

        case Version.parse(version) do
          {:ok, %Version{major: major}} when major >= 2 -> {:ok, version}
          _unsupported -> {:error, {:unsupported_compose_version, safe_bounded(output, opts)}}
        end

      {output, status} ->
        {:error, {:docker_compose_unavailable, status, safe_bounded(output, opts)}}
    end
  end

  defp normalize_image(%{
         "Id" => id,
         "RepoDigests" => repo_digests,
         "Architecture" => architecture,
         "Os" => os,
         "Config" => config
       })
       when is_binary(id) and is_list(repo_digests) and is_binary(architecture) and
              is_binary(os) and is_map(config) do
    labels = Map.get(config, "Labels") || %{}
    user = Map.get(config, "User") || ""

    if is_map(labels) and is_binary(user) and Enum.all?(repo_digests, &is_binary/1) do
      {:ok,
       %{
         id: id,
         repo_digests: repo_digests,
         architecture: architecture,
         os: os,
         user: user,
         labels: labels
       }}
    else
      {:error, :invalid_image_shape}
    end
  end

  defp normalize_image(_inspection), do: {:error, :invalid_image_shape}

  defp pull_error(reference, status, output, opts) do
    normalized = String.downcase(output)

    cond do
      String.contains?(normalized, "unauthorized") or
        String.contains?(normalized, "authentication required") or
          String.contains?(normalized, "denied") ->
        {:error, :control_plane_registry_authentication_required}

      String.contains?(normalized, "manifest unknown") or
          String.contains?(normalized, "not found") ->
        {:error, {:control_plane_version_unavailable, reference}}

      true ->
        {:error, {:control_plane_pull_failed, status, safe_bounded(output, opts)}}
    end
  end

  defp command(executable, args, opts, timeout_ms) do
    runner = Keyword.get(opts, :docker_command_runner)
    sink = Keyword.get(opts, :docker_output_writer, fn _chunk -> :ok end)
    {output_writer, flush} = OutputRedactor.stream_writer(opts, sink)

    command_opts = [
      stderr_to_stdout: true,
      timeout_ms: timeout_ms,
      output_writer: output_writer
    ]

    try do
      if is_function(runner, 3) do
        runner.(executable, args, command_opts)
      else
        Command.run(executable, args, command_opts)
      end
    after
      flush.()
    end
  end

  defp redact_result({output, status}, opts) when is_binary(output),
    do: {OutputRedactor.redact(output, opts), status}

  defp safe_bounded(output, opts) when is_binary(output),
    do: output |> OutputRedactor.redact(opts) |> bounded()

  defp safe_bounded(output, _opts), do: bounded(output)

  defp bounded(output) when is_binary(output) do
    if byte_size(output) <= @max_output_bytes,
      do: String.trim(output),
      else:
        output
        |> binary_part(byte_size(output) - @max_output_bytes, @max_output_bytes)
        |> String.trim()
  end

  defp bounded(output), do: inspect(output, limit: 20, printable_limit: 1_024)
end
