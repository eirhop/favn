defmodule Mix.Tasks.Favn.Dev do
  use Mix.Task

  @dialyzer {:nowarn_function, run: 1}

  @shortdoc "Starts local Favn dev stack"

  @moduledoc """
  Starts PostgreSQL 18, the installed prebuilt control plane, and
  the customer-built runner using the consumer-owned local Compose file.
  Selection precedence is `--compose-file`, `config :favn, :local`, then
  `deploy/local/compose.yml`. Without an explicit runner image, Favn builds
  `deploy/runner/Dockerfile` under an automatically generated local release ID.
  The successful selection is recorded for later lifecycle commands.
  """

  alias Favn.Dev
  alias Favn.Dev.EnvBootstrap
  alias Mix.Tasks.Favn.CLIArgs

  @requirements ["loadpaths"]

  @impl Mix.Task
  def run(args) do
    opts = args |> parse_args() |> Keyword.put(:progress_fun, &IO.puts/1)

    case EnvBootstrap.exec(:dev, args, opts) do
      {:ok, 0} ->
        :ok

      {:ok, status} ->
        System.halt(status)

      {:error, reason} ->
        Mix.raise(error_message(reason))
    end
  end

  @doc false
  @spec run_configured([String.t()]) :: :ok | no_return()
  def run_configured(args) do
    opts = args |> parse_args() |> Keyword.put(:progress_fun, &IO.puts/1)

    with {:ok, opts} <- EnvBootstrap.consume(:dev, opts) do
      run_dev(opts)
    else
      {:error, reason} -> Mix.raise(configured_error_message(reason))
    end
  end

  defp run_dev(opts) do
    case Dev.dev(opts) do
      :ok ->
        :ok

      {:error, reason} ->
        Mix.raise(error_message(reason))
    end
  end

  @doc false
  @spec parse_args([String.t()]) :: keyword()
  def parse_args(args) when is_list(args) do
    opts =
      CLIArgs.parse_no_args!("favn.dev", args,
        root_dir: :string,
        scheduler: :boolean,
        compose_file: :string,
        runner_image: :string
      )

    opts
  end

  defp error_message(:stack_already_running), do: "local stack already running"

  defp error_message({:lock_failed, :timeout}),
    do: "another Favn lifecycle command is active for this project; retry after it exits"

  defp error_message(:install_required), do: "install required; run mix favn.install"

  defp error_message(:install_stale),
    do:
      "install stale; run mix favn.install to refresh, or mix favn.install --force to repull and revalidate"

  defp error_message({:stack_partially_running, service_states}) do
    details =
      service_states
      |> Enum.map_join(", ", fn {service, state} -> "#{service}=#{state}" end)

    "local stack is in a partial/dead state (#{details}); run mix favn.stop to clean up before retrying"
  end

  defp error_message({:missing_tool, tool}),
    do: "missing required tool #{tool}; run mix favn.install after tool is available"

  defp error_message({:docker_engine_unavailable, _status, _output}),
    do: "Docker Engine is not reachable; start the Linux-container daemon and retry"

  defp error_message({:docker_compose_unavailable, _status, _output}),
    do: "Docker Compose is unavailable; install the Compose v2 plugin or newer and retry"

  defp error_message({:compose_file_missing, path}),
    do:
      "local Compose file does not exist: #{path}\nrun mix favn.init to create the default local scaffold"

  defp error_message({:compose_file_outside_project, path}),
    do: "local Compose file must be inside the Mix project: #{path}"

  defp error_message({:compose_file_symlink, path}),
    do: "local Compose file and its parent directories must not be symlinks: #{path}"

  defp error_message({:compose_file_not_regular, path}),
    do: "local Compose path is not a regular file: #{path}"

  defp error_message({:unsupported_compose_profile, :local, actual}),
    do: "mix favn.dev requires a local Compose profile; selected profile is #{inspect(actual)}"

  defp error_message({:missing_compose_roles, roles}),
    do: "local Compose contract is missing required Favn roles: #{inspect(roles)}"

  defp error_message({:duplicate_compose_role, role}),
    do: "local Compose contract declares the Favn role #{inspect(role)} more than once"

  defp error_message({:unknown_compose_role, role}),
    do: "local Compose contract declares unknown Favn role #{inspect(role)}"

  defp error_message({:root_owned_local_project, path}),
    do:
      "local project #{path} is owned by root; change its owner before running the non-root Favn runner"

  defp error_message({:unsupported_docker_server, os, architecture}),
    do: "unsupported Docker target #{os}/#{architecture}; Linux amd64 is required"

  defp error_message({:unsupported_docker_host, os, architecture}),
    do: "unsupported Docker host #{os}/#{architecture}; Linux amd64 or WSL2 amd64 is required"

  defp error_message({:docker_image_unavailable, image}),
    do: "customer runner image is unavailable: #{image}; build or pull it before mix favn.dev"

  defp error_message({:runner_dockerfile_missing, path}),
    do: "customer runner Dockerfile is missing: #{path}\nrun mix favn.init"

  defp error_message({:runner_image_build_failed, status, output}),
    do: "customer runner build failed (status=#{inspect(status)}): #{output}"

  defp error_message({:runner_image_release_id_mismatch, mismatch}),
    do:
      "customer runner ignored the generated release ID: expected #{mismatch.expected}, got #{mismatch.actual}"

  defp error_message({:compose_command_failed, phase, status, output}),
    do: "Docker Compose phase #{inspect(phase)} failed (status=#{inspect(status)}): #{output}"

  defp error_message(reason), do: "failed to start local stack: #{inspect(reason)}"

  defp configured_error_message(:env_bootstrap_required),
    do: "favn.dev.configured is an internal task; run mix favn.dev"

  defp configured_error_message(reason),
    do: "invalid favn.dev environment bootstrap: #{inspect(reason)}; run mix favn.dev"
end
