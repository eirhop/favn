defmodule Mix.Tasks.Favn.Install do
  use Mix.Task

  @shortdoc "Resolves project-local Favn install inputs"

  @moduledoc """
  Resolves and validates project-local install inputs under `.favn/install`.
  """

  alias Favn.Dev
  alias Mix.Tasks.Favn.CLIArgs

  @impl Mix.Task
  def run(args) do
    opts =
      CLIArgs.parse_no_args!("favn.install", args,
        root_dir: :string,
        force: :boolean
      )

    case Dev.install(opts) do
      {:ok, :installed} ->
        IO.puts("Favn install complete")

      {:ok, :already_installed} ->
        IO.puts("Favn install is already up to date")

      {:error, {:missing_tool, tool}} ->
        Mix.raise("install failed: missing required tool #{tool}")

      {:error, {:docker_engine_unavailable, _status, _output}} ->
        Mix.raise(
          "install failed: Docker Engine is not reachable; start a Linux-container Docker daemon and retry"
        )

      {:error, {:docker_compose_unavailable, _status, _output}} ->
        Mix.raise(
          "install failed: the Docker Compose plugin is unavailable; install Docker Compose v2 or newer and retry"
        )

      {:error, {:unsupported_compose_version, version}} ->
        Mix.raise(
          "install failed: unsupported Docker Compose version #{version}; Compose v2 or newer is required"
        )

      {:error, {:unsupported_docker_server, os, architecture}} ->
        Mix.raise(
          "install failed: unsupported Docker server target #{os}/#{architecture}; use a Linux amd64 daemon"
        )

      {:error, {:unsupported_docker_host, os, architecture}} ->
        Mix.raise(
          "install failed: unsupported Docker host #{os}/#{architecture}; use Linux amd64 or WSL2 amd64"
        )

      {:error, {:lock_failed, :timeout}} ->
        Mix.raise(
          "install failed: another Favn lifecycle command is active; retry after it exits"
        )

      {:error, :control_plane_registry_authentication_required} ->
        Mix.raise(
          "install failed: GHCR authentication required; configure Docker login/credential helpers and retry"
        )

      {:error, {:control_plane_version_unavailable, reference}} ->
        Mix.raise("install failed: no official control-plane image exists at #{reference}")

      {:error, reason} ->
        Mix.raise("install failed: #{inspect(reason)}")
    end
  end
end
