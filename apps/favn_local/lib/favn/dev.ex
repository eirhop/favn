defmodule Favn.Dev do
  @moduledoc """
  Local developer tooling facade owned by `apps/favn_local`.

  This module is intentionally small and delegates to focused implementation
  modules under `Favn.Dev.*`.

  ## Read This Module When

  Read `Favn.Dev` when the task is about:

  - starting or stopping the local stack
  - checking local runtime state
  - reloading manifests into a running local environment
  - reading local service logs
  - project-local packaging outputs

  Read `Favn` and the DSL modules instead when the task is about asset or
  pipeline authoring.

  ## Entry Points

  - `install/1`: prepare local tooling inputs and snapshots
  - `dev/1`: start local runner, orchestrator, and web processes
  - `status/1`: inspect current stack state
  - `reload/1`: rebuild and republish the manifest
  - `build_runner/1`, `build_web/1`, `build_orchestrator/1`, `build_single/1`:
    project-local packaging flows

  See `apps/favn_local/README.md` for the full local-tooling contract and `.favn/`
  layout details.
  """

  alias Favn.Dev.Build.Orchestrator, as: OrchestratorBuild
  alias Favn.Dev.Build.Runner, as: RunnerBuild
  alias Favn.Dev.Build.Single, as: SingleBuild
  alias Favn.Dev.Build.Web, as: WebBuild
  alias Favn.Dev.Install
  alias Favn.Dev.Logs
  alias Favn.Dev.Reload
  alias Favn.Dev.Reset
  alias Favn.Dev.Stack
  alias Favn.Dev.Status

  @type status_opts :: [root_dir: Path.t()]
  @type lifecycle_opts :: [root_dir: Path.t()]

  @doc """
  Resolves and validates local install inputs used by dev tooling.
  """
  @spec install(lifecycle_opts()) :: {:ok, :installed | :already_installed} | {:error, term()}
  def install(opts \\ []) when is_list(opts), do: Install.run(opts)

  @doc false
  @spec ensure_install_ready(lifecycle_opts()) :: :ok | {:error, term()}
  def ensure_install_ready(opts \\ []) when is_list(opts), do: Install.ensure_ready(opts)

  @doc """
  Deletes all project-local `.favn/` artifacts after ensuring no owned services
  are still running.
  """
  @spec reset(lifecycle_opts()) :: :ok | {:error, term()}
  def reset(opts \\ []) when is_list(opts), do: Reset.run(opts)

  @doc """
  Prints local service logs.
  """
  @spec logs(keyword()) :: :ok
  def logs(opts \\ []) when is_list(opts), do: Logs.run(opts)

  @doc """
  Builds the project-local runner packaging target.
  """
  @spec build_runner(lifecycle_opts()) :: {:ok, map()} | {:error, term()}
  def build_runner(opts \\ []) when is_list(opts), do: RunnerBuild.run(opts)

  @doc """
  Builds the project-local web packaging target.
  """
  @spec build_web(lifecycle_opts()) :: {:ok, map()} | {:error, term()}
  def build_web(opts \\ []) when is_list(opts), do: WebBuild.run(opts)

  @doc """
  Builds the project-local orchestrator packaging target.
  """
  @spec build_orchestrator(lifecycle_opts()) :: {:ok, map()} | {:error, term()}
  def build_orchestrator(opts \\ []) when is_list(opts), do: OrchestratorBuild.run(opts)

  @doc """
  Builds the project-local single-node assembly target.
  """
  @spec build_single(lifecycle_opts()) :: {:ok, map()} | {:error, term()}
  def build_single(opts \\ []) when is_list(opts), do: SingleBuild.run(opts)

  @doc """
  Starts local stack in foreground mode.
  """
  @spec dev(lifecycle_opts()) :: :ok | {:error, term()}
  def dev(opts \\ []) when is_list(opts), do: Stack.start_foreground(opts)

  @doc """
  Stops local stack using project-local runtime metadata.
  """
  @spec stop(lifecycle_opts()) :: :ok | {:error, term()}
  def stop(opts \\ []) when is_list(opts), do: Stack.stop(opts)

  @doc """
  Rebuilds and republishes the manifest to the running local orchestrator.
  """
  @spec reload(lifecycle_opts()) :: :ok | {:error, term()}
  def reload(opts \\ []) when is_list(opts), do: Reload.run(opts)

  @doc """
  Returns local stack status for the current project.
  """
  @spec status(status_opts()) :: map()
  def status(opts \\ []) when is_list(opts), do: Status.inspect_stack(opts)
end
