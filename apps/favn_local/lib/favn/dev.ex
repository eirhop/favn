defmodule Favn.Dev do
  @moduledoc """
  Local developer tooling facade owned by `apps/favn_local`.

  This module is intentionally small and delegates to focused implementation
  modules under `Favn.Dev.*`.

  ## Read This Module When

  Read `Favn.Dev` when the task is about:

  - starting or stopping the local stack
  - submitting or inspecting local operational backfills
  - bootstrapping a local sample project
  - validating local project setup
  - checking local runtime state
  - reloading manifests into a running local environment
  - reading local service logs
  - project-local packaging outputs

  Read `Favn` and the DSL modules instead when the task is about asset or
  pipeline authoring.

  ## Entry Points

  - `install/1`: prepare local tooling inputs and snapshots
  - `init/1`: generate a local DuckDB sample project scaffold
  - `doctor/1`: validate local project setup before running
  - `dev/1`: start local runner, orchestrator, and web processes
  - `status/1`: inspect current stack state
  - `diagnostics/1`: fetch service-authenticated operator diagnostics
  - `reload/1`: rebuild and republish the manifest
  - `build_runner/1`, `build_web/1`, `build_orchestrator/1`, `build_single/1`:
    project-local packaging flows
  - `bootstrap_single/1`: API-driven single-node backend bootstrap

  See `apps/favn_local/README.md` for the full local-tooling contract and `.favn/`
  layout details.
  """

  alias Favn.Dev.Backfill
  alias Favn.Dev.Bootstrap.Single, as: SingleBootstrap
  alias Favn.Dev.Build.Orchestrator, as: OrchestratorBuild
  alias Favn.Dev.Build.Runner, as: RunnerBuild
  alias Favn.Dev.Build.Single, as: SingleBuild
  alias Favn.Dev.Build.Web, as: WebBuild
  alias Favn.Dev.Diagnostics
  alias Favn.Dev.Doctor
  alias Favn.Dev.Init
  alias Favn.Dev.Install
  alias Favn.Dev.Logs
  alias Favn.Dev.Reload
  alias Favn.Dev.Reset
  alias Favn.Dev.Run
  alias Favn.Dev.Stack
  alias Favn.Dev.Status

  @type status_opts :: [root_dir: Path.t()]
  @type lifecycle_opts :: [root_dir: Path.t()]

  @doc """
  Bootstraps local Favn files in the current Mix project.
  """
  @spec init(lifecycle_opts()) :: {:ok, map()} | {:error, term()}
  def init(opts \\ []) when is_list(opts), do: Init.run(opts)

  @doc """
  Validates local Favn project setup.
  """
  @spec doctor(lifecycle_opts()) :: {:ok, [map()]} | {:error, [map()]}
  def doctor(opts \\ []) when is_list(opts), do: Doctor.run(opts)

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
  Bootstraps a single-node backend through orchestrator HTTP APIs.
  """
  @spec bootstrap_single(keyword()) :: {:ok, map()} | {:error, term()}
  def bootstrap_single(opts \\ []) when is_list(opts), do: SingleBootstrap.run(opts)

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
  Submits a pipeline run to the running local stack.
  """
  @spec run_pipeline(module() | String.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def run_pipeline(pipeline_module, opts \\ []) when is_list(opts),
    do: Run.pipeline(pipeline_module, opts)

  @doc """
  Submits a pipeline operational backfill to the running local stack.
  """
  @spec submit_backfill(module() | String.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def submit_backfill(pipeline_module, opts \\ []) when is_list(opts),
    do: Backfill.submit_pipeline(pipeline_module, opts)

  @doc """
  Lists child window rows for a backfill parent run.
  """
  @spec list_backfill_windows(String.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def list_backfill_windows(backfill_run_id, opts \\ []) when is_list(opts),
    do: Backfill.list_windows(backfill_run_id, opts)

  @doc """
  Lists projected coverage baselines from the local stack.
  """
  @spec list_coverage_baselines(keyword()) :: {:ok, map()} | {:error, term()}
  def list_coverage_baselines(opts \\ []) when is_list(opts),
    do: Backfill.list_coverage_baselines(opts)

  @doc """
  Lists latest projected asset/window states from the local stack.
  """
  @spec list_asset_window_states(keyword()) :: {:ok, map()} | {:error, term()}
  def list_asset_window_states(opts \\ []) when is_list(opts),
    do: Backfill.list_asset_window_states(opts)

  @doc """
  Reruns one failed backfill window.
  """
  @spec rerun_backfill_window(String.t(), String.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def rerun_backfill_window(backfill_run_id, window_key, opts \\ []) when is_list(opts),
    do: Backfill.rerun_window(backfill_run_id, window_key, opts)

  @doc """
  Repairs derived operational-backfill projections in the running local stack.
  """
  @spec repair_backfill_projections(keyword()) :: {:ok, map()} | {:error, term()}
  def repair_backfill_projections(opts \\ []) when is_list(opts),
    do: Backfill.repair_projections(opts)

  @doc """
  Fetches operator diagnostics from the running local stack.
  """
  @spec diagnostics(keyword()) :: {:ok, map()} | {:error, term()}
  def diagnostics(opts \\ []) when is_list(opts), do: Diagnostics.fetch(opts)

  @doc """
  Returns local stack status for the current project.
  """
  @spec status(status_opts()) :: map()
  def status(opts \\ []) when is_list(opts), do: Status.inspect_stack(opts)
end
