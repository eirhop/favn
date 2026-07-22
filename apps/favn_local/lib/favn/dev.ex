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
  - `inspect_relation/2`, `inspect_partitions/2`, `query/2`: inspect local SQL data
  - `init/1`: generate an explicit DuckDB sample or consumer-owned Compose template
  - `doctor/1`: validate local project setup before running
  - `dev/1`: start the production-like local Docker Compose topology
  - `maintainer_dev/1`: build or reuse a non-production control plane from `FAVN_CHECKOUT`
  - `status/1`: inspect current stack state
  - `diagnostics/1`: fetch service-authenticated operator diagnostics
  - `reload/1`: rebuild and republish the manifest
  - `run/2`: submit an asset or pipeline run with optional dependency and refresh intent
  - `list_runs/1`, `get_run/2`, `cancel_run/2`, `list_run_events/2`: inspect
    and control local runs through HTTP APIs
  - `build_runner/1`, `build_manifest/1`: immutable runner and aligned manifest releases
  - `publish/1`, `activate/1`: topology-neutral staged deployment operations

  See `apps/favn_local/README.md` for the full local-tooling contract and `.favn/`
  layout details.
  """

  alias Favn.Dev.Backfill
  alias Favn.Dev.Activate
  alias Favn.Dev.Build.Runner, as: RunnerBuild
  alias Favn.Dev.ComposeLifecycle
  alias Favn.Dev.DataInspection
  alias Favn.Dev.Doctor
  alias Favn.Dev.Init
  alias Favn.Dev.Install
  alias Favn.Dev.Maintainer
  alias Favn.Dev.Publish
  alias Favn.Dev.Reset
  alias Favn.Dev.Run
  alias Favn.Dev.Runs

  @type status_opts :: [root_dir: Path.t()]
  @type lifecycle_opts :: [root_dir: Path.t()]

  @doc """
  Scaffolds local Favn files in the current Mix project.

  Pass `duckdb: true, sample: true` for the authoring sample, or
  `target: :compose` with an optional `profile: :local | :single_host` and
  project-relative `output:` path for a consumer-owned deployment template.
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

  @doc """
  Inspects a configured local SQL relation.
  """
  @spec inspect_relation(String.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def inspect_relation(relation, opts \\ []) when is_binary(relation) and is_list(opts),
    do: DataInspection.inspect_relation(relation, opts)

  @doc """
  Inspects partition-like metadata for a configured local SQL relation.
  """
  @spec inspect_partitions(String.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def inspect_partitions(relation, opts \\ []) when is_binary(relation) and is_list(opts),
    do: DataInspection.inspect_partitions(relation, opts)

  @doc """
  Runs a local SQL query through the configured SQL runtime client.
  """
  @spec query(String.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def query(sql, opts \\ []) when is_binary(sql) and is_list(opts),
    do: DataInspection.query(sql, opts)

  @doc false
  @spec ensure_install_ready(lifecycle_opts()) :: :ok | {:error, term()}
  def ensure_install_ready(opts \\ []) when is_list(opts), do: Install.ensure_ready(opts)

  @doc """
  Removes generated local state and verified runner image tags after ensuring
  known Favn roles are stopped. Consumer Compose resources and `.favn/data`
  remain untouched.
  """
  @spec reset(lifecycle_opts()) :: :ok | {:error, term()}
  def reset(opts \\ []) when is_list(opts), do: Reset.run(opts)

  @doc """
  Prints local service logs.
  """
  @spec logs(keyword()) :: :ok | {:error, term()}
  def logs(opts \\ []) when is_list(opts), do: ComposeLifecycle.logs(opts)

  @doc """
  Builds the project-local runner packaging target.
  """
  @spec build_runner(lifecycle_opts()) :: {:ok, map()} | {:error, term()}
  def build_runner(opts \\ []) when is_list(opts), do: RunnerBuild.run(opts)

  @doc "Builds a manifest release aligned with an explicit runner descriptor."
  @spec build_manifest(keyword()) :: {:ok, map()} | {:error, term()}
  def build_manifest(opts) when is_list(opts), do: Favn.Dev.Build.Manifest.run(opts)

  @doc "Publishes an immutable manifest release as staged/inactive."
  @spec publish(keyword()) :: {:ok, map()} | {:error, term()}
  def publish(opts) when is_list(opts), do: Publish.run(opts)

  @doc "Activates one exact staged manifest for one workspace."
  @spec activate(keyword()) :: {:ok, map()} | {:error, term()}
  def activate(opts) when is_list(opts), do: Activate.run(opts)

  @doc """
  Starts local stack in foreground mode.
  """
  @spec dev(lifecycle_opts()) :: :ok | {:error, term()}
  def dev(opts \\ []) when is_list(opts), do: ComposeLifecycle.start_foreground(opts)

  @doc """
  Builds or reuses a non-production control plane from `FAVN_CHECKOUT`, then
  starts or reloads the local stack with that exact image.

  The consuming project must load all Favn path dependencies from the same
  checkout. Official installation remains owned by `install/1`.
  """
  @spec maintainer_dev(lifecycle_opts()) :: :ok | {:error, term()}
  def maintainer_dev(opts \\ []) when is_list(opts), do: Maintainer.run(opts)

  @doc """
  Stops local stack using project-local runtime metadata.
  """
  @spec stop(lifecycle_opts()) :: :ok | {:error, term()}
  def stop(opts \\ []) when is_list(opts), do: ComposeLifecycle.stop(opts)

  @doc """
  Applies a manifest-only, runner-environment, or immutable runner change to
  the recorded local deployment.
  """
  @spec reload(lifecycle_opts()) :: :ok | {:error, term()}
  def reload(opts \\ []) when is_list(opts), do: ComposeLifecycle.reload(opts)

  @doc """
  Submits an asset or pipeline run to the running local stack.

  Asset targets accept `dependencies: "all" | "none"` and refresh modes
  `"auto"`, `"missing"`, `"force_selected"`,
  `"force_selected_upstream"`, and `"force_all"`. Pipeline targets do not
  accept `:dependencies` and support only `"auto"`, `"missing"`, and
  `"force_all"` refresh modes. Omitted values use orchestrator-owned defaults.
  """
  @spec run(module() | String.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def run(target, opts \\ []) when is_list(opts), do: Run.submit(target, opts)

  @doc """
  Submits a pipeline run to the running local stack.
  """
  @spec run_pipeline(module() | String.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def run_pipeline(pipeline_module, opts \\ []) when is_list(opts),
    do: Run.submit(pipeline_module, opts)

  @doc """
  Lists persisted runs from the running local stack.
  """
  @spec list_runs(keyword()) :: {:ok, [map()]} | {:error, term()}
  def list_runs(opts \\ []) when is_list(opts), do: Runs.list(opts)

  @doc """
  Fetches one persisted run from the running local stack.
  """
  @spec get_run(String.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def get_run(run_id, opts \\ []) when is_binary(run_id) and is_list(opts),
    do: Runs.get(run_id, opts)

  @doc """
  Requests cancellation for one persisted run from the running local stack.
  """
  @spec cancel_run(String.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def cancel_run(run_id, opts \\ []) when is_binary(run_id) and is_list(opts),
    do: Runs.cancel(run_id, opts)

  @doc """
  Lists persisted run events from the running local stack.
  """
  @spec list_run_events(String.t(), keyword()) :: {:ok, [map()]} | {:error, term()}
  def list_run_events(run_id, opts \\ []) when is_binary(run_id) and is_list(opts),
    do: Runs.events(run_id, opts)

  @doc """
  Submits a pipeline operational backfill to the running local stack.
  """
  @spec submit_backfill(module() | String.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def submit_backfill(pipeline_module, opts \\ []) when is_list(opts),
    do: Backfill.submit_pipeline(pipeline_module, opts)

  @doc """
  Plans a pipeline operational backfill without creating runs.
  """
  @spec plan_backfill(module() | String.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def plan_backfill(pipeline_module, opts \\ []) when is_list(opts),
    do: Backfill.plan_pipeline(pipeline_module, opts)

  @doc "Plans exact currently missing windows for one active asset."
  @spec plan_missing_asset_backfill(module() | String.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def plan_missing_asset_backfill(asset, opts \\ []) when is_list(opts),
    do: Backfill.plan_missing_asset(asset, opts)

  @doc "Submits one previously reviewed exact missing-window plan."
  @spec submit_missing_asset_backfill(module() | String.t(), map(), keyword()) ::
          {:ok, String.t()} | {:error, term()}
  def submit_missing_asset_backfill(asset, plan, opts \\ []) when is_list(opts),
    do: Backfill.submit_missing_asset(asset, plan, opts)

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
  def diagnostics(opts \\ []) when is_list(opts), do: ComposeLifecycle.diagnostics(opts)

  @doc """
  Returns local stack status for the current project.
  """
  @spec status(status_opts()) :: map()
  def status(opts \\ []) when is_list(opts), do: ComposeLifecycle.status(opts)
end
