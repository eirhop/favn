defmodule Favn.Storage.Adapter do
  @moduledoc """
  Low-level storage behaviour accepted by the orchestrator-backed `Favn.Storage`
  facade.

  This contract operates on orchestrator control-plane data, not projected
  `%Favn.Run{}` values. Use `Favn.Storage` for the public run API.

  Backfill callbacks persist normalized read models owned by the orchestrator:
  coverage baselines, per-window backfill ledger rows, and latest asset/window
  state. Adapters should preserve the same upsert and filtering semantics across
  memory, SQLite, and Postgres.

  Adapter startup is optional. `child_spec/1` returns `:none` when no supervised
  process is required or when the adapter runtime is already started, and may
  return `{:error, reason}` for recoverable configuration errors.

  Scheduler state keys are exact keys. `{pipeline_module, nil}` addresses the
  nil schedule id and does not fall back to the latest concrete schedule id.

  Run events are unique by `{run_id, sequence}`. `append_run_event/3` treats an
  exact duplicate normalized event write as an idempotent success and returns
  `:ok` without adding another event. A duplicate sequence with different event
  content must return `{:error, :conflicting_event_sequence}`.

  `persist_run_transition/3` applies the same run-event duplicate semantics
  atomically with the run snapshot write. It returns `:idempotent` only when the
  stored run snapshot and stored event are both identical to the incoming write.
  """

  alias Favn.Manifest.Version
  alias FavnOrchestrator.Backfill.AssetWindowState
  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.Backfill.CoverageBaseline
  alias FavnOrchestrator.Page
  alias FavnOrchestrator.RunState

  @type adapter_opts :: keyword()
  @type list_opts :: Favn.list_runs_opts()
  @type filter_opts :: keyword()
  @type error :: :not_found | :invalid_opts | term()
  @type scheduler_key :: {module(), atom() | nil}
  @type child_spec_result :: {:ok, Supervisor.child_spec()} | :none | {:error, error()}

  @callback child_spec(adapter_opts()) :: child_spec_result()

  @callback put_manifest_version(Version.t(), adapter_opts()) :: :ok | {:error, error()}
  @callback get_manifest_version(String.t(), adapter_opts()) ::
              {:ok, Version.t()} | {:error, error()}
  @callback list_manifest_versions(adapter_opts()) :: {:ok, [Version.t()]} | {:error, error()}

  @callback set_active_manifest_version(String.t(), adapter_opts()) :: :ok | {:error, error()}
  @callback get_active_manifest_version(adapter_opts()) :: {:ok, String.t()} | {:error, error()}

  @callback put_run(RunState.t(), adapter_opts()) :: :ok | {:error, error()}
  @callback get_run(String.t(), adapter_opts()) :: {:ok, RunState.t()} | {:error, error()}
  @callback list_runs(list_opts(), adapter_opts()) :: {:ok, [RunState.t()]} | {:error, error()}
  @callback persist_run_transition(RunState.t(), map(), adapter_opts()) ::
              :ok | :idempotent | {:error, error()}

  @callback append_run_event(String.t(), map(), adapter_opts()) :: :ok | {:error, error()}
  @callback list_run_events(String.t(), adapter_opts()) :: {:ok, [map()]} | {:error, error()}

  @callback put_scheduler_state(scheduler_key(), map(), adapter_opts()) ::
              :ok | {:error, error()}

  @callback get_scheduler_state(scheduler_key(), adapter_opts()) ::
              {:ok, map() | nil} | {:error, error()}

  @callback put_coverage_baseline(CoverageBaseline.t(), adapter_opts()) ::
              :ok | {:error, error()}
  @callback get_coverage_baseline(String.t(), adapter_opts()) ::
              {:ok, CoverageBaseline.t()} | {:error, error()}
  @callback list_coverage_baselines(filter_opts(), adapter_opts()) ::
              {:ok, Page.t(CoverageBaseline.t())} | {:error, error()}

  @callback put_backfill_window(BackfillWindow.t(), adapter_opts()) ::
              :ok | {:error, error()}
  @callback get_backfill_window(String.t(), module(), String.t(), adapter_opts()) ::
              {:ok, BackfillWindow.t()} | {:error, error()}
  @callback list_backfill_windows(filter_opts(), adapter_opts()) ::
              {:ok, Page.t(BackfillWindow.t())} | {:error, error()}

  @callback put_asset_window_state(AssetWindowState.t(), adapter_opts()) ::
              :ok | {:error, error()}
  @callback get_asset_window_state(module(), atom(), String.t(), adapter_opts()) ::
              {:ok, AssetWindowState.t()} | {:error, error()}
  @callback list_asset_window_states(filter_opts(), adapter_opts()) ::
              {:ok, Page.t(AssetWindowState.t())} | {:error, error()}

  @callback replace_backfill_read_models(
              filter_opts(),
              [CoverageBaseline.t()],
              [BackfillWindow.t()],
              [AssetWindowState.t()],
              adapter_opts()
            ) :: :ok | {:error, error()}
end
