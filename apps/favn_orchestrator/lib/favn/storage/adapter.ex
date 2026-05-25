defmodule Favn.Storage.Adapter do
  @moduledoc """
  Low-level storage behaviour accepted by the orchestrator-backed `Favn.Storage`
  facade.

  This contract operates on orchestrator control-plane data, not projected
  `%Favn.Run{}` values. Use `Favn.Storage` for the runtime-facing storage
  facade.

  Backfill callbacks persist normalized read models owned by the orchestrator:
  coverage baselines, per-window backfill ledger rows, and latest asset/window
  state. Adapters should preserve the same upsert and filtering semantics across
  memory, SQLite, and Postgres.

  Bulk read-model callbacks must be semantically equivalent to applying the
  corresponding single-row put callback to each row in list order. When a bulk
  input contains duplicate natural keys, the last row in the list wins. Adapters
  may coalesce duplicates before writing, but the externally visible result must
  not depend on chunk size or database-specific duplicate handling.

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

  Execution lease callbacks are required. They enforce orchestrator-owned asset
  execution admission across run concurrency and shared execution pools before
  runner work is submitted. Run-scoped release must use keyed storage operations
  and return released scopes so wakeups stay targeted without global lease scans.
  """

  alias Favn.Manifest.Version
  alias FavnOrchestrator.AssetFreshnessState
  alias FavnOrchestrator.Backfill.AssetWindowState
  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.Backfill.CoverageBaseline
  alias FavnOrchestrator.Backfill.Progress, as: BackfillProgress
  alias FavnOrchestrator.CursorPage
  alias FavnOrchestrator.ExecutionAdmission.LeaseRelease
  alias FavnOrchestrator.MaterializationClaim
  alias FavnOrchestrator.Page
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.TargetStatus

  @type adapter_opts :: keyword()
  @type list_opts :: Favn.list_runs_opts()
  @type filter_opts :: keyword()
  @type error :: :not_found | :invalid_opts | term()
  @type scheduler_key :: {module(), atom() | nil}
  @type freshness_state_key :: {module(), atom(), String.t()}
  @type child_spec_result :: {:ok, Supervisor.child_spec()} | :none | {:error, error()}
  @type readiness_diagnostics :: map()
  @type diagnostics :: map()
  @type cursor_scan_opts :: keyword()
  @type read_model_replacement_scope :: :all | {:backfill_run, String.t()} | {:pipeline, module()}
  @type target_status_scope ::
          {:manifest_version, String.t()}
          | {:manifest_version, String.t(), TargetStatus.target_kind()}

  @callback child_spec(adapter_opts()) :: child_spec_result()
  @callback readiness(adapter_opts()) :: {:ok, readiness_diagnostics()} | {:error, error()}
  @callback diagnostics(adapter_opts()) :: {:ok, diagnostics()} | {:error, error()}

  @callback put_manifest_version(Version.t(), adapter_opts()) :: :ok | {:error, error()}
  @callback get_manifest_version(String.t(), adapter_opts()) ::
              {:ok, Version.t()} | {:error, error()}
  @callback get_manifest_version_by_content_hash(String.t(), adapter_opts()) ::
              {:ok, Version.t()} | {:error, error()}
  @callback list_manifest_versions(adapter_opts()) :: {:ok, [Version.t()]} | {:error, error()}

  @callback set_active_manifest_version(String.t(), adapter_opts()) :: :ok | {:error, error()}
  @callback get_active_manifest_version(adapter_opts()) :: {:ok, String.t()} | {:error, error()}

  @callback put_run(RunState.t(), adapter_opts()) :: :ok | {:error, error()}
  @callback get_run(String.t(), adapter_opts()) :: {:ok, RunState.t()} | {:error, error()}
  @callback list_runs(list_opts(), adapter_opts()) :: {:ok, [RunState.t()]} | {:error, error()}
  @callback list_execution_group_runs(String.t(), adapter_opts()) ::
              {:ok, [RunState.t()]} | {:error, error()}
  @callback list_execution_group_run_ids(String.t(), adapter_opts()) ::
              {:ok, [String.t()]} | {:error, error()}
  @callback list_execution_groups(filter_opts(), adapter_opts()) ::
              {:ok, Page.t(String.t())} | {:error, error()}
  @callback list_execution_group_summaries(filter_opts(), adapter_opts()) ::
              {:ok, Page.t(map())} | {:error, error()}
  @callback rebuild_execution_group_summaries(adapter_opts()) ::
              {:ok, non_neg_integer()} | {:error, error()}
  @callback persist_run_transition(RunState.t(), map(), adapter_opts()) ::
              :ok | :idempotent | {:error, error()}

  @callback append_run_event(String.t(), map(), adapter_opts()) :: :ok | {:error, error()}
  @callback list_run_events(String.t(), adapter_opts()) :: {:ok, [map()]} | {:error, error()}
  @callback list_run_events(String.t(), filter_opts(), adapter_opts()) ::
              {:ok, [map()]} | {:error, error()}
  @callback list_execution_group_events(String.t(), filter_opts(), adapter_opts()) ::
              {:ok, [map()]} | {:error, error()}
  @callback list_global_run_events(filter_opts(), adapter_opts()) ::
              {:ok, [map()]} | {:error, error()}

  @callback try_acquire_execution_lease(map(), adapter_opts()) ::
              {:ok, map()} | {:error, {:execution_capacity_exceeded, map()} | error()}
  @callback release_execution_lease(String.t(), adapter_opts()) :: :ok | {:error, error()}
  @callback release_execution_leases_for_run(String.t(), adapter_opts()) ::
              {:ok, LeaseRelease.t()} | {:error, error()}
  @callback expire_execution_leases(DateTime.t(), adapter_opts()) ::
              {:ok, non_neg_integer()} | {:error, error()}
  @callback list_execution_leases(adapter_opts()) :: {:ok, [map()]} | {:error, error()}
  @callback upsert_execution_admission_waiter(map(), adapter_opts()) ::
              {:ok, map()} | {:error, error()}
  @callback delete_execution_admission_waiter(String.t(), adapter_opts()) ::
              :ok | {:error, error()}
  @callback delete_execution_admission_waiters_for_run(String.t(), adapter_opts()) ::
              {:ok, non_neg_integer()} | {:error, error()}
  @callback list_execution_admission_waiters_for_scope(map(), filter_opts(), adapter_opts()) ::
              {:ok, [map()]} | {:error, error()}
  @callback expire_execution_admission_waiters(DateTime.t(), adapter_opts()) ::
              {:ok, non_neg_integer()} | {:error, error()}

  @callback try_acquire_materialization_claim(MaterializationClaim.t() | map(), adapter_opts()) ::
              {:ok, MaterializationClaim.t()}
              | {:already_succeeded, MaterializationClaim.t()}
              | {:already_claimed, MaterializationClaim.t()}
              | {:error, error()}
  @callback complete_materialization_claim(String.t(), map(), adapter_opts()) ::
              {:ok, MaterializationClaim.t()} | {:error, error()}
  @callback fail_materialization_claim(String.t(), map(), adapter_opts()) ::
              {:ok, MaterializationClaim.t()} | {:error, error()}
  @callback expire_materialization_claims(DateTime.t(), adapter_opts()) ::
              {:ok, non_neg_integer()} | {:error, error()}
  @callback get_materialization_claim(String.t(), adapter_opts()) ::
              {:ok, MaterializationClaim.t()} | {:error, error()}
  @callback list_materialization_claims(filter_opts(), adapter_opts()) ::
              {:ok, [MaterializationClaim.t()]} | {:error, error()}

  @callback persist_log_entries([Favn.Log.Entry.t()], adapter_opts()) ::
              {:ok, [Favn.Log.Entry.t()]} | {:error, error()}
  @callback list_logs(Favn.Log.Filter.t() | map() | keyword(), keyword(), adapter_opts()) ::
              {:ok, Page.t(Favn.Log.Entry.t())} | {:error, error()}
  @callback scan_logs(
              Favn.Log.Filter.t() | map() | keyword(),
              cursor_scan_opts(),
              adapter_opts()
            ) :: {:ok, CursorPage.t(Favn.Log.Entry.t())} | {:error, error()}
  @callback replay_logs_after(
              Favn.Log.Cursor.t() | String.t() | nil,
              Favn.Log.Filter.t() | map() | keyword(),
              keyword(),
              adapter_opts()
            ) :: {:ok, [Favn.Log.Entry.t()]} | {:error, error()}

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
  @callback put_backfill_windows([BackfillWindow.t()], adapter_opts()) ::
              :ok | {:error, error()}
  @callback get_backfill_window(String.t(), module(), String.t(), adapter_opts()) ::
              {:ok, BackfillWindow.t()} | {:error, error()}
  @callback list_backfill_windows(filter_opts(), adapter_opts()) ::
              {:ok, Page.t(BackfillWindow.t())} | {:error, error()}
  @callback scan_backfill_windows(filter_opts(), cursor_scan_opts(), adapter_opts()) ::
              {:ok, CursorPage.t(BackfillWindow.t())} | {:error, error()}
  @callback apply_backfill_child_projection(
              BackfillWindow.t(),
              [AssetWindowState.t()],
              adapter_opts()
            ) :: {:ok, BackfillProgress.t()} | {:error, error()}
  @callback get_backfill_progress(String.t(), adapter_opts()) ::
              {:ok, BackfillProgress.t()} | {:error, error()}
  @callback rebuild_backfill_progress(String.t(), adapter_opts()) ::
              {:ok, BackfillProgress.t()} | {:error, error()}

  @callback put_asset_window_state(AssetWindowState.t(), adapter_opts()) ::
              :ok | {:error, error()}
  @callback put_asset_window_states([AssetWindowState.t()], adapter_opts()) ::
              :ok | {:error, error()}
  @callback get_asset_window_state(module(), atom(), String.t(), adapter_opts()) ::
              {:ok, AssetWindowState.t()} | {:error, error()}
  @callback list_asset_window_states(filter_opts(), adapter_opts()) ::
              {:ok, Page.t(AssetWindowState.t())} | {:error, error()}

  @callback put_asset_freshness_state(AssetFreshnessState.t(), adapter_opts()) ::
              :ok | {:error, error()}
  @callback get_asset_freshness_state(module(), atom(), String.t(), adapter_opts()) ::
              {:ok, AssetFreshnessState.t()} | {:error, error()}
  @callback get_asset_freshness_states_by_keys([freshness_state_key()], adapter_opts()) ::
              {:ok, %{freshness_state_key() => AssetFreshnessState.t()}} | {:error, error()}
  @callback list_asset_freshness_states(filter_opts(), adapter_opts()) ::
              {:ok, Page.t(AssetFreshnessState.t())} | {:error, error()}
  @callback scan_asset_freshness_states(filter_opts(), cursor_scan_opts(), adapter_opts()) ::
              {:ok, CursorPage.t(AssetFreshnessState.t())} | {:error, error()}

  @callback upsert_target_status(TargetStatus.t(), adapter_opts()) :: :ok | {:error, error()}
  @callback get_target_status(
              String.t(),
              TargetStatus.target_kind(),
              String.t(),
              adapter_opts()
            ) :: {:ok, TargetStatus.t()} | {:error, error()}
  @callback list_target_statuses(
              String.t(),
              TargetStatus.target_kind(),
              [String.t()],
              adapter_opts()
            ) :: {:ok, %{String.t() => TargetStatus.t()}} | {:error, error()}
  @callback replace_target_statuses(target_status_scope(), [TargetStatus.t()], adapter_opts()) ::
              :ok | {:error, error()}
  @callback delete_target_statuses(target_status_scope(), adapter_opts()) ::
              :ok | {:error, error()}

  @callback replace_backfill_read_models(
              read_model_replacement_scope(),
              [CoverageBaseline.t()],
              [BackfillWindow.t()],
              [AssetWindowState.t()],
              adapter_opts()
            ) :: :ok | {:error, error()}

  @callback put_auth_actor(map(), adapter_opts()) :: :ok | {:error, error()}
  @callback put_auth_actor_with_credential(map(), map(), adapter_opts()) ::
              :ok | {:error, error()}
  @callback get_auth_actor(String.t(), adapter_opts()) :: {:ok, map()} | {:error, error()}
  @callback get_auth_actor_by_username(String.t(), adapter_opts()) ::
              {:ok, map()} | {:error, error()}
  @callback list_auth_actors(adapter_opts()) :: {:ok, [map()]} | {:error, error()}
  @callback put_auth_credential(String.t(), map(), adapter_opts()) :: :ok | {:error, error()}
  @callback update_auth_actor_password(String.t(), map(), map(), DateTime.t(), adapter_opts()) ::
              :ok | {:error, error()}
  @callback get_auth_credential(String.t(), adapter_opts()) :: {:ok, map()} | {:error, error()}
  @callback put_auth_session(map(), adapter_opts()) :: :ok | {:error, error()}
  @callback get_auth_session(String.t(), adapter_opts()) :: {:ok, map()} | {:error, error()}
  @callback get_auth_session_by_token_hash(String.t(), adapter_opts()) ::
              {:ok, map()} | {:error, error()}
  @callback revoke_auth_session(String.t(), DateTime.t(), adapter_opts()) ::
              :ok | {:error, error()}
  @callback revoke_auth_sessions_for_actor(String.t(), DateTime.t(), adapter_opts()) ::
              :ok | {:error, error()}
  @callback put_auth_audit(map(), adapter_opts()) :: :ok | {:error, error()}
  @callback list_auth_audit(keyword(), adapter_opts()) :: {:ok, [map()]} | {:error, error()}

  @callback reserve_idempotency_record(map(), adapter_opts()) ::
              {:ok, {:reserved, map()} | {:replay, map()}}
              | {:error, :idempotency_conflict | :operation_in_progress | error()}
  @callback complete_idempotency_record(String.t(), map(), adapter_opts()) ::
              :ok | {:error, error()}
  @callback get_idempotency_record(String.t(), adapter_opts()) ::
              {:ok, map()} | {:error, error()}

  @optional_callbacks readiness: 1,
                      diagnostics: 1,
                      put_auth_actor: 2,
                      put_auth_actor_with_credential: 3,
                      get_auth_actor: 2,
                      get_auth_actor_by_username: 2,
                      list_auth_actors: 1,
                      put_auth_credential: 3,
                      update_auth_actor_password: 5,
                      get_auth_credential: 2,
                      put_auth_session: 2,
                      get_auth_session: 2,
                      get_auth_session_by_token_hash: 2,
                      revoke_auth_session: 3,
                      revoke_auth_sessions_for_actor: 3,
                      put_auth_audit: 2,
                      list_auth_audit: 2,
                      reserve_idempotency_record: 2,
                      complete_idempotency_record: 3,
                      get_idempotency_record: 2,
                      scan_logs: 3,
                      list_execution_group_runs: 2,
                      list_execution_group_run_ids: 2,
                      list_execution_groups: 2,
                      list_execution_group_summaries: 2,
                      rebuild_execution_group_summaries: 1,
                      list_run_events: 3,
                      list_execution_group_events: 3,
                      put_backfill_windows: 2,
                      put_asset_window_states: 2,
                      put_asset_freshness_state: 2,
                      get_asset_freshness_state: 4,
                      list_asset_freshness_states: 2,
                      upsert_target_status: 2,
                      get_target_status: 4,
                      list_target_statuses: 4,
                      replace_target_statuses: 3,
                      delete_target_statuses: 2,
                      try_acquire_materialization_claim: 2,
                      complete_materialization_claim: 3,
                      fail_materialization_claim: 3,
                      expire_materialization_claims: 2,
                      get_materialization_claim: 2,
                      list_materialization_claims: 2
end
