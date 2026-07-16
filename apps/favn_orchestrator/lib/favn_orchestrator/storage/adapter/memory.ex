defmodule FavnOrchestrator.Storage.Adapter.Memory do
  @moduledoc false

  use GenServer

  @behaviour Favn.Storage.Adapter

  alias Favn.Manifest.Version
  alias Favn.RuntimeInput.Pin
  alias Favn.Scheduler.State, as: SchedulerState
  alias FavnOrchestrator.AssetFreshnessState
  alias FavnOrchestrator.Backfill.AssetWindowState
  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.Backfill.CoverageBaseline
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage.Adapter.Memory.Auth, as: MemoryAuth
  alias FavnOrchestrator.Storage.Adapter.Memory.Backfills
  alias FavnOrchestrator.Storage.Adapter.Memory.ExecutionAdmission, as: MemoryAdmission
  alias FavnOrchestrator.Storage.Adapter.Memory.ExecutionGroups
  alias FavnOrchestrator.Storage.Adapter.Memory.Freshness
  alias FavnOrchestrator.Storage.Adapter.Memory.Idempotency
  alias FavnOrchestrator.Storage.Adapter.Memory.Logs
  alias FavnOrchestrator.Storage.Adapter.Memory.Manifests
  alias FavnOrchestrator.Storage.Adapter.Memory.MaterializationClaims
  alias FavnOrchestrator.Storage.Adapter.Memory.Query
  alias FavnOrchestrator.Storage.Adapter.Memory.RunEvents
  alias FavnOrchestrator.Storage.Adapter.Memory.Runs
  alias FavnOrchestrator.Storage.Adapter.Memory.State
  alias FavnOrchestrator.Storage.Adapter.Memory.TargetStatuses
  alias FavnOrchestrator.Storage.ExecutionAdmissionWaiterCodec
  alias FavnOrchestrator.Storage.ExecutionLeaseCodec
  alias FavnOrchestrator.Storage.ExecutionOwnershipCodec
  alias FavnOrchestrator.Storage.MaterializationClaimCodec
  alias FavnOrchestrator.Storage.RunEventCodec
  alias FavnOrchestrator.Storage.RunStateCodec
  alias FavnOrchestrator.Storage.SchedulerStateCodec
  alias FavnOrchestrator.TargetStatus

  @type state :: State.t()

  @materialization_claim_filters [
    :claim_key,
    :asset_ref_module,
    :asset_ref_name,
    :freshness_key,
    :input_fingerprint,
    :run_id,
    :asset_step_id,
    :node_key,
    :runner_execution_id,
    :manifest_version_id,
    :manifest_content_hash,
    :freshness_version,
    :status
  ]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, %{}, name: name)
  end

  @spec reset(keyword()) :: :ok
  def reset(opts \\ []) when is_list(opts) do
    call(opts, :reset)
  end

  @impl true
  def child_spec(opts) when is_list(opts) do
    runtime_name = runtime_name(opts)

    if runtime_started?(runtime_name) do
      :none
    else
      start_opts =
        opts
        |> Keyword.delete(:server)
        |> Keyword.put(:name, runtime_name)

      {:ok,
       %{
         id: {__MODULE__, runtime_name},
         start: {__MODULE__, :start_link, [start_opts]},
         type: :worker,
         restart: :permanent,
         shutdown: 5000
       }}
    end
  end

  @impl true
  def diagnostics(opts) when is_list(opts) do
    {:ok,
     %{
       status: :ready,
       ready?: true,
       mode: :memory,
       adapter: __MODULE__,
       runtime: %{running?: runtime_started?(runtime_name(opts))}
     }}
  end

  @spec scheduler_child_spec(keyword()) :: Favn.Storage.Adapter.child_spec_result()
  def scheduler_child_spec(opts \\ []) when is_list(opts) do
    child_spec(opts)
  end

  @impl true
  def put_manifest_version(%Version{} = version, opts \\ []) when is_list(opts) do
    call(opts, {:put_manifest_version, version})
  end

  @impl true
  def get_manifest_version(manifest_version_id, opts \\ []) when is_binary(manifest_version_id) do
    call(opts, {:get_manifest_version, manifest_version_id})
  end

  @impl true
  def get_manifest_version_by_content_hash(content_hash, opts \\ [])
      when is_binary(content_hash) do
    call(opts, {:get_manifest_version_by_content_hash, content_hash})
  end

  @impl true
  def list_manifest_versions(opts \\ []) when is_list(opts) do
    call(opts, :list_manifest_versions)
  end

  @impl true
  def set_active_manifest_version(manifest_version_id, opts \\ [])
      when is_binary(manifest_version_id) do
    call(opts, {:set_active_manifest_version, manifest_version_id})
  end

  @impl true
  def get_active_manifest_version(opts \\ []) when is_list(opts) do
    call(opts, :get_active_manifest_version)
  end

  @impl true
  def put_run(%RunState{} = run, opts \\ []) when is_list(opts) do
    with {:ok, normalized} <- RunStateCodec.normalize(run) do
      call(opts, {:put_run, normalized})
    end
  end

  @impl true
  def create_runtime_input_pin(%Pin{} = pin, opts) when is_list(opts) do
    call(opts, {:create_runtime_input_pin, pin})
  end

  @impl true
  def get_runtime_input_pin(run_id, node_key, opts)
      when is_binary(run_id) and is_tuple(node_key) and is_list(opts) do
    call(opts, {:get_runtime_input_pin, run_id, node_key})
  end

  @impl true
  def list_runtime_input_pins(run_id, opts) when is_binary(run_id) and is_list(opts) do
    call(opts, {:list_runtime_input_pins, run_id})
  end

  @impl true
  def persist_run_transition(%RunState{} = run, event, opts)
      when is_map(event) and is_list(opts) do
    with {:ok, normalized_run} <- RunStateCodec.normalize(run),
         {:ok, normalized_event} <- RunEventCodec.normalize(run.id, event),
         :ok <- validate_transition_alignment(normalized_run, normalized_event) do
      call(opts, {:persist_run_transition, normalized_run, normalized_event})
    end
  end

  @impl true
  def put_execution_ownership(ownership, opts \\ []) when is_list(opts) do
    with {:ok, normalized} <- ExecutionOwnershipCodec.normalize(ownership) do
      call(opts, {:put_execution_ownership, normalized})
    end
  end

  @impl true
  def get_execution_ownership(ownership_id, opts \\ [])
      when is_binary(ownership_id) and is_list(opts) do
    call(opts, {:get_execution_ownership, ownership_id})
  end

  @impl true
  def list_execution_ownerships(run_id, opts \\ []) when is_binary(run_id) and is_list(opts) do
    call(opts, {:list_execution_ownerships, run_id})
  end

  @impl true
  def list_active_execution_ownerships(run_id, opts \\ [])
      when is_binary(run_id) and is_list(opts) do
    call(opts, {:list_active_execution_ownerships, run_id})
  end

  @impl true
  def get_run(run_id, opts \\ []) when is_binary(run_id) and is_list(opts) do
    call(opts, {:get_run, run_id})
  end

  @impl true
  def list_runs(run_opts \\ [], adapter_opts \\ [])
      when is_list(run_opts) and is_list(adapter_opts) do
    call(adapter_opts, {:list_runs, run_opts})
  end

  @impl true
  def list_target_runs(
        manifest_version_id,
        target_kind,
        target_ref,
        run_opts \\ [],
        adapter_opts \\ []
      )
      when is_binary(manifest_version_id) and target_kind in [:asset, :pipeline] and
             is_list(run_opts) and is_list(adapter_opts) do
    call(
      adapter_opts,
      {:list_target_runs, manifest_version_id, target_kind, target_ref, run_opts}
    )
  end

  @impl true
  def list_execution_group_runs(group_id, opts \\ [])
      when is_binary(group_id) and is_list(opts) do
    call(opts, {:list_execution_group_runs, group_id})
  end

  @impl true
  def list_execution_group_run_ids(group_id, opts \\ [])
      when is_binary(group_id) and is_list(opts) do
    call(opts, {:list_execution_group_run_ids, group_id})
  end

  @impl true
  def list_execution_groups(group_opts, opts) when is_list(group_opts) and is_list(opts) do
    call(opts, {:list_execution_groups, group_opts})
  end

  @impl true
  def list_execution_group_summaries(group_opts, opts)
      when is_list(group_opts) and is_list(opts) do
    call(opts, {:list_execution_group_summaries, group_opts})
  end

  @impl true
  def rebuild_execution_group_summaries(opts) when is_list(opts) do
    call(opts, :rebuild_execution_group_summaries)
  end

  @impl true
  def append_run_event(run_id, event, opts \\ [])
      when is_binary(run_id) and is_map(event) and is_list(opts) do
    with {:ok, normalized} <- RunEventCodec.normalize(run_id, event) do
      call(opts, {:append_run_event, run_id, normalized})
    end
  end

  @impl true
  def list_run_events(run_id, opts \\ []) when is_binary(run_id) and is_list(opts) do
    call(opts, {:list_run_events, run_id})
  end

  @impl true
  def list_run_events(run_id, run_event_opts, opts)
      when is_binary(run_id) and is_list(run_event_opts) and is_list(opts) do
    call(opts, {:list_run_events, run_id, run_event_opts})
  end

  @impl true
  def list_execution_group_events(group_id, run_event_opts, opts)
      when is_binary(group_id) and is_list(run_event_opts) and is_list(opts) do
    call(opts, {:list_execution_group_events, group_id, run_event_opts})
  end

  @impl true
  def list_global_run_events(run_event_opts, opts)
      when is_list(run_event_opts) and is_list(opts) do
    call(opts, {:list_global_run_events, run_event_opts})
  end

  @impl true
  def try_acquire_execution_lease(lease, opts) when is_map(lease) and is_list(opts) do
    with {:ok, normalized} <- ExecutionLeaseCodec.normalize(lease) do
      call(opts, {:try_acquire_execution_lease, normalized})
    end
  end

  @impl true
  def release_execution_lease(lease_id, opts) when is_binary(lease_id) and is_list(opts) do
    call(opts, {:release_execution_lease, lease_id})
  end

  @impl true
  def release_execution_leases_for_run(run_id, opts) when is_binary(run_id) and is_list(opts) do
    call(opts, {:release_execution_leases_for_run, run_id})
  end

  @impl true
  def expire_execution_leases(%DateTime{} = now, opts) when is_list(opts) do
    call(opts, {:expire_execution_leases, now})
  end

  @impl true
  def list_execution_leases(opts) when is_list(opts) do
    call(opts, :list_execution_leases)
  end

  @impl true
  def upsert_execution_admission_waiter(waiter, opts) when is_map(waiter) and is_list(opts) do
    with {:ok, normalized} <- ExecutionAdmissionWaiterCodec.normalize(waiter) do
      call(opts, {:upsert_execution_admission_waiter, normalized})
    end
  end

  @impl true
  def delete_execution_admission_waiter(waiter_id, opts)
      when is_binary(waiter_id) and is_list(opts) do
    call(opts, {:delete_execution_admission_waiter, waiter_id})
  end

  @impl true
  def delete_execution_admission_waiters_for_run(run_id, opts)
      when is_binary(run_id) and is_list(opts) do
    call(opts, {:delete_execution_admission_waiters_for_run, run_id})
  end

  @impl true
  def list_execution_admission_waiters_for_scope(scope, waiter_opts, opts)
      when is_map(scope) and is_list(waiter_opts) and is_list(opts) do
    with {:ok, normalized_scope} <- ExecutionLeaseCodec.normalize_scope(scope) do
      call(
        opts,
        {:list_execution_admission_waiters_for_scope, normalized_scope, waiter_opts}
      )
    end
  end

  @impl true
  def expire_execution_admission_waiters(%DateTime{} = now, opts) when is_list(opts) do
    call(opts, {:expire_execution_admission_waiters, now})
  end

  @impl true
  def try_acquire_materialization_claim(claim, opts) when is_map(claim) and is_list(opts) do
    with {:ok, normalized} <- MaterializationClaimCodec.normalize(claim) do
      call(opts, {:try_acquire_materialization_claim, normalized})
    end
  end

  @impl true
  def complete_materialization_claim(claim_key, completion, opts)
      when is_binary(claim_key) and is_map(completion) and is_list(opts) do
    call(opts, {:complete_materialization_claim, claim_key, completion})
  end

  @impl true
  def fail_materialization_claim(claim_key, failure, opts)
      when is_binary(claim_key) and is_map(failure) and is_list(opts) do
    call(opts, {:fail_materialization_claim, claim_key, failure})
  end

  @impl true
  def expire_materialization_claims(%DateTime{} = now, opts) when is_list(opts) do
    call(opts, {:expire_materialization_claims, now})
  end

  @impl true
  def get_materialization_claim(claim_key, opts) when is_binary(claim_key) and is_list(opts) do
    call(opts, {:get_materialization_claim, claim_key})
  end

  @impl true
  def list_materialization_claims(filters, opts) when is_list(filters) and is_list(opts) do
    with :ok <- Query.validate_filters(filters, @materialization_claim_filters) do
      call(opts, {:list_materialization_claims, filters})
    end
  end

  @impl true
  def persist_log_entries(entries, opts) when is_list(entries) and is_list(opts) do
    with {:ok, normalized} <- Logs.normalize_entries(entries) do
      call(opts, {:persist_log_entries, normalized})
    end
  end

  @impl true
  def list_logs(filter, opts, adapter_opts) when is_list(opts) and is_list(adapter_opts) do
    call(adapter_opts, {:list_logs, filter, opts})
  end

  @impl true
  def scan_logs(filter, scan_opts, adapter_opts)
      when is_list(scan_opts) and is_list(adapter_opts) do
    call(adapter_opts, {:scan_logs, filter, scan_opts})
  end

  @impl true
  def replay_logs_after(cursor, filter, opts, adapter_opts)
      when is_list(opts) and is_list(adapter_opts) do
    call(adapter_opts, {:replay_logs_after, cursor, filter, opts})
  end

  @impl true
  def put_scheduler_state(key, scheduler_state, opts)
      when is_map(scheduler_state) and is_list(opts) do
    with {:ok, normalized_key} <- SchedulerStateCodec.normalize_key(key),
         {:ok, normalized_state} <- SchedulerStateCodec.normalize_state(scheduler_state) do
      call(opts, {:put_scheduler_state, normalized_key, normalized_state})
    end
  end

  @spec put_scheduler_state(SchedulerState.t(), keyword()) :: :ok | {:error, term()}
  def put_scheduler_state(%SchedulerState{} = scheduler_state, opts) when is_list(opts) do
    key = {scheduler_state.pipeline_module, scheduler_state.schedule_id}

    payload =
      scheduler_state
      |> Map.from_struct()
      |> Map.drop([:pipeline_module, :schedule_id])

    put_scheduler_state(key, payload, opts)
  end

  @impl true
  def get_scheduler_state(key, opts \\ []) when is_list(opts) do
    with {:ok, normalized_key} <- SchedulerStateCodec.normalize_key(key) do
      call(opts, {:get_scheduler_state, normalized_key})
    end
  end

  @spec get_scheduler_state(module(), atom() | nil, keyword()) ::
          {:ok, SchedulerState.t() | nil} | {:error, term()}
  def get_scheduler_state(pipeline_module, schedule_id, opts)
      when is_atom(pipeline_module) and is_list(opts) do
    case get_scheduler_state({pipeline_module, schedule_id}, opts) do
      {:ok, nil} ->
        {:ok, nil}

      {:ok, state} when is_map(state) ->
        {:ok,
         struct(
           SchedulerState,
           Map.merge(state, %{pipeline_module: pipeline_module, schedule_id: schedule_id})
         )}

      other ->
        other
    end
  end

  @impl true
  def put_coverage_baseline(%CoverageBaseline{} = baseline, opts) when is_list(opts) do
    call(opts, {:put_coverage_baseline, baseline})
  end

  @impl true
  def get_coverage_baseline(baseline_id, opts) when is_binary(baseline_id) and is_list(opts) do
    call(opts, {:get_coverage_baseline, baseline_id})
  end

  @impl true
  def list_coverage_baselines(filters, opts) when is_list(filters) and is_list(opts) do
    call(opts, {:list_coverage_baselines, filters})
  end

  @impl true
  def put_backfill_window(%BackfillWindow{} = window, opts) when is_list(opts) do
    call(opts, {:put_backfill_window, window})
  end

  @impl true
  def put_backfill_windows(windows, opts) when is_list(windows) and is_list(opts) do
    call(opts, {:put_backfill_windows, windows})
  end

  @impl true
  def get_backfill_window(backfill_run_id, pipeline_module, window_key, opts)
      when is_binary(backfill_run_id) and is_atom(pipeline_module) and is_binary(window_key) and
             is_list(opts) do
    call(opts, {:get_backfill_window, {backfill_run_id, pipeline_module, window_key}})
  end

  @impl true
  def list_backfill_windows(filters, opts) when is_list(filters) and is_list(opts) do
    call(opts, {:list_backfill_windows, filters})
  end

  @impl true
  def scan_backfill_windows(filters, scan_opts, opts)
      when is_list(filters) and is_list(scan_opts) and is_list(opts) do
    call(opts, {:scan_backfill_windows, filters, scan_opts})
  end

  @impl true
  def apply_backfill_child_projection(%BackfillWindow{} = window, asset_window_states, opts)
      when is_list(asset_window_states) and is_list(opts) do
    call(opts, {:apply_backfill_child_projection, window, asset_window_states})
  end

  @impl true
  def get_backfill_progress(backfill_run_id, opts)
      when is_binary(backfill_run_id) and is_list(opts) do
    call(opts, {:get_backfill_progress, backfill_run_id})
  end

  @impl true
  def rebuild_backfill_progress(backfill_run_id, opts)
      when is_binary(backfill_run_id) and is_list(opts) do
    call(opts, {:rebuild_backfill_progress, backfill_run_id})
  end

  @impl true
  def put_asset_window_state(%AssetWindowState{} = state, opts) when is_list(opts) do
    call(opts, {:put_asset_window_state, state})
  end

  @impl true
  def put_asset_window_states(states, opts) when is_list(states) and is_list(opts) do
    call(opts, {:put_asset_window_states, states})
  end

  @impl true
  def get_asset_window_state(asset_ref_module, asset_ref_name, window_key, opts)
      when is_atom(asset_ref_module) and is_atom(asset_ref_name) and is_binary(window_key) and
             is_list(opts) do
    call(
      opts,
      {:get_asset_window_state, {asset_ref_module, asset_ref_name, window_key}}
    )
  end

  @impl true
  def list_asset_window_states(filters, opts) when is_list(filters) and is_list(opts) do
    call(opts, {:list_asset_window_states, filters})
  end

  @impl true
  def put_asset_freshness_state(%AssetFreshnessState{} = state, opts) when is_list(opts) do
    call(opts, {:put_asset_freshness_state, state})
  end

  @impl true
  def get_asset_freshness_state(asset_ref_module, asset_ref_name, freshness_key, opts)
      when is_atom(asset_ref_module) and is_atom(asset_ref_name) and is_binary(freshness_key) and
             is_list(opts) do
    call(
      opts,
      {:get_asset_freshness_state, {asset_ref_module, asset_ref_name, freshness_key}}
    )
  end

  @impl true
  def list_asset_freshness_states(filters, opts) when is_list(filters) and is_list(opts) do
    call(opts, {:list_asset_freshness_states, filters})
  end

  @impl true
  def scan_asset_freshness_states(filters, scan_opts, opts)
      when is_list(filters) and is_list(scan_opts) and is_list(opts) do
    call(opts, {:scan_asset_freshness_states, filters, scan_opts})
  end

  @impl true
  def get_asset_freshness_states_by_keys(keys, opts) when is_list(keys) and is_list(opts) do
    call(opts, {:get_asset_freshness_states_by_keys, keys})
  end

  @impl true
  def upsert_target_status(%TargetStatus{} = status, opts) when is_list(opts) do
    call(opts, {:upsert_target_status, status})
  end

  @impl true
  def get_target_status(manifest_version_id, target_kind, target_id, opts)
      when is_binary(manifest_version_id) and target_kind in [:asset, :pipeline] and
             is_binary(target_id) and is_list(opts) do
    call(opts, {:get_target_status, {manifest_version_id, target_kind, target_id}})
  end

  @impl true
  def list_target_statuses(manifest_version_id, target_kind, target_ids, opts)
      when is_binary(manifest_version_id) and target_kind in [:asset, :pipeline] and
             is_list(target_ids) and is_list(opts) do
    call(opts, {:list_target_statuses, manifest_version_id, target_kind, target_ids})
  end

  @impl true
  def replace_target_statuses(scope, statuses, opts) when is_list(statuses) and is_list(opts) do
    call(opts, {:replace_target_statuses, scope, statuses})
  end

  @impl true
  def delete_target_statuses(scope, opts) when is_list(opts) do
    call(opts, {:delete_target_statuses, scope})
  end

  @impl true
  def replace_backfill_read_models(
        scope,
        coverage_baselines,
        backfill_windows,
        asset_window_states,
        opts
      )
      when (scope == :all or is_tuple(scope)) and is_list(coverage_baselines) and
             is_list(backfill_windows) and
             is_list(asset_window_states) and is_list(opts) do
    call(
      opts,
      {:replace_backfill_read_models, scope, coverage_baselines, backfill_windows,
       asset_window_states}
    )
  end

  @impl true
  def put_auth_actor(actor, opts) when is_map(actor) and is_list(opts) do
    call(opts, {:put_auth_actor, actor})
  end

  @impl true
  def put_auth_actor_with_credential(actor, credential, opts)
      when is_map(actor) and is_map(credential) and is_list(opts) do
    call(opts, {:put_auth_actor_with_credential, actor, credential})
  end

  @impl true
  def get_auth_actor(actor_id, opts) when is_binary(actor_id) and is_list(opts) do
    call(opts, {:get_auth_actor, actor_id})
  end

  @impl true
  def get_auth_actor_by_username(username, opts) when is_binary(username) and is_list(opts) do
    call(opts, {:get_auth_actor_by_username, username})
  end

  @impl true
  def list_auth_actors(opts) when is_list(opts) do
    call(opts, :list_auth_actors)
  end

  @impl true
  def put_auth_credential(actor_id, credential, opts)
      when is_binary(actor_id) and is_map(credential) and is_list(opts) do
    call(opts, {:put_auth_credential, actor_id, credential})
  end

  @impl true
  def update_auth_actor_password(actor_id, actor, credential, revoked_at, opts)
      when is_binary(actor_id) and is_map(actor) and is_map(credential) and
             is_struct(revoked_at, DateTime) and is_list(opts) do
    call(opts, {:update_auth_actor_password, actor_id, actor, credential, revoked_at})
  end

  @impl true
  def get_auth_credential(actor_id, opts) when is_binary(actor_id) and is_list(opts) do
    call(opts, {:get_auth_credential, actor_id})
  end

  @impl true
  def put_auth_session(session, opts) when is_map(session) and is_list(opts) do
    call(opts, {:put_auth_session, session})
  end

  @impl true
  def get_auth_session(session_id, opts) when is_binary(session_id) and is_list(opts) do
    call(opts, {:get_auth_session, session_id})
  end

  @impl true
  def get_auth_session_by_token_hash(token_hash, opts)
      when is_binary(token_hash) and is_list(opts) do
    call(opts, {:get_auth_session_by_token_hash, token_hash})
  end

  @impl true
  def revoke_auth_session(session_id, revoked_at, opts)
      when is_binary(session_id) and is_struct(revoked_at, DateTime) and is_list(opts) do
    call(opts, {:revoke_auth_session, session_id, revoked_at})
  end

  @impl true
  def revoke_auth_sessions_for_actor(actor_id, revoked_at, opts)
      when is_binary(actor_id) and is_struct(revoked_at, DateTime) and is_list(opts) do
    call(opts, {:revoke_auth_sessions_for_actor, actor_id, revoked_at})
  end

  @impl true
  def put_auth_audit(entry, opts) when is_map(entry) and is_list(opts) do
    call(opts, {:put_auth_audit, entry})
  end

  @impl true
  def list_auth_audit(audit_opts, opts) when is_list(audit_opts) and is_list(opts) do
    call(opts, {:list_auth_audit, audit_opts})
  end

  @impl true
  def reserve_idempotency_record(record, opts) when is_map(record) and is_list(opts) do
    call(opts, {:reserve_idempotency_record, record})
  end

  @impl true
  def complete_idempotency_record(record_id, attrs, opts)
      when is_binary(record_id) and is_map(attrs) and is_list(opts) do
    call(opts, {:complete_idempotency_record, record_id, attrs})
  end

  @impl true
  def get_idempotency_record(record_id, opts) when is_binary(record_id) and is_list(opts) do
    call(opts, {:get_idempotency_record, record_id})
  end

  @impl true
  def init(_args) do
    {:ok, State.new()}
  end

  @impl true
  def handle_call(:reset, _from, _state) do
    {:reply, :ok, State.new()}
  end

  def handle_call({:put_manifest_version, %Version{} = version}, _from, state) do
    {reply, next_state} = Manifests.put(state, version)
    {:reply, reply, next_state}
  end

  def handle_call({:get_manifest_version, manifest_version_id}, _from, state) do
    {:reply, Manifests.get(state, manifest_version_id), state}
  end

  def handle_call({:get_manifest_version_by_content_hash, content_hash}, _from, state) do
    {:reply, Manifests.get_by_content_hash(state, content_hash), state}
  end

  def handle_call(:list_manifest_versions, _from, state) do
    {:reply, {:ok, Manifests.list(state)}, state}
  end

  def handle_call({:set_active_manifest_version, manifest_version_id}, _from, state) do
    {reply, next_state} = Manifests.activate(state, manifest_version_id)
    {:reply, reply, next_state}
  end

  def handle_call(:get_active_manifest_version, _from, state) do
    reply =
      case state.active_manifest_version_id do
        nil -> {:error, :active_manifest_not_set}
        manifest_version_id -> {:ok, manifest_version_id}
      end

    {:reply, reply, state}
  end

  def handle_call({:put_run, %RunState{} = incoming}, _from, state) do
    {reply, next_state} = Runs.put(state, incoming)

    normalized_reply = if reply == :idempotent, do: :ok, else: reply
    next_state = ExecutionGroups.refresh(next_state, incoming)

    {:reply, normalized_reply, next_state}
  end

  def handle_call({:create_runtime_input_pin, %Pin{} = pin}, _from, state) do
    key = {pin.run_id, pin.node_key}

    case Map.get(state.runtime_input_pins, key) do
      nil ->
        {:reply, {:ok, pin},
         %{state | runtime_input_pins: Map.put(state.runtime_input_pins, key, pin)}}

      %Pin{} = existing ->
        if Pin.equivalent?(existing, pin),
          do: {:reply, {:ok, existing}, state},
          else: {:reply, {:error, :runtime_input_pin_conflict}, state}
    end
  end

  def handle_call({:get_runtime_input_pin, run_id, node_key}, _from, state) do
    reply =
      case Map.fetch(state.runtime_input_pins, {run_id, node_key}) do
        {:ok, pin} -> {:ok, pin}
        :error -> {:error, :runtime_input_pin_not_found}
      end

    {:reply, reply, state}
  end

  def handle_call({:list_runtime_input_pins, run_id}, _from, state) do
    pins =
      state.runtime_input_pins
      |> Enum.flat_map(fn
        {{^run_id, _node_key}, pin} -> [pin]
        _other -> []
      end)
      |> Enum.sort_by(& &1.inserted_at, DateTime)

    {:reply, {:ok, pins}, state}
  end

  def handle_call({:persist_run_transition, %RunState{} = run, event}, _from, state) do
    {run_reply, run_state} = Runs.put(state, run)

    case run_reply do
      run_write_result when run_write_result in [:ok, :idempotent] ->
        current = Map.get(state.run_events, run.id, [])

        case RunEvents.append(current, event, state.run_event_global_sequence) do
          {:ok, event_write_result, next_events, next_global_sequence} ->
            next_state =
              %{
                run_state
                | run_events: Map.put(run_state.run_events, run.id, next_events),
                  run_event_global_sequence: next_global_sequence
              }
              |> ExecutionGroups.refresh(run)

            result =
              case {run_write_result, event_write_result} do
                {:idempotent, :idempotent} -> :idempotent
                _ -> :ok
              end

            {:reply, result, next_state}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:get_run, run_id}, _from, state) do
    {:reply, Runs.get(state, run_id), state}
  end

  def handle_call({:put_execution_ownership, ownership}, _from, state) do
    {:reply, :ok,
     %{
       state
       | execution_ownerships:
           Map.put(state.execution_ownerships, ownership.ownership_id, ownership)
     }}
  end

  def handle_call({:get_execution_ownership, ownership_id}, _from, state) do
    {:reply, Query.fetch(state.execution_ownerships, ownership_id), state}
  end

  def handle_call({:list_execution_ownerships, run_id}, _from, state) do
    ownerships =
      state.execution_ownerships
      |> Map.values()
      |> Enum.filter(&(&1.run_id == run_id))
      |> Enum.sort_by(&{&1.inserted_at, &1.ownership_id})

    {:reply, {:ok, ownerships}, state}
  end

  def handle_call({:list_active_execution_ownerships, run_id}, _from, state) do
    ownerships =
      state.execution_ownerships
      |> Map.values()
      |> Enum.filter(
        &(&1.run_id == run_id and FavnOrchestrator.RunExecutionOwnership.active?(&1))
      )
      |> Enum.sort_by(&{&1.inserted_at, &1.ownership_id})

    {:reply, {:ok, ownerships}, state}
  end

  def handle_call({:list_runs, run_opts}, _from, state) do
    {:reply, {:ok, Runs.list(state, run_opts)}, state}
  end

  def handle_call(
        {:list_target_runs, manifest_version_id, target_kind, target_ref, run_opts},
        _from,
        state
      ) do
    runs = Runs.list_target(state, manifest_version_id, target_kind, target_ref, run_opts)
    {:reply, {:ok, runs}, state}
  end

  def handle_call({:list_execution_group_runs, group_id}, _from, state) do
    {:reply, {:ok, ExecutionGroups.runs(state, group_id)}, state}
  end

  def handle_call({:list_execution_group_run_ids, group_id}, _from, state) do
    {:reply, {:ok, ExecutionGroups.run_ids(state, group_id)}, state}
  end

  def handle_call({:list_execution_groups, group_opts}, _from, state) do
    {:reply, {:ok, ExecutionGroups.list(state, group_opts)}, state}
  end

  def handle_call({:list_execution_group_summaries, group_opts}, _from, state) do
    {:reply, {:ok, ExecutionGroups.list_summaries(state, group_opts)}, state}
  end

  def handle_call(:rebuild_execution_group_summaries, _from, state) do
    {count, next_state} = ExecutionGroups.rebuild(state)
    {:reply, {:ok, count}, next_state}
  end

  def handle_call({:append_run_event, run_id, event}, _from, state) do
    current = Map.get(state.run_events, run_id, [])

    case RunEvents.append(current, event, state.run_event_global_sequence) do
      {:ok, _event_write_result, next_events, next_global_sequence} ->
        next_state = %{
          state
          | run_events: Map.put(state.run_events, run_id, next_events),
            run_event_global_sequence: next_global_sequence
        }

        {:reply, :ok, next_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:list_run_events, run_id}, _from, state) do
    events = Map.get(state.run_events, run_id, [])
    {:reply, {:ok, events}, state}
  end

  def handle_call({:list_run_events, run_id, run_event_opts}, _from, state) do
    events = Map.get(state.run_events, run_id, [])
    {:reply, RunEvents.list(events, run_event_opts), state}
  end

  def handle_call({:list_execution_group_events, group_id, run_event_opts}, _from, state) do
    run_ids = MapSet.new(ExecutionGroups.run_ids(state, group_id))

    events =
      state.run_events
      |> Map.values()
      |> List.flatten()
      |> Enum.filter(&(Map.get(&1, :run_id) in run_ids))

    {:reply, RunEvents.list_group(events, run_event_opts), state}
  end

  def handle_call({:list_global_run_events, opts}, _from, state) do
    {:reply, RunEvents.list_global(state.run_events, opts), state}
  end

  def handle_call({:try_acquire_execution_lease, lease}, _from, state) do
    {reply, next_state} = MemoryAdmission.acquire(state, lease)
    {:reply, reply, next_state}
  end

  def handle_call({:release_execution_lease, lease_id}, _from, state) do
    {:reply, :ok, MemoryAdmission.release(state, lease_id)}
  end

  def handle_call({:release_execution_leases_for_run, run_id}, _from, state) do
    {release, next_state} = MemoryAdmission.release_for_run(state, run_id)
    {:reply, {:ok, release}, next_state}
  end

  def handle_call({:expire_execution_leases, now}, _from, state) do
    {expired_count, next_state} = MemoryAdmission.expire(state, now)
    {:reply, {:ok, expired_count}, next_state}
  end

  def handle_call(:list_execution_leases, _from, state) do
    {:reply, {:ok, MemoryAdmission.list(state)}, state}
  end

  def handle_call({:upsert_execution_admission_waiter, waiter}, _from, state) do
    {next_waiter, next_state} = MemoryAdmission.upsert_waiter(state, waiter)
    {:reply, {:ok, next_waiter}, next_state}
  end

  def handle_call({:delete_execution_admission_waiter, waiter_id}, _from, state) do
    {:reply, :ok, MemoryAdmission.delete_waiter(state, waiter_id)}
  end

  def handle_call({:delete_execution_admission_waiters_for_run, run_id}, _from, state) do
    {deleted, next_state} = MemoryAdmission.delete_waiters_for_run(state, run_id)
    {:reply, {:ok, deleted}, next_state}
  end

  def handle_call({:list_execution_admission_waiters_for_scope, scope, opts}, _from, state) do
    {:reply, {:ok, MemoryAdmission.list_waiters_for_scope(state, scope, opts)}, state}
  end

  def handle_call({:expire_execution_admission_waiters, now}, _from, state) do
    {expired, next_state} = MemoryAdmission.expire_waiters(state, now)
    {:reply, {:ok, expired}, next_state}
  end

  def handle_call({:try_acquire_materialization_claim, claim}, _from, state) do
    {reply, next_state} = MaterializationClaims.acquire(state, claim)
    {:reply, reply, next_state}
  end

  def handle_call({:complete_materialization_claim, claim_key, completion}, _from, state) do
    {reply, next_state} = MaterializationClaims.complete(state, claim_key, completion)
    {:reply, reply, next_state}
  end

  def handle_call({:fail_materialization_claim, claim_key, failure}, _from, state) do
    {reply, next_state} = MaterializationClaims.fail(state, claim_key, failure)
    {:reply, reply, next_state}
  end

  def handle_call({:expire_materialization_claims, now}, _from, state) do
    {expired_count, next_state} = MaterializationClaims.expire(state, now)
    {:reply, {:ok, expired_count}, next_state}
  end

  def handle_call({:get_materialization_claim, claim_key}, _from, state) do
    {:reply, MaterializationClaims.get(state, claim_key), state}
  end

  def handle_call({:list_materialization_claims, filters}, _from, state) do
    {:reply, {:ok, MaterializationClaims.list(state, filters)}, state}
  end

  def handle_call({:persist_log_entries, entries}, _from, state) do
    {persisted, next_state} = Logs.persist(state, entries)
    {:reply, {:ok, persisted}, next_state}
  end

  def handle_call({:list_logs, filter, opts}, _from, state) do
    {:reply, Logs.list(state, filter, opts), state}
  end

  def handle_call({:scan_logs, filter, scan_opts}, _from, state) do
    {:reply, Logs.scan(state, filter, scan_opts), state}
  end

  def handle_call({:replay_logs_after, cursor, filter, opts}, _from, state) do
    {:reply, Logs.replay(state, cursor, filter, opts), state}
  end

  def handle_call({:put_scheduler_state, key, scheduler_state}, _from, state) do
    current = Map.get(state.scheduler_states, key)
    incoming_version = Map.get(scheduler_state, :version)

    reply =
      case {current, incoming_version} do
        {nil, 1} ->
          :ok

        {nil, _other} ->
          {:error, :invalid_scheduler_version}

        {%{version: version}, incoming} when is_integer(version) and incoming == version + 1 ->
          :ok

        {%{version: _version}, _incoming} ->
          {:error, :stale_scheduler_state}
      end

    next_state =
      case reply do
        :ok ->
          %{state | scheduler_states: Map.put(state.scheduler_states, key, scheduler_state)}

        _ ->
          state
      end

    {:reply, reply, next_state}
  end

  def handle_call({:get_scheduler_state, key}, _from, state) do
    value =
      case Map.get(state.scheduler_states, key) do
        nil ->
          nil

        stored when is_map(stored) ->
          {pipeline_module, schedule_id} = key

          struct(
            SchedulerState,
            Map.merge(stored, %{pipeline_module: pipeline_module, schedule_id: schedule_id})
          )
      end

    {:reply, {:ok, value}, state}
  end

  def handle_call({:put_coverage_baseline, %CoverageBaseline{} = baseline}, _from, state) do
    {:reply, :ok, Backfills.put_baseline(state, baseline)}
  end

  def handle_call({:get_coverage_baseline, baseline_id}, _from, state) do
    {:reply, Backfills.get_baseline(state, baseline_id), state}
  end

  def handle_call({:list_coverage_baselines, filters}, _from, state) do
    {:reply, Backfills.list_baselines(state, filters), state}
  end

  def handle_call({:put_backfill_window, %BackfillWindow{} = window}, _from, state) do
    put_backfill_windows_with_progress(state, [window])
  end

  def handle_call({:put_backfill_windows, windows}, _from, state) do
    put_backfill_windows_with_progress(state, windows)
  end

  def handle_call({:get_backfill_window, key}, _from, state) do
    {:reply, Backfills.get_window(state, key), state}
  end

  def handle_call({:list_backfill_windows, filters}, _from, state) do
    {:reply, Backfills.list_windows(state, filters), state}
  end

  def handle_call({:scan_backfill_windows, filters, scan_opts}, _from, state) do
    {:reply, Backfills.scan_windows(state, filters, scan_opts), state}
  end

  def handle_call(
        {:apply_backfill_child_projection, %BackfillWindow{} = window, asset_window_states},
        _from,
        state
      ) do
    {reply, next_state} = Backfills.apply_child_projection(state, window, asset_window_states)

    next_state =
      if match?({:ok, _progress}, reply),
        do: ExecutionGroups.refresh(next_state, window.backfill_run_id),
        else: next_state

    {:reply, reply, next_state}
  end

  def handle_call({:get_backfill_progress, backfill_run_id}, _from, state) do
    {reply, next_state} = Backfills.get_progress(state, backfill_run_id)
    {:reply, reply, next_state}
  end

  def handle_call({:rebuild_backfill_progress, backfill_run_id}, _from, state) do
    {reply, next_state} = Backfills.rebuild_progress(state, backfill_run_id)
    {:reply, reply, next_state}
  end

  def handle_call({:put_asset_window_state, %AssetWindowState{} = window_state}, _from, state) do
    {:reply, :ok, Backfills.put_asset_window_state(state, window_state)}
  end

  def handle_call({:put_asset_window_states, window_states}, _from, state) do
    {:reply, :ok, Backfills.put_asset_window_states(state, window_states)}
  end

  def handle_call({:get_asset_window_state, key}, _from, state) do
    {:reply, Backfills.get_asset_window_state(state, key), state}
  end

  def handle_call({:list_asset_window_states, filters}, _from, state) do
    {:reply, Backfills.list_asset_window_states(state, filters), state}
  end

  def handle_call(
        {:put_asset_freshness_state, %AssetFreshnessState{} = freshness_state},
        _from,
        state
      ) do
    {:reply, :ok, Freshness.put(state, freshness_state)}
  end

  def handle_call({:get_asset_freshness_state, key}, _from, state) do
    {:reply, Freshness.get(state, key), state}
  end

  def handle_call({:list_asset_freshness_states, filters}, _from, state) do
    {:reply, Freshness.list(state, filters), state}
  end

  def handle_call({:scan_asset_freshness_states, filters, scan_opts}, _from, state) do
    {:reply, Freshness.scan(state, filters, scan_opts), state}
  end

  def handle_call({:get_asset_freshness_states_by_keys, keys}, _from, state) do
    {:reply, {:ok, Freshness.get_by_keys(state, keys)}, state}
  end

  def handle_call({:upsert_target_status, %TargetStatus{} = status}, _from, state) do
    {:reply, :ok, TargetStatuses.put(state, status)}
  end

  def handle_call({:get_target_status, key}, _from, state) do
    {:reply, TargetStatuses.get(state, key), state}
  end

  def handle_call(
        {:list_target_statuses, manifest_version_id, target_kind, target_ids},
        _from,
        state
      ) do
    rows = TargetStatuses.list(state, manifest_version_id, target_kind, target_ids)
    {:reply, {:ok, rows}, state}
  end

  def handle_call({:replace_target_statuses, scope, statuses}, _from, state) do
    case TargetStatuses.replace(state, scope, statuses) do
      {:ok, next_state} -> {:reply, :ok, next_state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:delete_target_statuses, scope}, _from, state) do
    case TargetStatuses.delete(state, scope) do
      {:ok, next_state} -> {:reply, :ok, next_state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call(
        {:replace_backfill_read_models, scope, coverage_baselines, backfill_windows,
         asset_window_states},
        _from,
        state
      ) do
    case Backfills.replace(
           state,
           scope,
           coverage_baselines,
           backfill_windows,
           asset_window_states
         ) do
      {:ok, affected_ids, next_state} ->
        {:reply, :ok, ExecutionGroups.refresh_many(next_state, affected_ids)}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:put_auth_actor, actor}, _from, state) do
    {reply, next_state} = MemoryAuth.put_actor(state, actor)
    {:reply, reply, next_state}
  end

  def handle_call({:put_auth_actor_with_credential, actor, credential}, _from, state) do
    {reply, next_state} = MemoryAuth.put_actor_with_credential(state, actor, credential)
    {:reply, reply, next_state}
  end

  def handle_call({:get_auth_actor, actor_id}, _from, state) do
    {:reply, MemoryAuth.get_actor(state, actor_id), state}
  end

  def handle_call({:get_auth_actor_by_username, username}, _from, state) do
    {:reply, MemoryAuth.get_actor_by_username(state, username), state}
  end

  def handle_call(:list_auth_actors, _from, state) do
    {:reply, {:ok, MemoryAuth.list_actors(state)}, state}
  end

  def handle_call({:put_auth_credential, actor_id, credential}, _from, state) do
    {:reply, :ok, MemoryAuth.put_credential(state, actor_id, credential)}
  end

  def handle_call(
        {:update_auth_actor_password, actor_id, actor, credential, revoked_at},
        _from,
        state
      ) do
    {reply, next_state} =
      MemoryAuth.update_password(state, actor_id, actor, credential, revoked_at)

    {:reply, reply, next_state}
  end

  def handle_call({:get_auth_credential, actor_id}, _from, state) do
    {:reply, MemoryAuth.get_credential(state, actor_id), state}
  end

  def handle_call({:put_auth_session, session}, _from, state) do
    {reply, next_state} = MemoryAuth.put_session(state, session)
    {:reply, reply, next_state}
  end

  def handle_call({:get_auth_session, session_id}, _from, state) do
    {:reply, MemoryAuth.get_session(state, session_id), state}
  end

  def handle_call({:get_auth_session_by_token_hash, token_hash}, _from, state) do
    {:reply, MemoryAuth.get_session_by_token_hash(state, token_hash), state}
  end

  def handle_call({:revoke_auth_session, session_id, revoked_at}, _from, state) do
    {reply, next_state} = MemoryAuth.revoke_session(state, session_id, revoked_at)
    {:reply, reply, next_state}
  end

  def handle_call({:revoke_auth_sessions_for_actor, actor_id, revoked_at}, _from, state) do
    {:reply, :ok, MemoryAuth.revoke_sessions_for_actor(state, actor_id, revoked_at)}
  end

  def handle_call({:put_auth_audit, entry}, _from, state) do
    {:reply, :ok, MemoryAuth.put_audit(state, entry)}
  end

  def handle_call({:list_auth_audit, opts}, _from, state) do
    {:reply, {:ok, MemoryAuth.list_audit(state, opts)}, state}
  end

  def handle_call({:reserve_idempotency_record, record}, _from, state) do
    {reply, next_state} = Idempotency.reserve(state, record)
    {:reply, reply, next_state}
  end

  def handle_call({:complete_idempotency_record, record_id, attrs}, _from, state) do
    {reply, next_state} = Idempotency.complete(state, record_id, attrs)
    {:reply, reply, next_state}
  end

  def handle_call({:get_idempotency_record, record_id}, _from, state) do
    {:reply, Idempotency.get(state, record_id), state}
  end

  defp put_backfill_windows_with_progress(state, windows) do
    case Backfills.put_windows_with_progress(state, windows) do
      {:ok, next_state} ->
        next_state =
          ExecutionGroups.refresh_many(next_state, Enum.map(windows, & &1.backfill_run_id))

        {:reply, :ok, next_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  defp validate_transition_alignment(%RunState{} = run, event) when is_map(event) do
    cond do
      Map.get(event, :run_id) != run.id ->
        {:error, :invalid_run_event_run_id}

      Map.get(event, :sequence) != run.event_seq ->
        {:error, :invalid_run_event_sequence}

      true ->
        :ok
    end
  end

  defp call(opts, message) do
    opts
    |> Keyword.get(:server, __MODULE__)
    |> GenServer.call(message)
  end

  defp runtime_name(opts) do
    Keyword.get(opts, :server, Keyword.get(opts, :name, __MODULE__))
  end

  defp runtime_started?(name) when is_atom(name), do: Process.whereis(name) != nil
  defp runtime_started?(_name), do: false
end
