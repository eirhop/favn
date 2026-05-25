defmodule FavnOrchestrator.Storage.Adapter.Memory do
  @moduledoc false

  use GenServer

  @behaviour Favn.Storage.Adapter

  alias Favn.Manifest.Version
  alias Favn.Scheduler.State, as: SchedulerState
  alias FavnOrchestrator.AssetFreshnessState
  alias FavnOrchestrator.Backfill.AssetWindowState
  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.Backfill.CoverageBaseline
  alias FavnOrchestrator.Backfill.Progress, as: BackfillProgress
  alias FavnOrchestrator.CursorPage
  alias FavnOrchestrator.MaterializationClaim
  alias FavnOrchestrator.Page
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.ExecutionAdmission.LeaseRelease
  alias FavnOrchestrator.Storage.ExecutionLeaseCodec
  alias FavnOrchestrator.Storage.LogEntryCodec
  alias FavnOrchestrator.Storage.ExecutionAdmissionWaiterCodec
  alias FavnOrchestrator.Storage.ExecutionGroupSummary
  alias FavnOrchestrator.Storage.MaterializationClaimCodec
  alias FavnOrchestrator.Storage.RunEventCodec
  alias FavnOrchestrator.Storage.RunQuery
  alias FavnOrchestrator.Storage.RunStateCodec
  alias FavnOrchestrator.Storage.SchedulerStateCodec
  alias FavnOrchestrator.Storage.WriteSemantics
  alias FavnOrchestrator.TargetStatus

  @log_filter_keys [
    :run_id,
    :asset_step_id,
    :runner_execution_id,
    :level,
    :source,
    :stream,
    :levels,
    :sources,
    :since,
    :until,
    :asset_ref,
    :node_key,
    :after_global_sequence
  ]

  @type state :: %{
          manifests: %{required(String.t()) => Version.t()},
          active_manifest_version_id: String.t() | nil,
          runs: %{required(String.t()) => RunState.t()},
          execution_group_summaries: %{required(String.t()) => map()},
          run_events: %{required(String.t()) => [map()]},
          run_event_global_sequence: non_neg_integer(),
          execution_leases: %{required(String.t()) => map()},
          execution_lease_ids_by_run: %{required(String.t()) => MapSet.t(String.t())},
          execution_admission_waiters: %{required(String.t()) => map()},
          materialization_claims: %{required(String.t()) => MaterializationClaim.t()},
          log_entries: [Favn.Log.Entry.t()],
          log_global_sequence: non_neg_integer(),
          scheduler_states: %{required({module(), atom() | nil}) => map()},
          coverage_baselines: %{required(String.t()) => CoverageBaseline.t()},
          backfill_windows: %{required({String.t(), module(), String.t()}) => BackfillWindow.t()},
          backfill_progress: %{required(String.t()) => BackfillProgress.t()},
          asset_window_states: %{required({module(), atom(), String.t()}) => AssetWindowState.t()},
          asset_freshness_states: %{
            required({module(), atom(), String.t()}) => AssetFreshnessState.t()
          },
          target_statuses: %{
            required({String.t(), TargetStatus.target_kind(), String.t()}) => TargetStatus.t()
          },
          auth_actors: %{required(String.t()) => map()},
          auth_usernames: %{required(String.t()) => String.t()},
          auth_credentials: %{required(String.t()) => map()},
          auth_sessions: %{required(String.t()) => map()},
          auth_session_hashes: %{required(String.t()) => String.t()},
          auth_audits: [map()],
          idempotency_records: %{required(String.t()) => map()}
        }

  @asset_freshness_state_filters [
    :asset_ref_module,
    :asset_ref_name,
    :freshness_key,
    :status,
    :freshness_version,
    :latest_success_run_id,
    :latest_attempt_run_id,
    :latest_attempt_status,
    :manifest_version_id,
    :manifest_content_hash
  ]

  @backfill_window_filters [
    :backfill_run_id,
    :pipeline_module,
    :window_key,
    :window_kind,
    :status,
    :coverage_baseline_id,
    :manifest_version_id
  ]

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
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, :reset)
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
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:put_manifest_version, version})
  end

  @impl true
  def get_manifest_version(manifest_version_id, opts \\ []) when is_binary(manifest_version_id) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:get_manifest_version, manifest_version_id})
  end

  @impl true
  def get_manifest_version_by_content_hash(content_hash, opts \\ [])
      when is_binary(content_hash) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:get_manifest_version_by_content_hash, content_hash})
  end

  @impl true
  def list_manifest_versions(opts \\ []) when is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, :list_manifest_versions)
  end

  @impl true
  def set_active_manifest_version(manifest_version_id, opts \\ [])
      when is_binary(manifest_version_id) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:set_active_manifest_version, manifest_version_id})
  end

  @impl true
  def get_active_manifest_version(opts \\ []) when is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, :get_active_manifest_version)
  end

  @impl true
  def put_run(%RunState{} = run, opts \\ []) when is_list(opts) do
    with {:ok, normalized} <- RunStateCodec.normalize(run) do
      server = Keyword.get(opts, :server, __MODULE__)
      GenServer.call(server, {:put_run, normalized})
    end
  end

  @impl true
  def persist_run_transition(%RunState{} = run, event, opts)
      when is_map(event) and is_list(opts) do
    with {:ok, normalized_run} <- RunStateCodec.normalize(run),
         {:ok, normalized_event} <- RunEventCodec.normalize(run.id, event),
         :ok <- validate_transition_alignment(normalized_run, normalized_event) do
      server = Keyword.get(opts, :server, __MODULE__)
      GenServer.call(server, {:persist_run_transition, normalized_run, normalized_event})
    end
  end

  @impl true
  def get_run(run_id, opts \\ []) when is_binary(run_id) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:get_run, run_id})
  end

  @impl true
  def list_runs(run_opts \\ [], adapter_opts \\ [])
      when is_list(run_opts) and is_list(adapter_opts) do
    server = Keyword.get(adapter_opts, :server, __MODULE__)
    GenServer.call(server, {:list_runs, run_opts})
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
    server = Keyword.get(adapter_opts, :server, __MODULE__)

    GenServer.call(
      server,
      {:list_target_runs, manifest_version_id, target_kind, target_ref, run_opts}
    )
  end

  @impl true
  def list_execution_group_runs(group_id, opts \\ [])
      when is_binary(group_id) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:list_execution_group_runs, group_id})
  end

  @impl true
  def list_execution_group_run_ids(group_id, opts \\ [])
      when is_binary(group_id) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:list_execution_group_run_ids, group_id})
  end

  @impl true
  def list_execution_groups(group_opts, opts) when is_list(group_opts) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:list_execution_groups, group_opts})
  end

  @impl true
  def list_execution_group_summaries(group_opts, opts)
      when is_list(group_opts) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:list_execution_group_summaries, group_opts})
  end

  @impl true
  def rebuild_execution_group_summaries(opts) when is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, :rebuild_execution_group_summaries)
  end

  @impl true
  def append_run_event(run_id, event, opts \\ [])
      when is_binary(run_id) and is_map(event) and is_list(opts) do
    with {:ok, normalized} <- RunEventCodec.normalize(run_id, event) do
      server = Keyword.get(opts, :server, __MODULE__)
      GenServer.call(server, {:append_run_event, run_id, normalized})
    end
  end

  @impl true
  def list_run_events(run_id, opts \\ []) when is_binary(run_id) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:list_run_events, run_id})
  end

  @impl true
  def list_run_events(run_id, run_event_opts, opts)
      when is_binary(run_id) and is_list(run_event_opts) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:list_run_events, run_id, run_event_opts})
  end

  @impl true
  def list_execution_group_events(group_id, run_event_opts, opts)
      when is_binary(group_id) and is_list(run_event_opts) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:list_execution_group_events, group_id, run_event_opts})
  end

  @impl true
  def list_global_run_events(run_event_opts, opts)
      when is_list(run_event_opts) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:list_global_run_events, run_event_opts})
  end

  @impl true
  def try_acquire_execution_lease(lease, opts) when is_map(lease) and is_list(opts) do
    with {:ok, normalized} <- normalize_execution_lease(lease) do
      server = Keyword.get(opts, :server, __MODULE__)
      GenServer.call(server, {:try_acquire_execution_lease, normalized})
    end
  end

  @impl true
  def release_execution_lease(lease_id, opts) when is_binary(lease_id) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:release_execution_lease, lease_id})
  end

  @impl true
  def release_execution_leases_for_run(run_id, opts) when is_binary(run_id) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:release_execution_leases_for_run, run_id})
  end

  @impl true
  def expire_execution_leases(%DateTime{} = now, opts) when is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:expire_execution_leases, now})
  end

  @impl true
  def list_execution_leases(opts) when is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, :list_execution_leases)
  end

  @impl true
  def upsert_execution_admission_waiter(waiter, opts) when is_map(waiter) and is_list(opts) do
    with {:ok, normalized} <- ExecutionAdmissionWaiterCodec.normalize(waiter) do
      server = Keyword.get(opts, :server, __MODULE__)
      GenServer.call(server, {:upsert_execution_admission_waiter, normalized})
    end
  end

  @impl true
  def delete_execution_admission_waiter(waiter_id, opts)
      when is_binary(waiter_id) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:delete_execution_admission_waiter, waiter_id})
  end

  @impl true
  def delete_execution_admission_waiters_for_run(run_id, opts)
      when is_binary(run_id) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:delete_execution_admission_waiters_for_run, run_id})
  end

  @impl true
  def list_execution_admission_waiters_for_scope(scope, waiter_opts, opts)
      when is_map(scope) and is_list(waiter_opts) and is_list(opts) do
    with {:ok, normalized_scope} <- normalize_execution_lease_scope(scope) do
      server = Keyword.get(opts, :server, __MODULE__)

      GenServer.call(
        server,
        {:list_execution_admission_waiters_for_scope, normalized_scope, waiter_opts}
      )
    end
  end

  @impl true
  def expire_execution_admission_waiters(%DateTime{} = now, opts) when is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:expire_execution_admission_waiters, now})
  end

  @impl true
  def try_acquire_materialization_claim(claim, opts) when is_map(claim) and is_list(opts) do
    with {:ok, normalized} <- MaterializationClaimCodec.normalize(claim) do
      server = Keyword.get(opts, :server, __MODULE__)
      GenServer.call(server, {:try_acquire_materialization_claim, normalized})
    end
  end

  @impl true
  def complete_materialization_claim(claim_key, completion, opts)
      when is_binary(claim_key) and is_map(completion) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:complete_materialization_claim, claim_key, completion})
  end

  @impl true
  def fail_materialization_claim(claim_key, failure, opts)
      when is_binary(claim_key) and is_map(failure) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:fail_materialization_claim, claim_key, failure})
  end

  @impl true
  def expire_materialization_claims(%DateTime{} = now, opts) when is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:expire_materialization_claims, now})
  end

  @impl true
  def get_materialization_claim(claim_key, opts) when is_binary(claim_key) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:get_materialization_claim, claim_key})
  end

  @impl true
  def list_materialization_claims(filters, opts) when is_list(filters) and is_list(opts) do
    with :ok <- validate_filters(filters, @materialization_claim_filters) do
      server = Keyword.get(opts, :server, __MODULE__)
      GenServer.call(server, {:list_materialization_claims, filters})
    end
  end

  @impl true
  def persist_log_entries(entries, opts) when is_list(entries) and is_list(opts) do
    with {:ok, normalized} <- normalize_log_entries(entries) do
      server = Keyword.get(opts, :server, __MODULE__)
      GenServer.call(server, {:persist_log_entries, normalized})
    end
  end

  @impl true
  def list_logs(filter, opts, adapter_opts) when is_list(opts) and is_list(adapter_opts) do
    server = Keyword.get(adapter_opts, :server, __MODULE__)
    GenServer.call(server, {:list_logs, filter, opts})
  end

  @impl true
  def scan_logs(filter, scan_opts, adapter_opts)
      when is_list(scan_opts) and is_list(adapter_opts) do
    server = Keyword.get(adapter_opts, :server, __MODULE__)
    GenServer.call(server, {:scan_logs, filter, scan_opts})
  end

  @impl true
  def replay_logs_after(cursor, filter, opts, adapter_opts)
      when is_list(opts) and is_list(adapter_opts) do
    server = Keyword.get(adapter_opts, :server, __MODULE__)
    GenServer.call(server, {:replay_logs_after, cursor, filter, opts})
  end

  @impl true
  def put_scheduler_state(key, scheduler_state, opts)
      when is_map(scheduler_state) and is_list(opts) do
    with {:ok, normalized_key} <- SchedulerStateCodec.normalize_key(key),
         {:ok, normalized_state} <- SchedulerStateCodec.normalize_state(scheduler_state) do
      server = Keyword.get(opts, :server, __MODULE__)
      GenServer.call(server, {:put_scheduler_state, normalized_key, normalized_state})
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
      server = Keyword.get(opts, :server, __MODULE__)
      GenServer.call(server, {:get_scheduler_state, normalized_key})
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
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:put_coverage_baseline, baseline})
  end

  @impl true
  def get_coverage_baseline(baseline_id, opts) when is_binary(baseline_id) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:get_coverage_baseline, baseline_id})
  end

  @impl true
  def list_coverage_baselines(filters, opts) when is_list(filters) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:list_coverage_baselines, filters})
  end

  @impl true
  def put_backfill_window(%BackfillWindow{} = window, opts) when is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:put_backfill_window, window})
  end

  @impl true
  def put_backfill_windows(windows, opts) when is_list(windows) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:put_backfill_windows, windows})
  end

  @impl true
  def get_backfill_window(backfill_run_id, pipeline_module, window_key, opts)
      when is_binary(backfill_run_id) and is_atom(pipeline_module) and is_binary(window_key) and
             is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:get_backfill_window, {backfill_run_id, pipeline_module, window_key}})
  end

  @impl true
  def list_backfill_windows(filters, opts) when is_list(filters) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:list_backfill_windows, filters})
  end

  @impl true
  def scan_backfill_windows(filters, scan_opts, opts)
      when is_list(filters) and is_list(scan_opts) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:scan_backfill_windows, filters, scan_opts})
  end

  @impl true
  def apply_backfill_child_projection(%BackfillWindow{} = window, asset_window_states, opts)
      when is_list(asset_window_states) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:apply_backfill_child_projection, window, asset_window_states})
  end

  @impl true
  def get_backfill_progress(backfill_run_id, opts)
      when is_binary(backfill_run_id) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:get_backfill_progress, backfill_run_id})
  end

  @impl true
  def rebuild_backfill_progress(backfill_run_id, opts)
      when is_binary(backfill_run_id) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:rebuild_backfill_progress, backfill_run_id})
  end

  @impl true
  def put_asset_window_state(%AssetWindowState{} = state, opts) when is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:put_asset_window_state, state})
  end

  @impl true
  def put_asset_window_states(states, opts) when is_list(states) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:put_asset_window_states, states})
  end

  @impl true
  def get_asset_window_state(asset_ref_module, asset_ref_name, window_key, opts)
      when is_atom(asset_ref_module) and is_atom(asset_ref_name) and is_binary(window_key) and
             is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)

    GenServer.call(
      server,
      {:get_asset_window_state, {asset_ref_module, asset_ref_name, window_key}}
    )
  end

  @impl true
  def list_asset_window_states(filters, opts) when is_list(filters) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:list_asset_window_states, filters})
  end

  @impl true
  def put_asset_freshness_state(%AssetFreshnessState{} = state, opts) when is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:put_asset_freshness_state, state})
  end

  @impl true
  def get_asset_freshness_state(asset_ref_module, asset_ref_name, freshness_key, opts)
      when is_atom(asset_ref_module) and is_atom(asset_ref_name) and is_binary(freshness_key) and
             is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)

    GenServer.call(
      server,
      {:get_asset_freshness_state, {asset_ref_module, asset_ref_name, freshness_key}}
    )
  end

  @impl true
  def list_asset_freshness_states(filters, opts) when is_list(filters) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:list_asset_freshness_states, filters})
  end

  @impl true
  def scan_asset_freshness_states(filters, scan_opts, opts)
      when is_list(filters) and is_list(scan_opts) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:scan_asset_freshness_states, filters, scan_opts})
  end

  @impl true
  def get_asset_freshness_states_by_keys(keys, opts) when is_list(keys) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:get_asset_freshness_states_by_keys, keys})
  end

  @impl true
  def upsert_target_status(%TargetStatus{} = status, opts) when is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:upsert_target_status, status})
  end

  @impl true
  def get_target_status(manifest_version_id, target_kind, target_id, opts)
      when is_binary(manifest_version_id) and target_kind in [:asset, :pipeline] and
             is_binary(target_id) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:get_target_status, {manifest_version_id, target_kind, target_id}})
  end

  @impl true
  def list_target_statuses(manifest_version_id, target_kind, target_ids, opts)
      when is_binary(manifest_version_id) and target_kind in [:asset, :pipeline] and
             is_list(target_ids) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:list_target_statuses, manifest_version_id, target_kind, target_ids})
  end

  @impl true
  def replace_target_statuses(scope, statuses, opts) when is_list(statuses) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:replace_target_statuses, scope, statuses})
  end

  @impl true
  def delete_target_statuses(scope, opts) when is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:delete_target_statuses, scope})
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
    server = Keyword.get(opts, :server, __MODULE__)

    GenServer.call(
      server,
      {:replace_backfill_read_models, scope, coverage_baselines, backfill_windows,
       asset_window_states}
    )
  end

  @impl true
  def put_auth_actor(actor, opts) when is_map(actor) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:put_auth_actor, actor})
  end

  @impl true
  def put_auth_actor_with_credential(actor, credential, opts)
      when is_map(actor) and is_map(credential) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:put_auth_actor_with_credential, actor, credential})
  end

  @impl true
  def get_auth_actor(actor_id, opts) when is_binary(actor_id) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:get_auth_actor, actor_id})
  end

  @impl true
  def get_auth_actor_by_username(username, opts) when is_binary(username) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:get_auth_actor_by_username, username})
  end

  @impl true
  def list_auth_actors(opts) when is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, :list_auth_actors)
  end

  @impl true
  def put_auth_credential(actor_id, credential, opts)
      when is_binary(actor_id) and is_map(credential) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:put_auth_credential, actor_id, credential})
  end

  @impl true
  def update_auth_actor_password(actor_id, actor, credential, revoked_at, opts)
      when is_binary(actor_id) and is_map(actor) and is_map(credential) and
             is_struct(revoked_at, DateTime) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:update_auth_actor_password, actor_id, actor, credential, revoked_at})
  end

  @impl true
  def get_auth_credential(actor_id, opts) when is_binary(actor_id) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:get_auth_credential, actor_id})
  end

  @impl true
  def put_auth_session(session, opts) when is_map(session) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:put_auth_session, session})
  end

  @impl true
  def get_auth_session(session_id, opts) when is_binary(session_id) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:get_auth_session, session_id})
  end

  @impl true
  def get_auth_session_by_token_hash(token_hash, opts)
      when is_binary(token_hash) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:get_auth_session_by_token_hash, token_hash})
  end

  @impl true
  def revoke_auth_session(session_id, revoked_at, opts)
      when is_binary(session_id) and is_struct(revoked_at, DateTime) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:revoke_auth_session, session_id, revoked_at})
  end

  @impl true
  def revoke_auth_sessions_for_actor(actor_id, revoked_at, opts)
      when is_binary(actor_id) and is_struct(revoked_at, DateTime) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:revoke_auth_sessions_for_actor, actor_id, revoked_at})
  end

  @impl true
  def put_auth_audit(entry, opts) when is_map(entry) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:put_auth_audit, entry})
  end

  @impl true
  def list_auth_audit(audit_opts, opts) when is_list(audit_opts) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:list_auth_audit, audit_opts})
  end

  @impl true
  def reserve_idempotency_record(record, opts) when is_map(record) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:reserve_idempotency_record, record})
  end

  @impl true
  def complete_idempotency_record(record_id, attrs, opts)
      when is_binary(record_id) and is_map(attrs) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:complete_idempotency_record, record_id, attrs})
  end

  @impl true
  def get_idempotency_record(record_id, opts) when is_binary(record_id) and is_list(opts) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:get_idempotency_record, record_id})
  end

  @impl true
  def init(_args) do
    {:ok, initial_state()}
  end

  defp initial_state do
    %{
      manifests: %{},
      active_manifest_version_id: nil,
      runs: %{},
      execution_group_summaries: %{},
      run_events: %{},
      run_event_global_sequence: 0,
      execution_leases: %{},
      execution_lease_ids_by_run: %{},
      execution_admission_waiters: %{},
      materialization_claims: %{},
      log_entries: [],
      log_global_sequence: 0,
      scheduler_states: %{},
      coverage_baselines: %{},
      backfill_windows: %{},
      backfill_progress: %{},
      asset_window_states: %{},
      asset_freshness_states: %{},
      target_statuses: %{},
      auth_actors: %{},
      auth_usernames: %{},
      auth_credentials: %{},
      auth_sessions: %{},
      auth_session_hashes: %{},
      auth_audits: [],
      idempotency_records: %{}
    }
  end

  defp manifest_content_hash_exists?(manifests, content_hash) do
    manifests
    |> Map.values()
    |> Enum.any?(&(&1.content_hash == content_hash))
  end

  @impl true
  def handle_call(:reset, _from, _state) do
    {:reply, :ok, initial_state()}
  end

  def handle_call({:put_manifest_version, %Version{} = version}, _from, state) do
    next_state =
      cond do
        manifest_content_hash_exists?(state.manifests, version.content_hash) ->
          state

        Map.has_key?(state.manifests, version.manifest_version_id) ->
          state

        true ->
          put_in(state, [:manifests, version.manifest_version_id], version)
      end

    reply =
      case Map.fetch(state.manifests, version.manifest_version_id) do
        {:ok, %Version{content_hash: hash}} when hash != version.content_hash ->
          {:error, :manifest_version_conflict}

        _ ->
          :ok
      end

    {:reply, reply, next_state}
  end

  def handle_call({:get_manifest_version, manifest_version_id}, _from, state) do
    reply =
      case Map.fetch(state.manifests, manifest_version_id) do
        {:ok, %Version{} = version} -> {:ok, version}
        :error -> {:error, :manifest_version_not_found}
      end

    {:reply, reply, state}
  end

  def handle_call({:get_manifest_version_by_content_hash, content_hash}, _from, state) do
    reply =
      state.manifests
      |> Map.values()
      |> Enum.find(&(&1.content_hash == content_hash))
      |> case do
        %Version{} = version -> {:ok, version}
        nil -> {:error, :manifest_version_not_found}
      end

    {:reply, reply, state}
  end

  def handle_call(:list_manifest_versions, _from, state) do
    versions =
      state.manifests
      |> Map.values()
      |> Enum.sort_by(& &1.manifest_version_id)

    {:reply, {:ok, versions}, state}
  end

  def handle_call({:set_active_manifest_version, manifest_version_id}, _from, state) do
    case Map.has_key?(state.manifests, manifest_version_id) do
      true ->
        {:reply, :ok, %{state | active_manifest_version_id: manifest_version_id}}

      false ->
        {:reply, {:error, :manifest_version_not_found}, state}
    end
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
    {reply, runs} = put_run_with_semantics(state.runs, incoming)

    normalized_reply = if reply == :idempotent, do: :ok, else: reply

    next_state = %{state | runs: runs} |> refresh_execution_group_summary(incoming)

    {:reply, normalized_reply, next_state}
  end

  def handle_call({:persist_run_transition, %RunState{} = run, event}, _from, state) do
    {run_reply, runs} = put_run_with_semantics(state.runs, run)

    case run_reply do
      run_write_result when run_write_result in [:ok, :idempotent] ->
        current = Map.get(state.run_events, run.id, [])

        case append_event_with_semantics(current, event, state.run_event_global_sequence) do
          {:ok, event_write_result, next_events, next_global_sequence} ->
            next_state =
              %{
                state
                | runs: runs,
                  run_events: Map.put(state.run_events, run.id, next_events),
                  run_event_global_sequence: next_global_sequence
              }
              |> refresh_execution_group_summary(run)

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
    reply =
      case Map.fetch(state.runs, run_id) do
        {:ok, %RunState{} = run} -> {:ok, run}
        :error -> {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call({:list_runs, run_opts}, _from, state) do
    runs =
      state.runs
      |> Map.values()
      |> filter_runs(run_opts)
      |> Enum.sort_by(&run_sort_key/1, :desc)
      |> maybe_limit_runs(run_opts)

    {:reply, {:ok, runs}, state}
  end

  def handle_call(
        {:list_target_runs, manifest_version_id, target_kind, target_ref, run_opts},
        _from,
        state
      ) do
    runs =
      state.runs
      |> Map.values()
      |> filter_runs(Keyword.put(run_opts, :manifest_version_id, manifest_version_id))
      |> Enum.filter(&target_run?(&1, target_kind, target_ref))
      |> Enum.sort_by(&run_sort_key/1, :desc)
      |> maybe_limit_runs(run_opts)

    {:reply, {:ok, runs}, state}
  end

  def handle_call({:list_execution_group_runs, group_id}, _from, state) do
    runs = execution_group_runs(state.runs, group_id)
    {:reply, {:ok, runs}, state}
  end

  def handle_call({:list_execution_group_run_ids, group_id}, _from, state) do
    run_ids =
      state.runs
      |> execution_group_runs(group_id)
      |> Enum.map(& &1.id)

    {:reply, {:ok, run_ids}, state}
  end

  def handle_call({:list_execution_groups, group_opts}, _from, state) do
    page_opts = page_opts(group_opts)

    page =
      state.runs
      |> execution_group_ids(group_opts)
      |> Enum.drop(Keyword.fetch!(page_opts, :offset))
      |> Enum.take(Keyword.fetch!(page_opts, :limit) + 1)
      |> Page.from_fetched(page_opts)

    {:reply, {:ok, page}, state}
  end

  def handle_call({:list_execution_group_summaries, group_opts}, _from, state) do
    page_opts = page_opts(group_opts)

    page =
      state.execution_group_summaries
      |> Map.values()
      |> filter_execution_group_summaries(group_opts)
      |> sort_execution_group_summaries(Keyword.get(group_opts, :sort, :started_desc))
      |> Enum.drop(Keyword.fetch!(page_opts, :offset))
      |> Enum.take(Keyword.fetch!(page_opts, :limit) + 1)
      |> Page.from_fetched(page_opts)

    {:reply, {:ok, page}, state}
  end

  def handle_call(:rebuild_execution_group_summaries, _from, state) do
    group_ids =
      state.runs
      |> Map.values()
      |> Enum.map(&RunQuery.root_execution_group_id/1)
      |> Enum.uniq()

    next_state = refresh_execution_group_summaries(state, group_ids)

    {:reply, {:ok, length(group_ids)}, next_state}
  end

  def handle_call({:append_run_event, run_id, event}, _from, state) do
    current = Map.get(state.run_events, run_id, [])

    case append_event_with_semantics(current, event, state.run_event_global_sequence) do
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
    reply =
      state.run_events
      |> Map.get(run_id, [])
      |> filter_run_events(run_event_opts)
      |> event_filter_reply()

    {:reply, reply, state}
  end

  def handle_call({:list_execution_group_events, group_id, run_event_opts}, _from, state) do
    run_ids = MapSet.new(Enum.map(execution_group_runs(state.runs, group_id), & &1.id))

    reply =
      state.run_events
      |> Map.values()
      |> List.flatten()
      |> Enum.filter(&(Map.get(&1, :run_id) in run_ids))
      |> Enum.sort_by(&event_sort_key/1)
      |> filter_execution_group_events(run_event_opts)
      |> event_filter_reply()

    {:reply, reply, state}
  end

  def handle_call({:list_global_run_events, opts}, _from, state) do
    after_sequence = Keyword.get(opts, :after_global_sequence)
    limit = Keyword.get(opts, :limit, 200)

    events =
      state.run_events
      |> Map.values()
      |> List.flatten()
      |> Enum.filter(
        &(is_integer(Map.get(&1, :global_sequence)) and Map.get(&1, :global_sequence) > 0)
      )
      |> Enum.sort_by(&Map.fetch!(&1, :global_sequence))

    reply =
      cond do
        not (is_integer(limit) and limit > 0) ->
          {:error, :cursor_invalid}

        is_nil(after_sequence) ->
          {:ok, events |> Enum.reverse() |> Enum.take(limit) |> Enum.reverse()}

        is_integer(after_sequence) and after_sequence == 0 ->
          {:ok, Enum.take(events, limit)}

        is_integer(after_sequence) and after_sequence > 0 ->
          if Enum.any?(events, &(Map.get(&1, :global_sequence) == after_sequence)) do
            {:ok,
             events
             |> Enum.filter(&(Map.get(&1, :global_sequence) > after_sequence))
             |> Enum.take(limit)}
          else
            {:error, :cursor_invalid}
          end

        true ->
          {:error, :cursor_invalid}
      end

    {:reply, reply, state}
  end

  def handle_call({:try_acquire_execution_lease, lease}, _from, state) do
    {expired_count, active_leases} =
      prune_execution_leases(state.execution_leases, lease.acquired_at)

    active_lease_ids_by_run = execution_lease_ids_by_run(active_leases)

    reply = execution_lease_capacity(active_leases, lease.scopes)

    case reply do
      :ok ->
        next_leases = Map.put(active_leases, lease.lease_id, lease)
        next_lease_ids_by_run = put_execution_lease_id(active_lease_ids_by_run, lease)

        {:reply, {:ok, lease},
         %{
           state
           | execution_leases: next_leases,
             execution_lease_ids_by_run: next_lease_ids_by_run
         }}

      {:error, reason} ->
        _ = expired_count

        {:reply, {:error, reason},
         %{
           state
           | execution_leases: active_leases,
             execution_lease_ids_by_run: active_lease_ids_by_run
         }}
    end
  end

  def handle_call({:release_execution_lease, lease_id}, _from, state) do
    {lease, execution_leases} = Map.pop(state.execution_leases, lease_id)

    execution_lease_ids_by_run =
      if lease do
        delete_execution_lease_id(state.execution_lease_ids_by_run, lease)
      else
        state.execution_lease_ids_by_run
      end

    {:reply, :ok,
     %{
       state
       | execution_leases: execution_leases,
         execution_lease_ids_by_run: execution_lease_ids_by_run
     }}
  end

  def handle_call({:release_execution_leases_for_run, run_id}, _from, state) do
    lease_ids = Map.get(state.execution_lease_ids_by_run, run_id, MapSet.new())

    {released, execution_leases} = pop_execution_leases(state.execution_leases, lease_ids)
    reply = lease_release(run_id, released)

    {:reply, {:ok, reply},
     %{
       state
       | execution_leases: execution_leases,
         execution_lease_ids_by_run: Map.delete(state.execution_lease_ids_by_run, run_id)
     }}
  end

  def handle_call({:expire_execution_leases, now}, _from, state) do
    {expired_count, active_leases} = prune_execution_leases(state.execution_leases, now)

    {:reply, {:ok, expired_count},
     %{
       state
       | execution_leases: active_leases,
         execution_lease_ids_by_run: execution_lease_ids_by_run(active_leases)
     }}
  end

  def handle_call(:list_execution_leases, _from, state) do
    leases = state.execution_leases |> Map.values() |> Enum.sort_by(& &1.lease_id)
    {:reply, {:ok, leases}, state}
  end

  def handle_call({:upsert_execution_admission_waiter, waiter}, _from, state) do
    next_waiter = next_execution_admission_waiter(waiter, state.execution_admission_waiters)

    {:reply, {:ok, next_waiter},
     %{
       state
       | execution_admission_waiters:
           Map.put(state.execution_admission_waiters, next_waiter.waiter_id, next_waiter)
     }}
  end

  def handle_call({:delete_execution_admission_waiter, waiter_id}, _from, state) do
    {:reply, :ok,
     %{
       state
       | execution_admission_waiters: Map.delete(state.execution_admission_waiters, waiter_id)
     }}
  end

  def handle_call({:delete_execution_admission_waiters_for_run, run_id}, _from, state) do
    {deleted, waiters} =
      pop_execution_admission_waiters_for_run(state.execution_admission_waiters, run_id)

    {:reply, {:ok, deleted}, %{state | execution_admission_waiters: waiters}}
  end

  def handle_call({:list_execution_admission_waiters_for_scope, scope, opts}, _from, state) do
    limit = waiter_limit(opts)
    identity = execution_scope_identity(scope)

    waiters =
      state.execution_admission_waiters
      |> Map.values()
      |> Enum.filter(&(execution_scope_identity(&1.blocked_scope) == identity))
      |> sort_execution_admission_waiters()
      |> Enum.take(limit)

    {:reply, {:ok, waiters}, state}
  end

  def handle_call({:expire_execution_admission_waiters, now}, _from, state) do
    {expired, active} = prune_execution_admission_waiters(state.execution_admission_waiters, now)
    {:reply, {:ok, expired}, %{state | execution_admission_waiters: active}}
  end

  def handle_call({:try_acquire_materialization_claim, claim}, _from, state) do
    {materialization_claims, _expired_count} =
      expire_materialization_claims_in_memory(state.materialization_claims, claim.claimed_at)

    existing = Map.get(materialization_claims, claim.claim_key)

    case materialization_claim_acquire_decision(existing, claim.claimed_at) do
      :insert ->
        next_claims = Map.put(materialization_claims, claim.claim_key, claim)
        {:reply, {:ok, claim}, %{state | materialization_claims: next_claims}}

      :reclaim ->
        next_claims = Map.put(materialization_claims, claim.claim_key, claim)
        {:reply, {:ok, claim}, %{state | materialization_claims: next_claims}}

      {:already_succeeded, existing} ->
        {:reply, {:already_succeeded, existing},
         %{state | materialization_claims: materialization_claims}}

      {:already_claimed, existing} ->
        {:reply, {:already_claimed, existing},
         %{state | materialization_claims: materialization_claims}}
    end
  end

  def handle_call({:complete_materialization_claim, claim_key, completion}, _from, state) do
    case Map.fetch(state.materialization_claims, claim_key) do
      {:ok, %MaterializationClaim{status: :claimed} = claim} ->
        completed = apply_materialization_completion(claim, completion)
        next_claims = Map.put(state.materialization_claims, claim_key, completed)
        {:reply, {:ok, completed}, %{state | materialization_claims: next_claims}}

      {:ok, _claim} ->
        {:reply, {:error, :not_found}, state}

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:fail_materialization_claim, claim_key, failure}, _from, state) do
    case Map.fetch(state.materialization_claims, claim_key) do
      {:ok, %MaterializationClaim{status: :claimed} = claim} ->
        failed = apply_materialization_failure(claim, failure)
        next_claims = Map.put(state.materialization_claims, claim_key, failed)
        {:reply, {:ok, failed}, %{state | materialization_claims: next_claims}}

      {:ok, _claim} ->
        {:reply, {:error, :not_found}, state}

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:expire_materialization_claims, now}, _from, state) do
    {claims, expired_count} =
      expire_materialization_claims_in_memory(state.materialization_claims, now)

    {:reply, {:ok, expired_count}, %{state | materialization_claims: claims}}
  end

  def handle_call({:get_materialization_claim, claim_key}, _from, state) do
    {:reply, fetch_or_not_found(state.materialization_claims, claim_key), state}
  end

  def handle_call({:list_materialization_claims, filters}, _from, state) do
    claims =
      state.materialization_claims
      |> Map.values()
      |> filter_by(filters)
      |> Enum.sort_by(& &1.claim_key)

    {:reply, {:ok, claims}, state}
  end

  def handle_call({:persist_log_entries, entries}, _from, state) do
    {persisted, next_entries, next_sequence} =
      Enum.reduce(entries, {[], log_entries(state), log_global_sequence(state)}, fn entry,
                                                                                    {persisted,
                                                                                     stored,
                                                                                     sequence} ->
        case find_idempotent_log_entry(stored, entry) do
          nil ->
            next_sequence = sequence + 1
            persisted_entry = LogEntryCodec.assign_global_sequence(entry, next_sequence)
            {[persisted_entry | persisted], stored ++ [persisted_entry], next_sequence}

          existing ->
            {[existing | persisted], stored, sequence}
        end
      end)

    {:reply, {:ok, Enum.reverse(persisted)},
     Map.merge(state, %{log_entries: next_entries, log_global_sequence: next_sequence})}
  end

  def handle_call({:list_logs, filter, opts}, _from, state) do
    page_opts = page_opts(opts)
    order = log_order(opts)

    rows =
      log_entries(state)
      |> filter_logs(filter)
      |> Enum.sort_by(&Map.get(&1, :global_sequence, 0), order)
      |> Enum.drop(Keyword.fetch!(page_opts, :offset))
      |> Enum.take(Keyword.fetch!(page_opts, :limit) + 1)

    {:reply, {:ok, Page.from_fetched(rows, page_opts)}, state}
  end

  def handle_call({:scan_logs, filter, scan_opts}, _from, state) do
    reply =
      with {:ok, after_sequence} <- log_cursor_sequence(Keyword.get(scan_opts, :after)) do
        rows =
          log_entries(state)
          |> filter_logs(filter)
          |> Enum.filter(&(Map.get(&1, :global_sequence, 0) > after_sequence))
          |> Enum.sort_by(&Map.get(&1, :global_sequence, 0))
          |> Enum.take(Keyword.fetch!(scan_opts, :limit) + 1)

        {:ok, CursorPage.from_fetched(rows, scan_opts, &log_entry_cursor!/1)}
      end

    {:reply, reply, state}
  end

  def handle_call({:replay_logs_after, cursor, filter, opts}, _from, state) do
    limit = Keyword.get(opts, :limit, 200)

    reply =
      with {:ok, after_sequence} <- log_cursor_sequence(cursor),
           :ok <- validate_replay_limit(limit) do
        rows =
          log_entries(state)
          |> filter_logs(filter)
          |> Enum.filter(&(Map.get(&1, :global_sequence, 0) > after_sequence))
          |> Enum.sort_by(&Map.get(&1, :global_sequence, 0))
          |> Enum.take(limit)

        {:ok, rows}
      end

    {:reply, reply, state}
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
          put_in(state, [:scheduler_states, key], scheduler_state)

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
    {:reply, :ok, put_in(state, [:coverage_baselines, baseline.baseline_id], baseline)}
  end

  def handle_call({:get_coverage_baseline, baseline_id}, _from, state) do
    {:reply, fetch_or_not_found(state.coverage_baselines, baseline_id), state}
  end

  def handle_call({:list_coverage_baselines, filters}, _from, state) do
    rows =
      state.coverage_baselines
      |> Map.values()
      |> filter_by(filters)
      |> Enum.sort_by(&{DateTime.to_unix(&1.updated_at, :microsecond) * -1, &1.baseline_id})
      |> offset_and_fetch(filters)

    {:reply, {:ok, Page.from_fetched(rows, page_opts(filters))}, state}
  end

  def handle_call({:put_backfill_window, %BackfillWindow{} = window}, _from, state) do
    key = {window.backfill_run_id, window.pipeline_module, window.window_key}

    next_state =
      state
      |> put_in([:backfill_windows, key], window)
      |> refresh_execution_group_summary(window.backfill_run_id)

    {:reply, :ok, next_state}
  end

  def handle_call({:put_backfill_windows, windows}, _from, state) do
    next_state =
      %{state | backfill_windows: put_backfill_window_values(state.backfill_windows, windows)}
      |> refresh_execution_group_summaries(Enum.map(windows, & &1.backfill_run_id))

    {:reply, :ok, next_state}
  end

  def handle_call({:get_backfill_window, key}, _from, state) do
    {:reply, fetch_or_not_found(state.backfill_windows, key), state}
  end

  def handle_call({:list_backfill_windows, filters}, _from, state) do
    rows =
      state.backfill_windows
      |> Map.values()
      |> filter_by(filters)
      |> Enum.sort_by(
        &{DateTime.to_unix(&1.window_start_at, :microsecond), &1.backfill_run_id,
         Atom.to_string(&1.pipeline_module), &1.window_key}
      )
      |> offset_and_fetch(filters)

    {:reply, {:ok, Page.from_fetched(rows, page_opts(filters))}, state}
  end

  def handle_call({:scan_backfill_windows, filters, scan_opts}, _from, state) do
    with :ok <- validate_filters(filters, @backfill_window_filters),
         {:ok, after_key} <- backfill_window_cursor(Keyword.get(scan_opts, :after)) do
      rows =
        state.backfill_windows
        |> Map.values()
        |> filter_by(filters)
        |> Enum.sort_by(&backfill_window_sort_key/1)
        |> cursor_drop(after_key, &backfill_window_sort_key/1)
        |> Enum.take(Keyword.fetch!(scan_opts, :limit) + 1)

      page = CursorPage.from_fetched(rows, scan_opts, &backfill_window_cursor!/1)
      {:reply, {:ok, page}, state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call(
        {:apply_backfill_child_projection, %BackfillWindow{} = window, asset_window_states},
        _from,
        state
      ) do
    key = {window.backfill_run_id, window.pipeline_module, window.window_key}
    old_status = state.backfill_windows[key] && state.backfill_windows[key].status

    next_state = %{
      state
      | backfill_windows: Map.put(state.backfill_windows, key, window),
        asset_window_states:
          put_asset_window_state_values(state.asset_window_states, asset_window_states)
    }

    case next_progress(next_state, window.backfill_run_id, old_status, window.status) do
      {:ok, progress} ->
        next_state = put_in(next_state, [:backfill_progress, window.backfill_run_id], progress)
        {:reply, {:ok, progress}, next_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:get_backfill_progress, backfill_run_id}, _from, state) do
    case Map.fetch(state.backfill_progress, backfill_run_id) do
      {:ok, %BackfillProgress{} = progress} ->
        {:reply, {:ok, progress}, state}

      :error ->
        case rebuild_progress_from_windows(state.backfill_windows, backfill_run_id) do
          {:ok, progress} ->
            {:reply, {:ok, progress},
             put_in(state, [:backfill_progress, backfill_run_id], progress)}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end
    end
  end

  def handle_call({:rebuild_backfill_progress, backfill_run_id}, _from, state) do
    case rebuild_progress_from_windows(state.backfill_windows, backfill_run_id) do
      {:ok, progress} ->
        {:reply, {:ok, progress}, put_in(state, [:backfill_progress, backfill_run_id], progress)}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:put_asset_window_state, %AssetWindowState{} = window_state}, _from, state) do
    key = {window_state.asset_ref_module, window_state.asset_ref_name, window_state.window_key}
    {:reply, :ok, put_in(state, [:asset_window_states, key], window_state)}
  end

  def handle_call({:put_asset_window_states, window_states}, _from, state) do
    {:reply, :ok,
     %{
       state
       | asset_window_states:
           put_asset_window_state_values(state.asset_window_states, window_states)
     }}
  end

  def handle_call({:get_asset_window_state, key}, _from, state) do
    {:reply, fetch_or_not_found(state.asset_window_states, key), state}
  end

  def handle_call({:list_asset_window_states, filters}, _from, state) do
    rows =
      state.asset_window_states
      |> Map.values()
      |> filter_by(filters)
      |> Enum.sort_by(
        &{DateTime.to_unix(&1.updated_at, :microsecond) * -1, Atom.to_string(&1.asset_ref_module),
         Atom.to_string(&1.asset_ref_name), &1.window_key}
      )
      |> offset_and_fetch(filters)

    {:reply, {:ok, Page.from_fetched(rows, page_opts(filters))}, state}
  end

  def handle_call(
        {:put_asset_freshness_state, %AssetFreshnessState{} = freshness_state},
        _from,
        state
      ) do
    key =
      {freshness_state.asset_ref_module, freshness_state.asset_ref_name,
       freshness_state.freshness_key}

    {:reply, :ok, put_in(state, [:asset_freshness_states, key], freshness_state)}
  end

  def handle_call({:get_asset_freshness_state, key}, _from, state) do
    {:reply, fetch_or_not_found(state.asset_freshness_states, key), state}
  end

  def handle_call({:list_asset_freshness_states, filters}, _from, state) do
    case validate_filters(filters, @asset_freshness_state_filters) do
      :ok ->
        rows =
          state.asset_freshness_states
          |> Map.values()
          |> filter_by(filters)
          |> Enum.sort_by(
            &{DateTime.to_unix(&1.updated_at, :microsecond) * -1,
             Atom.to_string(&1.asset_ref_module), Atom.to_string(&1.asset_ref_name),
             inspect(&1.freshness_key)}
          )
          |> offset_and_fetch(filters)

        {:reply, {:ok, Page.from_fetched(rows, page_opts(filters))}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:scan_asset_freshness_states, filters, scan_opts}, _from, state) do
    with :ok <- validate_filters(filters, @asset_freshness_state_filters),
         {:ok, after_key} <- asset_freshness_cursor(Keyword.get(scan_opts, :after)) do
      rows =
        state.asset_freshness_states
        |> Map.values()
        |> filter_by(filters)
        |> Enum.sort_by(&asset_freshness_sort_key/1)
        |> cursor_drop(after_key, &asset_freshness_sort_key/1)
        |> Enum.take(Keyword.fetch!(scan_opts, :limit) + 1)

      page = CursorPage.from_fetched(rows, scan_opts, &asset_freshness_cursor!/1)
      {:reply, {:ok, page}, state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:get_asset_freshness_states_by_keys, keys}, _from, state) do
    rows =
      keys
      |> Enum.reduce(%{}, fn key, acc ->
        case Map.fetch(state.asset_freshness_states, key) do
          {:ok, %AssetFreshnessState{} = freshness_state} -> Map.put(acc, key, freshness_state)
          :error -> acc
        end
      end)

    {:reply, {:ok, rows}, state}
  end

  def handle_call({:upsert_target_status, %TargetStatus{} = status}, _from, state) do
    key = target_status_key(status)
    {:reply, :ok, put_in(state, [:target_statuses, key], status)}
  end

  def handle_call({:get_target_status, key}, _from, state) do
    {:reply, fetch_or_not_found(state.target_statuses, key), state}
  end

  def handle_call(
        {:list_target_statuses, manifest_version_id, target_kind, target_ids},
        _from,
        state
      ) do
    target_ids = MapSet.new(target_ids)

    rows =
      state.target_statuses
      |> Map.values()
      |> Enum.filter(fn %TargetStatus{} = status ->
        status.manifest_version_id == manifest_version_id and status.target_kind == target_kind and
          MapSet.member?(target_ids, status.target_id)
      end)
      |> Map.new(&{&1.target_id, &1})

    {:reply, {:ok, rows}, state}
  end

  def handle_call({:replace_target_statuses, scope, statuses}, _from, state) do
    with {:ok, scope} <- target_status_scope(scope),
         :ok <- validate_target_status_scope_rows(scope, statuses) do
      target_statuses =
        state.target_statuses
        |> reject_target_status_scope(scope)
        |> put_target_status_values(statuses)

      {:reply, :ok, %{state | target_statuses: target_statuses}}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:delete_target_statuses, scope}, _from, state) do
    case target_status_scope(scope) do
      {:ok, scope} ->
        {:reply, :ok,
         %{state | target_statuses: reject_target_status_scope(state.target_statuses, scope)}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call(
        {:replace_backfill_read_models, scope, coverage_baselines, backfill_windows,
         asset_window_states},
        _from,
        state
      ) do
    case replacement_scope(scope) do
      {:ok, scope} ->
        next_backfill_windows =
          state.backfill_windows
          |> reject_replacement_scope(scope)
          |> put_backfill_window_values(backfill_windows)

        affected_backfill_ids =
          affected_backfill_ids(state.backfill_windows, scope, backfill_windows)

        next_state = %{
          state
          | coverage_baselines:
              state.coverage_baselines
              |> reject_replacement_scope(scope)
              |> put_coverage_baseline_values(coverage_baselines),
            backfill_windows: next_backfill_windows,
            backfill_progress:
              state.backfill_progress
              |> reject_replacement_progress(scope)
              |> rebuild_progress_for_ids(next_backfill_windows, affected_backfill_ids),
            asset_window_states:
              state.asset_window_states
              |> reject_replacement_scope(scope)
              |> put_asset_window_state_values(asset_window_states)
        }

        {:reply, :ok, next_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:put_auth_actor, actor}, _from, state) do
    next_state = %{
      state
      | auth_actors: Map.put(state.auth_actors, actor.id, actor),
        auth_usernames: Map.put(state.auth_usernames, actor.username, actor.id)
    }

    {:reply, :ok, next_state}
  end

  def handle_call({:put_auth_actor_with_credential, actor, credential}, _from, state) do
    next_state = %{
      state
      | auth_actors: Map.put(state.auth_actors, actor.id, actor),
        auth_usernames: Map.put(state.auth_usernames, actor.username, actor.id),
        auth_credentials: Map.put(state.auth_credentials, actor.id, credential)
    }

    {:reply, :ok, next_state}
  end

  def handle_call({:get_auth_actor, actor_id}, _from, state) do
    {:reply, fetch_or_not_found(state.auth_actors, actor_id), state}
  end

  def handle_call({:get_auth_actor_by_username, username}, _from, state) do
    reply =
      with {:ok, actor_id} <- fetch_or_not_found(state.auth_usernames, username) do
        fetch_or_not_found(state.auth_actors, actor_id)
      end

    {:reply, reply, state}
  end

  def handle_call(:list_auth_actors, _from, state) do
    actors = state.auth_actors |> Map.values() |> Enum.sort_by(& &1.username)
    {:reply, {:ok, actors}, state}
  end

  def handle_call({:put_auth_credential, actor_id, credential}, _from, state) do
    {:reply, :ok,
     %{state | auth_credentials: Map.put(state.auth_credentials, actor_id, credential)}}
  end

  def handle_call(
        {:update_auth_actor_password, actor_id, actor, credential, revoked_at},
        _from,
        state
      ) do
    sessions =
      Map.new(state.auth_sessions, fn {session_id, session} ->
        if session.actor_id == actor_id and is_nil(session.revoked_at) do
          {session_id, %{session | revoked_at: revoked_at}}
        else
          {session_id, session}
        end
      end)

    next_state = %{
      state
      | auth_actors: Map.put(state.auth_actors, actor_id, actor),
        auth_usernames: Map.put(state.auth_usernames, actor.username, actor_id),
        auth_credentials: Map.put(state.auth_credentials, actor_id, credential),
        auth_sessions: sessions
    }

    {:reply, :ok, next_state}
  end

  def handle_call({:get_auth_credential, actor_id}, _from, state) do
    {:reply, fetch_or_not_found(state.auth_credentials, actor_id), state}
  end

  def handle_call({:put_auth_session, session}, _from, state) do
    next_state = %{
      state
      | auth_sessions: Map.put(state.auth_sessions, session.id, session),
        auth_session_hashes: Map.put(state.auth_session_hashes, session.token_hash, session.id)
    }

    {:reply, :ok, next_state}
  end

  def handle_call({:get_auth_session, session_id}, _from, state) do
    {:reply, fetch_or_not_found(state.auth_sessions, session_id), state}
  end

  def handle_call({:get_auth_session_by_token_hash, token_hash}, _from, state) do
    reply =
      with {:ok, session_id} <- fetch_or_not_found(state.auth_session_hashes, token_hash) do
        fetch_or_not_found(state.auth_sessions, session_id)
      end

    {:reply, reply, state}
  end

  def handle_call({:revoke_auth_session, session_id, revoked_at}, _from, state) do
    case Map.fetch(state.auth_sessions, session_id) do
      {:ok, session} ->
        sessions =
          Map.put(
            state.auth_sessions,
            session_id,
            if(is_nil(session.revoked_at), do: %{session | revoked_at: revoked_at}, else: session)
          )

        {:reply, :ok, %{state | auth_sessions: sessions}}

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:revoke_auth_sessions_for_actor, actor_id, revoked_at}, _from, state) do
    sessions =
      Map.new(state.auth_sessions, fn {session_id, session} ->
        if session.actor_id == actor_id and is_nil(session.revoked_at) do
          {session_id, %{session | revoked_at: revoked_at}}
        else
          {session_id, session}
        end
      end)

    {:reply, :ok, %{state | auth_sessions: sessions}}
  end

  def handle_call({:put_auth_audit, entry}, _from, state) do
    {:reply, :ok, %{state | auth_audits: [entry | state.auth_audits]}}
  end

  def handle_call({:list_auth_audit, opts}, _from, state) do
    limit = opts |> Keyword.get(:limit, 100) |> max(1) |> min(500)
    {:reply, {:ok, state.auth_audits |> Enum.take(limit) |> Enum.reverse()}, state}
  end

  def handle_call({:reserve_idempotency_record, record}, _from, state) do
    case Map.fetch(state.idempotency_records, record.id) do
      :error ->
        stored = normalize_idempotency_record(record)

        {:reply, {:ok, {:reserved, stored}},
         %{state | idempotency_records: Map.put(state.idempotency_records, stored.id, stored)}}

      {:ok, stored} ->
        if expired_idempotency_record?(stored) do
          replacement = normalize_idempotency_record(record)

          {:reply, {:ok, {:reserved, replacement}},
           %{
             state
             | idempotency_records:
                 Map.put(state.idempotency_records, replacement.id, replacement)
           }}
        else
          {:reply, classify_idempotency_record(stored, record.request_fingerprint), state}
        end
    end
  end

  def handle_call({:complete_idempotency_record, record_id, attrs}, _from, state) do
    case Map.fetch(state.idempotency_records, record_id) do
      {:ok, stored} ->
        updated = stored |> Map.merge(attrs) |> normalize_idempotency_record()

        {:reply, :ok,
         %{state | idempotency_records: Map.put(state.idempotency_records, record_id, updated)}}

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:get_idempotency_record, record_id}, _from, state) do
    {:reply, fetch_or_not_found(state.idempotency_records, record_id), state}
  end

  defp put_run_with_semantics(runs, %RunState{} = incoming) do
    case Map.fetch(runs, incoming.id) do
      :error ->
        {{:ok, :ok}, Map.put(runs, incoming.id, incoming)}

      {:ok, %RunState{} = existing} ->
        case WriteSemantics.decide(
               existing.event_seq,
               existing.snapshot_hash,
               incoming.event_seq,
               incoming.snapshot_hash
             ) do
          :replace -> {{:ok, :ok}, Map.put(runs, incoming.id, incoming)}
          :idempotent -> {{:ok, :idempotent}, runs}
          {:error, reason} -> {{:error, reason}, runs}
        end
    end
    |> normalize_put_run_reply()
  end

  defp normalize_put_run_reply({{:ok, result}, runs}) when result in [:ok, :idempotent],
    do: {result, runs}

  defp normalize_put_run_reply({{:error, reason}, runs}), do: {{:error, reason}, runs}

  defp filter_runs(runs, run_opts) do
    Enum.filter(runs, fn run ->
      matches_run_filter?(run, :status, Keyword.get(run_opts, :status)) and
        matches_run_filter?(
          run,
          :manifest_version_id,
          Keyword.get(run_opts, :manifest_version_id)
        )
    end)
  end

  defp target_run?(%RunState{} = run, :asset, target_ref) do
    target_ref_text = RunQuery.public_ref(target_ref)

    run
    |> RunQuery.target_refs()
    |> Enum.map(&RunQuery.public_ref/1)
    |> Enum.member?(target_ref_text)
  end

  defp target_run?(%RunState{} = run, :pipeline, target_ref) do
    pipeline_submit_ref_text(run) == RunQuery.public_ref(target_ref)
  end

  defp pipeline_submit_ref_text(%RunState{} = run) do
    metadata = run.metadata || %{}

    case Map.get(metadata, :pipeline_submit_ref, Map.get(metadata, "pipeline_submit_ref")) do
      value when is_atom(value) or is_binary(value) -> RunQuery.public_ref(value)
      _other -> nil
    end
  end

  defp execution_group_runs(runs, group_id) when is_map(runs) do
    runs
    |> Map.values()
    |> Enum.filter(&(RunQuery.root_execution_group_id(&1) == group_id))
    |> Enum.sort_by(&execution_group_run_sort_key/1)
  end

  defp execution_group_ids(runs, group_opts) when is_map(runs) do
    runs
    |> Map.values()
    |> Enum.group_by(&RunQuery.root_execution_group_id/1)
    |> Enum.map(fn {group_id, group_runs} ->
      root =
        Enum.find(group_runs, &(&1.id == group_id)) || Enum.min_by(group_runs, &run_sort_key/1)

      %{
        id: group_id,
        root: root,
        runs: group_runs,
        activity: Enum.map(group_runs, &run_sort_key/1) |> Enum.max(fn -> 0 end)
      }
    end)
    |> Enum.filter(&matches_execution_group_filters?(&1, group_opts))
    |> sort_execution_groups(Keyword.get(group_opts, :sort, :started_desc))
    |> Enum.map(& &1.id)
  end

  defp refresh_execution_group_summary(state, %RunState{} = run) do
    refresh_execution_group_summary(state, RunQuery.root_execution_group_id(run))
  end

  defp refresh_execution_group_summary(state, group_id) when is_binary(group_id) do
    runs = execution_group_runs(state.runs, group_id)
    windows = execution_group_windows(state.backfill_windows, group_id)

    case ExecutionGroupSummary.build(runs, windows) do
      {:ok, summary} ->
        put_in(state, [:execution_group_summaries, group_id], summary)

      {:error, :empty_execution_group} ->
        update_in(state, [:execution_group_summaries], &Map.delete(&1, group_id))
    end
  end

  defp refresh_execution_group_summaries(state, group_ids) do
    group_ids
    |> Enum.uniq()
    |> Enum.reduce(state, &refresh_execution_group_summary(&2, &1))
  end

  defp execution_group_windows(windows, group_id) when is_map(windows) do
    windows
    |> Map.values()
    |> Enum.filter(&(&1.backfill_run_id == group_id))
  end

  defp filter_execution_group_summaries(summaries, group_opts) do
    Enum.filter(summaries, fn summary ->
      matches_summary_status?(summary, Keyword.get(group_opts, :status)) and
        matches_summary_trigger?(summary, Keyword.get(group_opts, :trigger_type)) and
        matches_summary_target?(summary, Keyword.get(group_opts, :target_asset)) and
        matches_summary_search?(summary, Keyword.get(group_opts, :search)) and
        matches_summary_window?(summary, Keyword.get(group_opts, :window)) and
        matches_summary_only_filters?(summary, group_opts)
    end)
  end

  defp sort_execution_group_summaries(summaries, :failed_first),
    do:
      Enum.sort_by(summaries, &{if(&1.failure_count > 0, do: 0, else: 1), -summary_activity(&1)})

  defp sort_execution_group_summaries(summaries, :running_first),
    do: Enum.sort_by(summaries, &{if(&1.active?, do: 0, else: 1), -summary_activity(&1)})

  defp sort_execution_group_summaries(summaries, :status_priority),
    do: Enum.sort_by(summaries, &{summary_status_priority(&1), -summary_activity(&1)})

  defp sort_execution_group_summaries(summaries, _sort),
    do: Enum.sort_by(summaries, &summary_activity/1, :desc)

  defp matches_summary_status?(_summary, nil), do: true
  defp matches_summary_status?(summary, status), do: summary.root_status == status

  defp matches_summary_trigger?(_summary, nil), do: true
  defp matches_summary_trigger?(summary, trigger), do: summary.trigger_type == trigger

  defp matches_summary_target?(_summary, nil), do: true
  defp matches_summary_target?(summary, target), do: target in summary.target_assets

  defp matches_summary_search?(_summary, value) when value in [nil, ""], do: true

  defp matches_summary_search?(summary, search) do
    search = String.downcase(to_string(search))

    [summary.id, summary.trigger_type | summary.target_assets]
    |> Enum.reject(&is_nil/1)
    |> Enum.join(" ")
    |> String.downcase()
    |> String.contains?(search)
  end

  defp matches_summary_window?(_summary, nil), do: true
  defp matches_summary_window?(summary, :has_window), do: summary.total_windows > 0
  defp matches_summary_window?(summary, :no_window), do: summary.total_windows == 0
  defp matches_summary_window?(_summary, _window), do: true

  defp matches_summary_only_filters?(summary, opts) do
    (not Keyword.get(opts, :only_failed, false) or summary.failure_count > 0) and
      (not Keyword.get(opts, :only_running, false) or summary.active?) and
      (not Keyword.get(opts, :only_incomplete, false) or summary.active?)
  end

  defp summary_activity(summary), do: datetime_sort_value(summary.last_activity_at)

  defp summary_status_priority(summary) do
    cond do
      summary.failure_count > 0 -> 0
      summary.active? -> 1
      true -> 2
    end
  end

  defp datetime_sort_value(%DateTime{} = datetime), do: DateTime.to_unix(datetime, :microsecond)
  defp datetime_sort_value(_datetime), do: 0

  defp matches_execution_group_filters?(group, opts) do
    matches_execution_group_status?(group, Keyword.get(opts, :status)) and
      matches_execution_group_trigger?(group, Keyword.get(opts, :trigger_type)) and
      matches_execution_group_target?(group, Keyword.get(opts, :target_asset)) and
      matches_execution_group_search?(group, Keyword.get(opts, :search)) and
      matches_execution_group_window?(group, Keyword.get(opts, :window)) and
      matches_execution_group_only_filters?(group, opts)
  end

  defp matches_execution_group_status?(_group, nil), do: true
  defp matches_execution_group_status?(%{root: root}, status), do: root.status == status

  defp matches_execution_group_trigger?(_group, nil), do: true

  defp matches_execution_group_trigger?(%{root: root}, trigger),
    do: RunQuery.trigger_type(root) == trigger

  defp matches_execution_group_target?(_group, nil), do: true

  defp matches_execution_group_target?(%{root: root}, target) do
    root
    |> RunQuery.target_refs()
    |> Enum.map(&RunQuery.public_ref/1)
    |> Enum.member?(target)
  end

  defp matches_execution_group_search?(_group, value) when value in [nil, ""], do: true

  defp matches_execution_group_search?(%{id: id, root: root}, search) do
    search = String.downcase(to_string(search))
    metadata = RunQuery.metadata(root)

    [
      id,
      metadata.trigger_type,
      metadata.asset_ref_text,
      metadata.target_refs_text,
      metadata.window_key
    ]
    |> Enum.reject(&is_nil/1)
    |> Enum.join(" ")
    |> String.downcase()
    |> String.contains?(search)
  end

  defp matches_execution_group_window?(_group, nil), do: true
  defp matches_execution_group_window?(group, :has_window), do: execution_group_has_window?(group)

  defp matches_execution_group_window?(group, :no_window),
    do: not execution_group_has_window?(group)

  defp matches_execution_group_window?(_group, _window), do: true

  defp matches_execution_group_only_filters?(group, opts) do
    (not Keyword.get(opts, :only_failed, false) or failed_execution_group?(group)) and
      (not Keyword.get(opts, :only_running, false) or running_execution_group?(group)) and
      (not Keyword.get(opts, :only_incomplete, false) or running_execution_group?(group))
  end

  defp sort_execution_groups(groups, :failed_first),
    do: Enum.sort_by(groups, &{if(failed_execution_group?(&1), do: 0, else: 1), -&1.activity})

  defp sort_execution_groups(groups, :running_first),
    do: Enum.sort_by(groups, &{if(running_execution_group?(&1), do: 0, else: 1), -&1.activity})

  defp sort_execution_groups(groups, :status_priority),
    do: Enum.sort_by(groups, &{execution_group_status_priority(&1), -&1.activity})

  defp sort_execution_groups(groups, _sort), do: Enum.sort_by(groups, & &1.activity, :desc)

  defp execution_group_run_sort_key(%RunState{id: id} = run) do
    case RunQuery.root_execution_group_id(run) do
      ^id -> {0, id}
      _other -> {1, id}
    end
  end

  defp execution_group_has_window?(%{runs: runs}) do
    Enum.any?(runs, fn run -> RunQuery.metadata(run).window_key not in [nil, ""] end)
  end

  defp failed_execution_group?(%{root: root, runs: runs}) do
    root.status in [:error, :partial, :cancelled, :timed_out] or
      Enum.any?(runs, &(&1.status in [:error, :partial, :cancelled, :timed_out]))
  end

  defp running_execution_group?(%{root: root, runs: runs}) do
    root.status in [:pending, :running] or Enum.any?(runs, &(&1.status in [:pending, :running]))
  end

  defp execution_group_status_priority(group) do
    cond do
      failed_execution_group?(group) -> 0
      running_execution_group?(group) -> 1
      true -> 2
    end
  end

  defp filter_run_events(events, opts) do
    with :ok <- validate_event_order(opts) do
      events
      |> Enum.filter(fn event ->
        case Keyword.get(opts, :after_sequence) do
          sequence when is_integer(sequence) and sequence >= 0 ->
            Map.get(event, :sequence) > sequence

          _other ->
            true
        end
      end)
      |> order_events(opts)
      |> maybe_limit_events(opts)
    end
  end

  defp filter_execution_group_events(events, opts) do
    with :ok <- validate_event_order(opts) do
      events
      |> Enum.filter(fn event ->
        case Keyword.get(opts, :after_global_sequence) do
          sequence when is_integer(sequence) and sequence >= 0 ->
            Map.get(event, :global_sequence, 0) > sequence

          _other ->
            true
        end
      end)
      |> order_events(opts)
      |> maybe_limit_events(opts)
    end
  end

  defp validate_event_order(opts) do
    case Keyword.get(opts, :order, :asc) do
      order when order in [:asc, :desc] -> :ok
      _order -> {:error, :invalid_opts}
    end
  end

  defp order_events(events, opts) do
    case Keyword.get(opts, :order, :asc) do
      :desc -> Enum.reverse(events)
      _order -> events
    end
  end

  defp event_filter_reply({:error, _reason} = error), do: error
  defp event_filter_reply(events) when is_list(events), do: {:ok, events}

  defp maybe_limit_events(events, opts) do
    case Keyword.get(opts, :limit) do
      limit when is_integer(limit) and limit > 0 -> Enum.take(events, limit)
      _other -> events
    end
  end

  defp event_sort_key(event) do
    {Map.get(event, :global_sequence) || 0, Map.get(event, :run_id) || "",
     Map.get(event, :sequence) || 0}
  end

  defp matches_run_filter?(_run, _field, nil), do: true
  defp matches_run_filter?(run, field, expected), do: Map.get(run, field) == expected

  defp run_sort_key(%RunState{updated_at: %DateTime{} = updated_at}),
    do: DateTime.to_unix(updated_at, :microsecond)

  defp run_sort_key(%RunState{}), do: 0

  defp fetch_or_not_found(values, key) do
    case Map.fetch(values, key) do
      {:ok, value} -> {:ok, value}
      :error -> {:error, :not_found}
    end
  end

  defp classify_idempotency_record(stored, request_fingerprint) do
    cond do
      stored.request_fingerprint != request_fingerprint ->
        {:error, :idempotency_conflict}

      stored.status == :in_progress ->
        {:error, :operation_in_progress}

      stored.status in [:completed, :failed] ->
        {:ok, {:replay, stored}}

      true ->
        {:error, {:invalid_idempotency_status, stored.status}}
    end
  end

  defp normalize_idempotency_record(record) when is_map(record) do
    record
    |> atomize_known_keys()
    |> Map.update!(:status, &normalize_status/1)
  end

  defp atomize_known_keys(record) do
    Enum.reduce(record, %{}, fn {key, value}, acc ->
      Map.put(acc, atomize_idempotency_key(key), value)
    end)
  end

  defp atomize_idempotency_key(key) when is_atom(key), do: key
  defp atomize_idempotency_key(key) when is_binary(key), do: String.to_existing_atom(key)

  defp normalize_status(status) when is_atom(status), do: status
  defp normalize_status(status) when is_binary(status), do: String.to_existing_atom(status)

  defp expired_idempotency_record?(%{expires_at: %DateTime{} = expires_at}) do
    DateTime.compare(expires_at, DateTime.utc_now()) != :gt
  end

  defp expired_idempotency_record?(_record), do: false

  defp filter_by(values, filters) do
    filters = Keyword.drop(filters, [:limit, :offset])

    Enum.filter(values, fn value ->
      Enum.all?(filters, fn {key, expected} -> Map.get(value, key) == expected end)
    end)
  end

  defp validate_filters(filters, allowed_keys) do
    filters
    |> Keyword.drop([:limit, :offset])
    |> Enum.find_value(:ok, fn {key, _value} ->
      if key in allowed_keys, do: false, else: {:error, {:unsupported_filter, key}}
    end)
  end

  defp normalize_log_entries(entries) do
    Enum.reduce_while(entries, {:ok, []}, fn entry, {:ok, acc} ->
      case LogEntryCodec.normalize(entry) do
        {:ok, normalized} -> {:cont, {:ok, [normalized | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, normalized} -> {:ok, Enum.reverse(normalized)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp log_entries(state), do: Map.get(state, :log_entries, [])

  defp log_order(opts) do
    case Keyword.get(opts, :order, :asc) do
      :desc -> :desc
      "desc" -> :desc
      _other -> :asc
    end
  end

  defp log_global_sequence(state), do: Map.get(state, :log_global_sequence, 0)

  defp find_idempotent_log_entry(entries, entry) do
    producer_id = Map.get(entry, :producer_id)
    producer_sequence = Map.get(entry, :producer_sequence)

    if is_binary(producer_id) and is_integer(producer_sequence) do
      Enum.find(entries, fn existing ->
        Map.get(existing, :producer_id) == producer_id and
          Map.get(existing, :producer_sequence) == producer_sequence
      end)
    end
  end

  defp filter_logs(entries, filter) do
    filters = normalize_log_filter(filter)

    Enum.filter(entries, fn entry ->
      Enum.all?(filters, fn
        {:after_global_sequence, sequence} ->
          Map.get(entry, :global_sequence, 0) > sequence

        {:levels, []} ->
          true

        {:levels, levels} when is_list(levels) ->
          Map.get(entry, :level) in levels

        {:sources, []} ->
          true

        {:sources, sources} when is_list(sources) ->
          Map.get(entry, :source) in sources

        {:since, %DateTime{} = since} ->
          DateTime.compare(Map.get(entry, :occurred_at), since) != :lt

        {:until, %DateTime{} = until} ->
          DateTime.compare(Map.get(entry, :occurred_at), until) != :gt

        {:asset_ref, expected} ->
          Map.get(entry, :asset_ref) == expected

        {:node_key, expected} ->
          Map.get(entry, :node_key) == expected

        {key, expected} ->
          Map.get(entry, key) == expected
      end)
    end)
  end

  defp normalize_log_filter(filter) when is_list(filter),
    do: Keyword.drop(filter, [:limit, :offset])

  defp normalize_log_filter(%_{} = filter),
    do: filter |> Map.from_struct() |> normalize_log_filter()

  defp normalize_log_filter(filter) when is_map(filter) do
    filter
    |> Enum.map(fn {key, value} -> {normalize_filter_key(key), value} end)
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
  end

  defp normalize_log_filter(_filter), do: []

  defp normalize_filter_key(key) when is_atom(key), do: key

  defp normalize_filter_key(key) when is_binary(key) do
    Enum.find(@log_filter_keys, key, &(Atom.to_string(&1) == key)) || key
  end

  defp log_cursor_sequence(nil), do: {:ok, 0}
  defp log_cursor_sequence(value) when is_integer(value) and value >= 0, do: {:ok, value}

  defp log_cursor_sequence(value) when is_binary(value) do
    case Integer.parse(value) do
      {sequence, ""} when sequence >= 0 ->
        {:ok, sequence}

      _ ->
        case Favn.Log.Cursor.parse(value) do
          {:ok, cursor} -> log_cursor_sequence(cursor)
          {:error, _reason} -> {:error, :cursor_invalid}
        end
    end
  end

  defp log_cursor_sequence(%_{} = cursor),
    do: cursor |> Map.from_struct() |> log_cursor_sequence()

  defp log_cursor_sequence(%{} = cursor) do
    cursor
    |> Map.get(
      :global_sequence,
      Map.get(cursor, :after_global_sequence, Map.get(cursor, "global_sequence"))
    )
    |> log_cursor_sequence()
  end

  defp log_cursor_sequence(_cursor), do: {:error, :cursor_invalid}

  defp log_entry_cursor!(%Favn.Log.Entry{} = entry) do
    %{kind: :log_entry, global_sequence: entry.global_sequence}
  end

  defp validate_replay_limit(limit) when is_integer(limit) and limit > 0, do: :ok
  defp validate_replay_limit(_limit), do: {:error, :cursor_invalid}

  defp replacement_scope(:all), do: {:ok, :all}
  defp replacement_scope({:backfill_run, id}) when is_binary(id), do: {:ok, {:backfill_run, id}}
  defp replacement_scope({:pipeline, module}) when is_atom(module), do: {:ok, {:pipeline, module}}

  defp replacement_scope(scope),
    do: {:error, {:unsupported_replacement_scope, scope}}

  defp target_status_scope({:manifest_version, manifest_version_id})
       when is_binary(manifest_version_id),
       do: {:ok, {:manifest_version, manifest_version_id}}

  defp target_status_scope({:manifest_version, manifest_version_id, target_kind})
       when is_binary(manifest_version_id) and target_kind in [:asset, :pipeline],
       do: {:ok, {:manifest_version, manifest_version_id, target_kind}}

  defp target_status_scope(scope), do: {:error, {:unsupported_target_status_scope, scope}}

  defp validate_target_status_scope_rows(scope, statuses) do
    if Enum.all?(statuses, &target_status_in_scope?(&1, scope)) do
      :ok
    else
      {:error, :target_status_scope_mismatch}
    end
  end

  defp reject_target_status_scope(values, scope) when is_map(values) do
    values
    |> Enum.reject(fn {_key, status} -> target_status_in_scope?(status, scope) end)
    |> Map.new()
  end

  defp target_status_in_scope?(%TargetStatus{manifest_version_id: id}, {:manifest_version, id}),
    do: true

  defp target_status_in_scope?(
         %TargetStatus{manifest_version_id: id, target_kind: kind},
         {:manifest_version, id, kind}
       ),
       do: true

  defp target_status_in_scope?(_status, _scope), do: false

  defp reject_replacement_scope(_values, :all), do: %{}

  defp reject_replacement_scope(values, scope) when is_map(values) do
    values
    |> Enum.reject(fn {_key, value} -> in_replacement_scope?(value, scope) end)
    |> Map.new()
  end

  defp reject_replacement_progress(_values, :all), do: %{}

  defp reject_replacement_progress(values, {:backfill_run, backfill_run_id}),
    do: Map.delete(values, backfill_run_id)

  defp reject_replacement_progress(values, {:pipeline, _module}), do: values

  defp in_replacement_scope?(_value, :all), do: true

  defp in_replacement_scope?(%CoverageBaseline{created_by_run_id: id}, {:backfill_run, id}),
    do: true

  defp in_replacement_scope?(%BackfillWindow{backfill_run_id: id}, {:backfill_run, id}),
    do: true

  defp in_replacement_scope?(%AssetWindowState{latest_parent_run_id: id}, {:backfill_run, id}),
    do: true

  defp in_replacement_scope?(%{pipeline_module: module}, {:pipeline, module}), do: true
  defp in_replacement_scope?(_value, _scope), do: false

  defp affected_backfill_ids(backfill_windows, :all, replacement_windows) do
    backfill_windows
    |> Map.values()
    |> Enum.map(& &1.backfill_run_id)
    |> Kernel.++(Enum.map(replacement_windows, & &1.backfill_run_id))
    |> Enum.uniq()
  end

  defp affected_backfill_ids(_backfill_windows, {:backfill_run, id}, replacement_windows) do
    [id | Enum.map(replacement_windows, & &1.backfill_run_id)]
    |> Enum.uniq()
  end

  defp affected_backfill_ids(backfill_windows, {:pipeline, module}, replacement_windows) do
    deleted_ids =
      backfill_windows
      |> Map.values()
      |> Enum.filter(&(&1.pipeline_module == module))
      |> Enum.map(& &1.backfill_run_id)

    (deleted_ids ++ Enum.map(replacement_windows, & &1.backfill_run_id))
    |> Enum.uniq()
  end

  defp put_coverage_baseline_values(values, baselines) do
    Enum.reduce(baselines, values, fn %CoverageBaseline{} = baseline, acc ->
      Map.put(acc, baseline.baseline_id, baseline)
    end)
  end

  defp put_backfill_window_values(values, windows) do
    Enum.reduce(windows, values, fn %BackfillWindow{} = window, acc ->
      Map.put(acc, {window.backfill_run_id, window.pipeline_module, window.window_key}, window)
    end)
  end

  defp put_asset_window_state_values(values, states) do
    Enum.reduce(states, values, fn %AssetWindowState{} = window_state, acc ->
      Map.put(
        acc,
        {window_state.asset_ref_module, window_state.asset_ref_name, window_state.window_key},
        window_state
      )
    end)
  end

  defp put_target_status_values(values, statuses) do
    Enum.reduce(statuses, values, fn %TargetStatus{} = status, acc ->
      Map.put(acc, target_status_key(status), status)
    end)
  end

  defp target_status_key(%TargetStatus{} = status) do
    {status.manifest_version_id, status.target_kind, status.target_id}
  end

  defp next_progress(state, backfill_run_id, old_status, new_status) do
    case Map.fetch(state.backfill_progress, backfill_run_id) do
      {:ok, %BackfillProgress{} = progress} ->
        case BackfillProgress.apply_status_change(
               progress,
               old_status,
               new_status,
               DateTime.utc_now()
             ) do
          {:ok, %BackfillProgress{} = next_progress} ->
            {:ok, next_progress}

          {:error, {:stale_backfill_progress, _old_status, _new_status, _counts}} ->
            rebuild_progress_from_windows(state.backfill_windows, backfill_run_id)

          {:error, reason} ->
            {:error, reason}
        end

      :error ->
        rebuild_progress_from_windows(state.backfill_windows, backfill_run_id)
    end
  end

  defp rebuild_progress_from_windows(backfill_windows, backfill_run_id) do
    windows =
      backfill_windows
      |> Map.values()
      |> Enum.filter(&(&1.backfill_run_id == backfill_run_id))

    case windows do
      [] -> {:error, :not_found}
      windows -> BackfillProgress.from_windows(backfill_run_id, windows, DateTime.utc_now())
    end
  end

  defp rebuild_progress_for_ids(progress, backfill_windows, ids) do
    Enum.reduce(ids, progress, fn backfill_run_id, acc ->
      case rebuild_progress_from_windows(backfill_windows, backfill_run_id) do
        {:ok, %BackfillProgress{} = rebuilt} -> Map.put(acc, backfill_run_id, rebuilt)
        {:error, :not_found} -> Map.delete(acc, backfill_run_id)
        {:error, _reason} -> acc
      end
    end)
  end

  defp cursor_drop(rows, nil, _sort_fun), do: rows

  defp cursor_drop(rows, after_key, sort_fun) do
    Enum.drop_while(rows, &(sort_fun.(&1) <= after_key))
  end

  defp backfill_window_cursor(nil), do: {:ok, nil}

  defp backfill_window_cursor(%{
         kind: :backfill_window,
         window_start_at: %DateTime{} = window_start_at,
         backfill_run_id: backfill_run_id,
         pipeline_module: pipeline_module,
         window_key: window_key
       })
       when is_binary(backfill_run_id) and is_atom(pipeline_module) and is_binary(window_key),
       do:
         {:ok,
          backfill_window_sort_key(window_start_at, backfill_run_id, pipeline_module, window_key)}

  defp backfill_window_cursor(_cursor), do: {:error, :invalid_cursor_pagination}

  defp backfill_window_cursor!(%BackfillWindow{} = window) do
    %{
      kind: :backfill_window,
      window_start_at: window.window_start_at,
      backfill_run_id: window.backfill_run_id,
      pipeline_module: window.pipeline_module,
      window_key: window.window_key
    }
  end

  defp backfill_window_sort_key(%BackfillWindow{} = window) do
    backfill_window_sort_key(
      window.window_start_at,
      window.backfill_run_id,
      window.pipeline_module,
      window.window_key
    )
  end

  defp backfill_window_sort_key(
         %DateTime{} = window_start_at,
         backfill_run_id,
         pipeline_module,
         window_key
       ) do
    {DateTime.to_unix(window_start_at, :microsecond), backfill_run_id,
     Atom.to_string(pipeline_module), window_key}
  end

  defp asset_freshness_cursor(nil), do: {:ok, nil}

  defp asset_freshness_cursor(%{
         kind: :asset_freshness_state,
         updated_at: %DateTime{} = updated_at,
         asset_ref_module: asset_ref_module,
         asset_ref_name: asset_ref_name,
         freshness_key: freshness_key
       })
       when is_atom(asset_ref_module) and is_atom(asset_ref_name) and is_binary(freshness_key),
       do:
         {:ok,
          asset_freshness_sort_key(updated_at, asset_ref_module, asset_ref_name, freshness_key)}

  defp asset_freshness_cursor(_cursor), do: {:error, :invalid_cursor_pagination}

  defp asset_freshness_cursor!(%AssetFreshnessState{} = state) do
    %{
      kind: :asset_freshness_state,
      updated_at: state.updated_at,
      asset_ref_module: state.asset_ref_module,
      asset_ref_name: state.asset_ref_name,
      freshness_key: state.freshness_key
    }
  end

  defp asset_freshness_sort_key(%AssetFreshnessState{} = state) do
    asset_freshness_sort_key(
      state.updated_at,
      state.asset_ref_module,
      state.asset_ref_name,
      state.freshness_key
    )
  end

  defp asset_freshness_sort_key(
         %DateTime{} = updated_at,
         asset_ref_module,
         asset_ref_name,
         freshness_key
       ) do
    {DateTime.to_unix(updated_at, :microsecond) * -1, Atom.to_string(asset_ref_module),
     Atom.to_string(asset_ref_name), freshness_key}
  end

  defp offset_and_fetch(values, filters) do
    opts = page_opts(filters)

    values
    |> Enum.drop(Keyword.fetch!(opts, :offset))
    |> Enum.take(Keyword.fetch!(opts, :limit) + 1)
  end

  defp page_opts(filters) do
    {:ok, opts} = Page.normalize_opts(filters)
    opts
  end

  defp maybe_limit_runs(runs, run_opts) do
    case Keyword.get(run_opts, :limit) do
      limit when is_integer(limit) and limit > 0 -> Enum.take(runs, limit)
      _ -> runs
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

  defp append_event_with_semantics(current_events, event, current_global_sequence)
       when is_list(current_events) and is_integer(current_global_sequence) do
    sequence = Map.get(event, :sequence)

    existing = Enum.find(current_events, &(Map.get(&1, :sequence) == sequence))

    case WriteSemantics.decide_run_event_append(existing, event) do
      :insert ->
        next_global_sequence = current_global_sequence + 1
        event = Map.put(event, :global_sequence, next_global_sequence)

        {:ok, :ok, Enum.sort_by(current_events ++ [event], &Map.get(&1, :sequence, 0)),
         next_global_sequence}

      :idempotent ->
        {:ok, :idempotent, current_events, current_global_sequence}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp normalize_execution_lease(lease) do
    with {:ok, lease_id} <- fetch_string_field(lease, :lease_id),
         {:ok, run_id} <- fetch_string_field(lease, :run_id),
         {:ok, asset_step_id} <- fetch_string_field(lease, :asset_step_id),
         {:ok, scopes} <- normalize_execution_lease_scopes(field_value(lease, :scopes)),
         {:ok, acquired_at} <- fetch_datetime_field(lease, :acquired_at),
         {:ok, expires_at} <- fetch_datetime_field(lease, :expires_at) do
      {:ok,
       %{
         lease_id: lease_id,
         run_id: run_id,
         asset_step_id: asset_step_id,
         scopes: scopes,
         acquired_at: acquired_at,
         expires_at: expires_at
       }}
    end
  end

  defp normalize_execution_lease_scopes(scopes) when is_list(scopes) and scopes != [] do
    scopes
    |> Enum.reduce_while({:ok, []}, fn scope, {:ok, acc} ->
      case normalize_execution_lease_scope(scope) do
        {:ok, normalized} -> {:cont, {:ok, [normalized | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, normalized} -> {:ok, Enum.reverse(normalized)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp normalize_execution_lease_scopes(_scopes), do: {:error, :invalid_execution_lease_scopes}

  defp normalize_execution_lease_scope(scope) when is_map(scope) do
    with {:ok, kind} <- fetch_atom_or_string_field(scope, :kind),
         {:ok, key} <- fetch_string_field(scope, :key),
         {:ok, limit} <- fetch_positive_integer_field(scope, :limit) do
      {:ok, %{kind: kind, key: key, limit: limit}}
    end
  end

  defp normalize_execution_lease_scope(_scope), do: {:error, :invalid_execution_lease_scope}

  defp prune_execution_leases(leases, %DateTime{} = now) do
    Enum.reduce(leases, {0, %{}}, fn {lease_id, lease}, {expired_count, active} ->
      if DateTime.compare(lease.expires_at, now) == :gt do
        {expired_count, Map.put(active, lease_id, lease)}
      else
        {expired_count + 1, active}
      end
    end)
  end

  defp execution_lease_ids_by_run(leases) do
    Enum.reduce(leases, %{}, fn {_lease_id, lease}, index ->
      put_execution_lease_id(index, lease)
    end)
  end

  defp put_execution_lease_id(index, lease) do
    Map.update(index, lease.run_id, MapSet.new([lease.lease_id]), &MapSet.put(&1, lease.lease_id))
  end

  defp delete_execution_lease_id(index, lease) do
    Map.update(index, lease.run_id, MapSet.new(), fn lease_ids ->
      MapSet.delete(lease_ids, lease.lease_id)
    end)
    |> drop_empty_execution_lease_run(lease.run_id)
  end

  defp drop_empty_execution_lease_run(index, run_id) do
    case Map.fetch(index, run_id) do
      {:ok, lease_ids} ->
        if MapSet.size(lease_ids) == 0, do: Map.delete(index, run_id), else: index

      _other ->
        index
    end
  end

  defp pop_execution_leases(leases, lease_ids) do
    Enum.reduce(lease_ids, {[], leases}, fn lease_id, {released, active} ->
      case Map.pop(active, lease_id) do
        {nil, next_active} -> {released, next_active}
        {lease, next_active} -> {[lease | released], next_active}
      end
    end)
  end

  defp lease_release(run_id, leases) do
    scopes =
      leases
      |> Enum.flat_map(& &1.scopes)
      |> Enum.uniq_by(&ExecutionLeaseCodec.scope_identity/1)

    LeaseRelease.new(run_id, length(leases), scopes)
  end

  defp next_execution_admission_waiter(waiter, waiters) do
    case Map.get(waiters, waiter.waiter_id) do
      nil ->
        waiter

      existing ->
        %{
          waiter
          | inserted_at: existing.inserted_at,
            wake_generation: existing.wake_generation + 1
        }
    end
  end

  defp pop_execution_admission_waiters_for_run(waiters, run_id) do
    Enum.reduce(waiters, {0, %{}}, fn {waiter_id, waiter}, {deleted, active} ->
      if waiter.run_id == run_id do
        {deleted + 1, active}
      else
        {deleted, Map.put(active, waiter_id, waiter)}
      end
    end)
  end

  defp prune_execution_admission_waiters(waiters, %DateTime{} = now) do
    Enum.reduce(waiters, {0, %{}}, fn {waiter_id, waiter}, {expired_count, active} ->
      if is_nil(waiter.deadline_at) or DateTime.compare(waiter.deadline_at, now) == :gt do
        {expired_count, Map.put(active, waiter_id, waiter)}
      else
        {expired_count + 1, active}
      end
    end)
  end

  defp waiter_limit(opts) do
    case Keyword.get(opts, :limit, 50) do
      limit when is_integer(limit) and limit > 0 -> limit
      _other -> 50
    end
  end

  defp sort_execution_admission_waiters(waiters) do
    Enum.sort(waiters, fn left, right ->
      case DateTime.compare(left.inserted_at, right.inserted_at) do
        :lt -> true
        :gt -> false
        :eq -> left.waiter_id <= right.waiter_id
      end
    end)
  end

  defp execution_lease_capacity(leases, scopes) do
    Enum.find_value(scopes, :ok, fn scope ->
      active_count = count_execution_scope(leases, scope)

      if active_count >= scope.limit do
        {:error, {:execution_capacity_exceeded, scope}}
      end
    end)
  end

  defp count_execution_scope(leases, scope) do
    identity = execution_scope_identity(scope)

    leases
    |> Map.values()
    |> Enum.count(fn lease ->
      Enum.any?(lease.scopes, &(execution_scope_identity(&1) == identity))
    end)
  end

  defp execution_scope_identity(scope), do: {to_string(scope.kind), scope.key}

  defp materialization_claim_acquire_decision(nil, %DateTime{}), do: :insert

  defp materialization_claim_acquire_decision(
         %MaterializationClaim{status: :succeeded} = claim,
         %DateTime{}
       ),
       do: {:already_succeeded, claim}

  defp materialization_claim_acquire_decision(
         %MaterializationClaim{status: :claimed} = claim,
         %DateTime{} = now
       ) do
    if MaterializationClaim.active?(claim, now), do: {:already_claimed, claim}, else: :reclaim
  end

  defp materialization_claim_acquire_decision(%MaterializationClaim{}, %DateTime{}), do: :reclaim

  defp expire_materialization_claims_in_memory(claims, %DateTime{} = now) do
    Enum.reduce(claims, {%{}, 0}, fn {claim_key, claim}, {acc, count} ->
      if claim.status == :claimed and DateTime.compare(claim.expires_at, now) != :gt do
        expired = %{claim | status: :expired, finished_at: now}
        {Map.put(acc, claim_key, expired), count + 1}
      else
        {Map.put(acc, claim_key, claim), count}
      end
    end)
  end

  defp apply_materialization_completion(%MaterializationClaim{} = claim, completion) do
    %{
      claim
      | status: :succeeded,
        freshness_version: field_value(completion, :freshness_version) || claim.freshness_version,
        finished_at: field_value(completion, :finished_at) || DateTime.utc_now(),
        metadata: field_value(completion, :metadata) || claim.metadata,
        error: field_value(completion, :error)
    }
  end

  defp apply_materialization_failure(%MaterializationClaim{} = claim, failure) do
    status = normalize_materialization_failure_status(field_value(failure, :status) || :failed)

    status =
      if status in MaterializationClaim.terminal_failure_statuses() do
        status
      else
        :failed
      end

    %{
      claim
      | status: status,
        error: field_value(failure, :error),
        finished_at: field_value(failure, :finished_at) || DateTime.utc_now(),
        metadata: field_value(failure, :metadata) || claim.metadata
    }
  end

  defp normalize_materialization_failure_status(status) when is_atom(status), do: status

  defp normalize_materialization_failure_status(status) when is_binary(status) do
    Enum.find(
      MaterializationClaim.terminal_failure_statuses(),
      :failed,
      &(Atom.to_string(&1) == status)
    )
  end

  defp fetch_string_field(map, field) do
    case field_value(map, field) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _other -> {:error, {:invalid_execution_lease_field, field}}
    end
  end

  defp fetch_atom_or_string_field(map, field) do
    case field_value(map, field) do
      value when is_atom(value) and not is_nil(value) -> {:ok, value}
      value when is_binary(value) and value != "" -> {:ok, value}
      _other -> {:error, {:invalid_execution_lease_field, field}}
    end
  end

  defp fetch_positive_integer_field(map, field) do
    case field_value(map, field) do
      value when is_integer(value) and value > 0 -> {:ok, value}
      _other -> {:error, {:invalid_execution_lease_field, field}}
    end
  end

  defp fetch_datetime_field(map, field) do
    case field_value(map, field) do
      %DateTime{} = value -> {:ok, value}
      _other -> {:error, {:invalid_execution_lease_field, field}}
    end
  end

  defp field_value(map, field), do: Map.get(map, field) || Map.get(map, Atom.to_string(field))

  defp runtime_name(opts) do
    Keyword.get(opts, :server, Keyword.get(opts, :name, __MODULE__))
  end

  defp runtime_started?(name) when is_atom(name), do: Process.whereis(name) != nil
  defp runtime_started?(_name), do: false
end
