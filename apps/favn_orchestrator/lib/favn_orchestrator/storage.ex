defmodule FavnOrchestrator.Storage do
  @moduledoc """
  Storage facade for orchestrator control-plane state.

  This facade stores authoritative run snapshots/events and normalized derived
  read models used by operational backfills: coverage baselines, backfill-window
  ledger rows, and latest asset/window state. Public run reads should continue
  to go through `FavnOrchestrator`; storage calls are for runtime internals and
  adapter implementations.
  """

  alias Favn.Manifest.Version
  alias Favn.Storage.Adapter, as: StorageAdapter
  alias FavnOrchestrator.AssetFreshnessState
  alias FavnOrchestrator.Backfill.AssetWindowState
  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.Backfill.CoverageBaseline
  alias FavnOrchestrator.Backfill.Progress, as: BackfillProgress
  alias FavnOrchestrator.ExecutionAdmission.LeaseRelease
  alias FavnOrchestrator.ExecutionAdmission.Waiter, as: AdmissionWaiter
  alias FavnOrchestrator.CursorPage
  alias FavnOrchestrator.MaterializationClaim
  alias FavnOrchestrator.Page
  alias FavnOrchestrator.RuntimeConfig
  alias FavnOrchestrator.RunExecutionOwnership
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.TargetStatus

  @type freshness_state_key :: StorageAdapter.freshness_state_key()
  @type read_model_replacement_scope :: StorageAdapter.read_model_replacement_scope()
  @type target_status_scope :: StorageAdapter.target_status_scope()

  @spec child_specs() :: {:ok, [Supervisor.child_spec()]} | {:error, term()}
  @spec child_specs(RuntimeConfig.t()) :: {:ok, [Supervisor.child_spec()]} | {:error, term()}
  def child_specs(runtime_config \\ RuntimeConfig.current())

  def child_specs(%RuntimeConfig{} = runtime_config) do
    adapter = runtime_config.storage_adapter

    with :ok <- validate_adapter(adapter),
         child_spec_result <- adapter.child_spec(runtime_config.storage_adapter_opts),
         {:ok, child_spec} <- normalize_child_spec_result(child_spec_result) do
      {:ok, maybe_child_to_list(child_spec)}
    end
  end

  @spec readiness() :: {:ok, map()} | {:error, term()}
  def readiness do
    adapter_call(fn adapter, opts ->
      if function_exported?(adapter, :readiness, 1) do
        adapter.readiness(opts)
      else
        if function_exported?(adapter, :diagnostics, 1) do
          adapter.diagnostics(opts)
        else
          {:ok, unsupported_readiness(adapter)}
        end
      end
    end)
  end

  @spec diagnostics() :: {:ok, map()} | {:error, term()}
  def diagnostics do
    adapter_call(fn adapter, opts ->
      cond do
        function_exported?(adapter, :diagnostics, 1) ->
          adapter.diagnostics(opts)

        function_exported?(adapter, :readiness, 1) ->
          adapter.readiness(opts)

        true ->
          {:ok, unsupported_readiness(adapter)}
      end
    end)
  end

  defp unsupported_readiness(adapter) do
    %{
      status: :ready,
      ready?: true,
      adapter: adapter
    }
  end

  @spec put_manifest_version(Version.t()) :: :ok | {:error, term()}
  def put_manifest_version(%Version{} = version) do
    adapter_call(fn adapter, opts -> adapter.put_manifest_version(version, opts) end)
  end

  @spec get_manifest_version(String.t()) :: {:ok, Version.t()} | {:error, term()}
  def get_manifest_version(manifest_version_id) when is_binary(manifest_version_id) do
    adapter_call(fn adapter, opts -> adapter.get_manifest_version(manifest_version_id, opts) end)
  end

  @spec get_manifest_version_by_content_hash(String.t()) :: {:ok, Version.t()} | {:error, term()}
  def get_manifest_version_by_content_hash(content_hash) when is_binary(content_hash) do
    adapter_call(fn adapter, opts ->
      adapter.get_manifest_version_by_content_hash(content_hash, opts)
    end)
  end

  @spec list_manifest_versions() :: {:ok, [Version.t()]} | {:error, term()}
  def list_manifest_versions do
    adapter_call(fn adapter, opts -> adapter.list_manifest_versions(opts) end)
  end

  @spec set_active_manifest_version(String.t()) :: :ok | {:error, term()}
  def set_active_manifest_version(manifest_version_id) when is_binary(manifest_version_id) do
    adapter_call(fn adapter, opts ->
      adapter.set_active_manifest_version(manifest_version_id, opts)
    end)
  end

  @spec get_active_manifest_version() :: {:ok, String.t()} | {:error, term()}
  def get_active_manifest_version do
    adapter_call(fn adapter, opts -> adapter.get_active_manifest_version(opts) end)
  end

  @spec put_run(RunState.t()) :: :ok | {:error, term()}
  def put_run(%RunState{} = run) do
    adapter_call(fn adapter, opts -> adapter.put_run(run, opts) end)
  end

  @spec persist_run_transition(RunState.t(), map()) :: :ok | :idempotent | {:error, term()}
  def persist_run_transition(%RunState{} = run, event) when is_map(event) do
    adapter_call(fn adapter, opts -> adapter.persist_run_transition(run, event, opts) end)
  end

  @spec put_execution_ownership(RunExecutionOwnership.t() | map()) :: :ok | {:error, term()}
  def put_execution_ownership(ownership) do
    adapter_call(fn adapter, opts -> adapter.put_execution_ownership(ownership, opts) end)
  end

  @spec get_execution_ownership(String.t()) :: {:ok, RunExecutionOwnership.t()} | {:error, term()}
  def get_execution_ownership(ownership_id) when is_binary(ownership_id) do
    adapter_call(fn adapter, opts -> adapter.get_execution_ownership(ownership_id, opts) end)
  end

  @spec list_execution_ownerships(String.t()) ::
          {:ok, [RunExecutionOwnership.t()]} | {:error, term()}
  def list_execution_ownerships(run_id) when is_binary(run_id) do
    adapter_call(fn adapter, opts -> adapter.list_execution_ownerships(run_id, opts) end)
  end

  @spec list_active_execution_ownerships(String.t()) ::
          {:ok, [RunExecutionOwnership.t()]} | {:error, term()}
  def list_active_execution_ownerships(run_id) when is_binary(run_id) do
    adapter_call(fn adapter, opts -> adapter.list_active_execution_ownerships(run_id, opts) end)
  end

  @spec get_run(String.t()) :: {:ok, RunState.t()} | {:error, term()}
  def get_run(run_id) when is_binary(run_id) do
    adapter_call(fn adapter, opts -> adapter.get_run(run_id, opts) end)
  end

  @spec list_runs(keyword()) :: {:ok, [RunState.t()]} | {:error, term()}
  def list_runs(run_opts \\ []) when is_list(run_opts) do
    adapter_call(fn adapter, opts -> adapter.list_runs(run_opts, opts) end)
  end

  @spec list_target_runs(
          String.t(),
          TargetStatus.target_kind(),
          Favn.Ref.t() | module(),
          keyword()
        ) ::
          {:ok, [RunState.t()]} | {:error, term()}
  def list_target_runs(manifest_version_id, target_kind, target_ref, run_opts \\ [])
      when is_binary(manifest_version_id) and target_kind in [:asset, :pipeline] and
             is_list(run_opts) do
    adapter_call(fn adapter, opts ->
      adapter.list_target_runs(manifest_version_id, target_kind, target_ref, run_opts, opts)
    end)
  end

  @spec list_execution_group_runs(String.t()) :: {:ok, [RunState.t()]} | {:error, term()}
  def list_execution_group_runs(group_id) when is_binary(group_id) do
    optional_adapter_call(
      :list_execution_group_runs,
      [group_id],
      :execution_group_reads_not_supported
    )
  end

  @spec list_execution_group_run_ids(String.t()) :: {:ok, [String.t()]} | {:error, term()}
  def list_execution_group_run_ids(group_id) when is_binary(group_id) do
    optional_adapter_call(
      :list_execution_group_run_ids,
      [group_id],
      :execution_group_reads_not_supported
    )
  end

  @spec list_execution_groups(keyword()) :: {:ok, Page.t(String.t())} | {:error, term()}
  def list_execution_groups(group_opts \\ []) when is_list(group_opts) do
    paginated_adapter_call(group_opts, fn adapter, filters, opts ->
      if function_exported?(adapter, :list_execution_groups, 2) do
        adapter.list_execution_groups(filters, opts)
      else
        {:error, :execution_group_reads_not_supported}
      end
    end)
  end

  @spec list_execution_group_summaries(keyword()) :: {:ok, Page.t(map())} | {:error, term()}
  def list_execution_group_summaries(group_opts \\ []) when is_list(group_opts) do
    paginated_adapter_call(group_opts, fn adapter, filters, opts ->
      if function_exported?(adapter, :list_execution_group_summaries, 2) do
        adapter.list_execution_group_summaries(filters, opts)
      else
        {:error, :execution_group_summary_reads_not_supported}
      end
    end)
  end

  @spec rebuild_execution_group_summaries() :: {:ok, non_neg_integer()} | {:error, term()}
  def rebuild_execution_group_summaries do
    adapter_call(fn adapter, opts ->
      if function_exported?(adapter, :rebuild_execution_group_summaries, 1) do
        adapter.rebuild_execution_group_summaries(opts)
      else
        {:error, :execution_group_summary_reads_not_supported}
      end
    end)
  end

  @spec append_run_event(String.t(), map()) :: :ok | {:error, term()}
  def append_run_event(run_id, event) when is_binary(run_id) and is_map(event) do
    adapter_call(fn adapter, opts -> adapter.append_run_event(run_id, event, opts) end)
  end

  @spec list_run_events(String.t(), keyword()) :: {:ok, [map()]} | {:error, term()}
  def list_run_events(run_id, run_event_opts \\ [])
      when is_binary(run_id) and is_list(run_event_opts) do
    adapter_call(fn adapter, opts ->
      if function_exported?(adapter, :list_run_events, 3) do
        adapter.list_run_events(run_id, run_event_opts, opts)
      else
        with {:ok, events} <- adapter.list_run_events(run_id, opts) do
          {:ok, filter_run_events(events, run_event_opts)}
        end
      end
    end)
  end

  @spec list_execution_group_events(String.t(), keyword()) :: {:ok, [map()]} | {:error, term()}
  def list_execution_group_events(group_id, run_event_opts \\ [])
      when is_binary(group_id) and is_list(run_event_opts) do
    optional_adapter_call(
      :list_execution_group_events,
      [group_id, run_event_opts],
      :execution_group_reads_not_supported
    )
  end

  @spec list_global_run_events(keyword()) :: {:ok, [map()]} | {:error, term()}
  def list_global_run_events(run_event_opts \\ []) when is_list(run_event_opts) do
    adapter_call(fn adapter, opts -> adapter.list_global_run_events(run_event_opts, opts) end)
  end

  @spec try_acquire_execution_lease(map()) ::
          {:ok, map()} | {:error, {:execution_capacity_exceeded, map()} | term()}
  def try_acquire_execution_lease(lease) when is_map(lease) do
    adapter_call(fn adapter, opts -> adapter.try_acquire_execution_lease(lease, opts) end)
  end

  @spec release_execution_lease(String.t()) :: :ok | {:error, term()}
  def release_execution_lease(lease_id) when is_binary(lease_id) do
    adapter_call(fn adapter, opts -> adapter.release_execution_lease(lease_id, opts) end)
  end

  @spec release_execution_leases_for_run(String.t()) :: {:ok, LeaseRelease.t()} | {:error, term()}
  def release_execution_leases_for_run(run_id) when is_binary(run_id) do
    adapter_call(fn adapter, opts -> adapter.release_execution_leases_for_run(run_id, opts) end)
  end

  @spec expire_execution_leases(DateTime.t()) :: {:ok, non_neg_integer()} | {:error, term()}
  def expire_execution_leases(%DateTime{} = now) do
    adapter_call(fn adapter, opts -> adapter.expire_execution_leases(now, opts) end)
  end

  @spec list_execution_leases() :: {:ok, [map()]} | {:error, term()}
  def list_execution_leases do
    adapter_call(fn adapter, opts -> adapter.list_execution_leases(opts) end)
  end

  @spec upsert_execution_admission_waiter(map() | AdmissionWaiter.t()) ::
          {:ok, AdmissionWaiter.t()} | {:error, term()}
  def upsert_execution_admission_waiter(waiter) when is_map(waiter) do
    adapter_call(fn adapter, opts -> adapter.upsert_execution_admission_waiter(waiter, opts) end)
  end

  @spec delete_execution_admission_waiter(String.t()) :: :ok | {:error, term()}
  def delete_execution_admission_waiter(waiter_id) when is_binary(waiter_id) do
    adapter_call(fn adapter, opts ->
      adapter.delete_execution_admission_waiter(waiter_id, opts)
    end)
  end

  @spec delete_execution_admission_waiters_for_run(String.t()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def delete_execution_admission_waiters_for_run(run_id) when is_binary(run_id) do
    adapter_call(fn adapter, opts ->
      adapter.delete_execution_admission_waiters_for_run(run_id, opts)
    end)
  end

  @spec list_execution_admission_waiters_for_scope(map(), keyword()) ::
          {:ok, [AdmissionWaiter.t()]} | {:error, term()}
  def list_execution_admission_waiters_for_scope(scope, waiter_opts \\ [])
      when is_map(scope) and is_list(waiter_opts) do
    adapter_call(fn adapter, opts ->
      adapter.list_execution_admission_waiters_for_scope(scope, waiter_opts, opts)
    end)
  end

  @spec expire_execution_admission_waiters(DateTime.t()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def expire_execution_admission_waiters(%DateTime{} = now) do
    adapter_call(fn adapter, opts -> adapter.expire_execution_admission_waiters(now, opts) end)
  end

  @spec try_acquire_materialization_claim(MaterializationClaim.t() | map()) ::
          {:ok, MaterializationClaim.t()}
          | {:already_succeeded, MaterializationClaim.t()}
          | {:already_claimed, MaterializationClaim.t()}
          | {:error, term()}
  def try_acquire_materialization_claim(claim) when is_map(claim) do
    optional_adapter_call(
      :try_acquire_materialization_claim,
      [claim],
      :materialization_claims_not_supported
    )
  end

  @spec complete_materialization_claim(String.t(), map()) ::
          {:ok, MaterializationClaim.t()} | {:error, term()}
  def complete_materialization_claim(claim_key, completion)
      when is_binary(claim_key) and is_map(completion) do
    optional_adapter_call(
      :complete_materialization_claim,
      [claim_key, completion],
      :materialization_claims_not_supported
    )
  end

  @spec fail_materialization_claim(String.t(), map()) ::
          {:ok, MaterializationClaim.t()} | {:error, term()}
  def fail_materialization_claim(claim_key, failure)
      when is_binary(claim_key) and is_map(failure) do
    optional_adapter_call(
      :fail_materialization_claim,
      [claim_key, failure],
      :materialization_claims_not_supported
    )
  end

  @spec expire_materialization_claims(DateTime.t()) :: {:ok, non_neg_integer()} | {:error, term()}
  def expire_materialization_claims(%DateTime{} = now) do
    optional_adapter_call(
      :expire_materialization_claims,
      [now],
      :materialization_claims_not_supported
    )
  end

  @spec get_materialization_claim(String.t()) ::
          {:ok, MaterializationClaim.t()} | {:error, term()}
  def get_materialization_claim(claim_key) when is_binary(claim_key) do
    optional_adapter_call(
      :get_materialization_claim,
      [claim_key],
      :materialization_claims_not_supported
    )
  end

  @spec list_materialization_claims(keyword()) ::
          {:ok, [MaterializationClaim.t()]} | {:error, term()}
  def list_materialization_claims(filters \\ []) when is_list(filters) do
    optional_adapter_call(
      :list_materialization_claims,
      [filters],
      :materialization_claims_not_supported
    )
  end

  @spec persist_log_entries([Favn.Log.Entry.t()]) ::
          {:ok, [Favn.Log.Entry.t()]} | {:error, term()}
  def persist_log_entries(entries) when is_list(entries) do
    with {:ok, redacted_entries} <- redact_log_entries(entries) do
      adapter_call(fn adapter, opts -> adapter.persist_log_entries(redacted_entries, opts) end)
    end
  end

  @spec list_logs(Favn.Log.Filter.t() | map() | keyword(), keyword()) ::
          {:ok, Page.t(Favn.Log.Entry.t())} | {:error, term()}
  def list_logs(filter \\ [], opts \\ []) when is_list(opts) do
    with {:ok, page_opts} <- Page.normalize_opts(opts) do
      adapter_call(fn adapter, adapter_opts ->
        adapter.list_logs(filter, Keyword.merge(opts, page_opts), adapter_opts)
      end)
    end
  end

  @spec scan_logs(Favn.Log.Filter.t() | map() | keyword(), keyword()) ::
          {:ok, CursorPage.t(Favn.Log.Entry.t())} | {:error, term()}
  def scan_logs(filter \\ [], scan_opts \\ []) when is_list(scan_opts) do
    with {:ok, normalized_opts} <- CursorPage.normalize_opts(scan_opts) do
      adapter_call(fn adapter, adapter_opts ->
        if function_exported?(adapter, :scan_logs, 3) do
          adapter.scan_logs(filter, normalized_opts, adapter_opts)
        else
          {:error, :log_cursor_reads_not_supported}
        end
      end)
    end
  end

  @spec replay_logs_after(
          Favn.Log.Cursor.t() | String.t() | nil,
          Favn.Log.Filter.t() | map() | keyword(),
          keyword()
        ) :: {:ok, [Favn.Log.Entry.t()]} | {:error, term()}
  def replay_logs_after(cursor, filter \\ [], opts \\ []) when is_list(opts) do
    adapter_call(fn adapter, adapter_opts ->
      adapter.replay_logs_after(cursor, filter, opts, adapter_opts)
    end)
  end

  @spec put_scheduler_state({module(), atom() | nil}, map()) :: :ok | {:error, term()}
  def put_scheduler_state({module, schedule_id} = key, state)
      when is_atom(module) and is_map(state) do
    _ = schedule_id
    adapter_call(fn adapter, opts -> adapter.put_scheduler_state(key, state, opts) end)
  end

  @spec get_scheduler_state({module(), atom() | nil}) :: {:ok, map() | nil} | {:error, term()}
  def get_scheduler_state({module, schedule_id} = key) when is_atom(module) do
    _ = schedule_id
    adapter_call(fn adapter, opts -> adapter.get_scheduler_state(key, opts) end)
  end

  @spec put_coverage_baseline(CoverageBaseline.t()) :: :ok | {:error, term()}
  def put_coverage_baseline(%CoverageBaseline{} = baseline) do
    adapter_call(fn adapter, opts -> adapter.put_coverage_baseline(baseline, opts) end)
  end

  @spec get_coverage_baseline(String.t()) :: {:ok, CoverageBaseline.t()} | {:error, term()}
  def get_coverage_baseline(baseline_id) when is_binary(baseline_id) do
    adapter_call(fn adapter, opts -> adapter.get_coverage_baseline(baseline_id, opts) end)
  end

  @spec list_coverage_baselines(keyword()) ::
          {:ok, Page.t(CoverageBaseline.t())} | {:error, term()}
  def list_coverage_baselines(filters \\ []) when is_list(filters) do
    paginated_adapter_call(filters, fn adapter, filters, opts ->
      adapter.list_coverage_baselines(filters, opts)
    end)
  end

  @spec put_backfill_window(BackfillWindow.t()) :: :ok | {:error, term()}
  def put_backfill_window(%BackfillWindow{} = window) do
    adapter_call(fn adapter, opts -> adapter.put_backfill_window(window, opts) end)
  end

  @spec put_backfill_windows([BackfillWindow.t()]) :: :ok | {:error, term()}
  def put_backfill_windows(windows) when is_list(windows) do
    if Enum.all?(windows, &match?(%BackfillWindow{}, &1)) do
      adapter_call(fn adapter, opts ->
        if function_exported?(adapter, :put_backfill_windows, 2) do
          adapter.put_backfill_windows(windows, opts)
        else
          put_all_adapter(windows, &adapter.put_backfill_window(&1, opts))
        end
      end)
    else
      {:error, :invalid_backfill_window}
    end
  end

  @spec get_backfill_window(String.t(), module(), String.t()) ::
          {:ok, BackfillWindow.t()} | {:error, term()}
  def get_backfill_window(backfill_run_id, pipeline_module, window_key)
      when is_binary(backfill_run_id) and is_atom(pipeline_module) and is_binary(window_key) do
    adapter_call(fn adapter, opts ->
      adapter.get_backfill_window(backfill_run_id, pipeline_module, window_key, opts)
    end)
  end

  @spec list_backfill_windows(keyword()) ::
          {:ok, Page.t(BackfillWindow.t())} | {:error, term()}
  def list_backfill_windows(filters \\ []) when is_list(filters) do
    paginated_adapter_call(filters, fn adapter, filters, opts ->
      adapter.list_backfill_windows(filters, opts)
    end)
  end

  @spec scan_backfill_windows(keyword(), keyword()) ::
          {:ok, CursorPage.t(BackfillWindow.t())} | {:error, term()}
  def scan_backfill_windows(filters \\ [], scan_opts \\ [])
      when is_list(filters) and is_list(scan_opts) do
    cursor_adapter_call(filters, scan_opts, fn adapter, filters, scan_opts, opts ->
      adapter.scan_backfill_windows(filters, scan_opts, opts)
    end)
  end

  @spec apply_backfill_child_projection(BackfillWindow.t(), [AssetWindowState.t()]) ::
          {:ok, BackfillProgress.t()} | {:error, term()}
  def apply_backfill_child_projection(%BackfillWindow{} = window, asset_window_states)
      when is_list(asset_window_states) do
    adapter_call(fn adapter, opts ->
      adapter.apply_backfill_child_projection(window, asset_window_states, opts)
    end)
  end

  @spec get_backfill_progress(String.t()) :: {:ok, BackfillProgress.t()} | {:error, term()}
  def get_backfill_progress(backfill_run_id) when is_binary(backfill_run_id) do
    adapter_call(fn adapter, opts -> adapter.get_backfill_progress(backfill_run_id, opts) end)
  end

  @spec rebuild_backfill_progress(String.t()) :: {:ok, BackfillProgress.t()} | {:error, term()}
  def rebuild_backfill_progress(backfill_run_id) when is_binary(backfill_run_id) do
    adapter_call(fn adapter, opts -> adapter.rebuild_backfill_progress(backfill_run_id, opts) end)
  end

  @spec put_asset_window_state(AssetWindowState.t()) :: :ok | {:error, term()}
  def put_asset_window_state(%AssetWindowState{} = state) do
    adapter_call(fn adapter, opts -> adapter.put_asset_window_state(state, opts) end)
  end

  @spec put_asset_window_states([AssetWindowState.t()]) :: :ok | {:error, term()}
  def put_asset_window_states(states) when is_list(states) do
    if Enum.all?(states, &match?(%AssetWindowState{}, &1)) do
      adapter_call(fn adapter, opts ->
        if function_exported?(adapter, :put_asset_window_states, 2) do
          adapter.put_asset_window_states(states, opts)
        else
          put_all_adapter(states, &adapter.put_asset_window_state(&1, opts))
        end
      end)
    else
      {:error, :invalid_asset_window_state}
    end
  end

  @spec get_asset_window_state(module(), atom(), String.t()) ::
          {:ok, AssetWindowState.t()} | {:error, term()}
  def get_asset_window_state(asset_ref_module, asset_ref_name, window_key)
      when is_atom(asset_ref_module) and is_atom(asset_ref_name) and is_binary(window_key) do
    adapter_call(fn adapter, opts ->
      adapter.get_asset_window_state(asset_ref_module, asset_ref_name, window_key, opts)
    end)
  end

  @spec list_asset_window_states(keyword()) ::
          {:ok, Page.t(AssetWindowState.t())} | {:error, term()}
  def list_asset_window_states(filters \\ []) when is_list(filters) do
    paginated_adapter_call(filters, fn adapter, filters, opts ->
      adapter.list_asset_window_states(filters, opts)
    end)
  end

  @spec put_asset_freshness_state(AssetFreshnessState.t()) :: :ok | {:error, term()}
  def put_asset_freshness_state(%AssetFreshnessState{} = state) do
    optional_adapter_call(
      :put_asset_freshness_state,
      [state],
      :asset_freshness_state_not_supported
    )
  end

  @spec get_asset_freshness_state(module(), atom(), String.t()) ::
          {:ok, AssetFreshnessState.t()} | {:error, term()}
  def get_asset_freshness_state(asset_ref_module, asset_ref_name, freshness_key)
      when is_atom(asset_ref_module) and is_atom(asset_ref_name) and is_binary(freshness_key) do
    optional_adapter_call(
      :get_asset_freshness_state,
      [asset_ref_module, asset_ref_name, freshness_key],
      :asset_freshness_state_not_supported
    )
  end

  @spec get_asset_freshness_states_by_keys([freshness_state_key()]) ::
          {:ok, %{freshness_state_key() => AssetFreshnessState.t()}} | {:error, term()}
  def get_asset_freshness_states_by_keys(keys) when is_list(keys) do
    with {:ok, keys} <- normalize_freshness_state_keys(keys) do
      adapter_call(fn adapter, opts -> adapter.get_asset_freshness_states_by_keys(keys, opts) end)
    end
  end

  @spec list_asset_freshness_states(keyword()) ::
          {:ok, Page.t(AssetFreshnessState.t())} | {:error, term()}
  def list_asset_freshness_states(filters \\ []) when is_list(filters) do
    with {:ok, page_opts} <- Page.normalize_opts(filters) do
      optional_adapter_call(
        :list_asset_freshness_states,
        [Keyword.merge(filters, page_opts)],
        :asset_freshness_state_not_supported
      )
    end
  end

  @spec scan_asset_freshness_states(keyword(), keyword()) ::
          {:ok, CursorPage.t(AssetFreshnessState.t())} | {:error, term()}
  def scan_asset_freshness_states(filters \\ [], scan_opts \\ [])
      when is_list(filters) and is_list(scan_opts) do
    cursor_adapter_call(filters, scan_opts, fn adapter, filters, scan_opts, opts ->
      adapter.scan_asset_freshness_states(filters, scan_opts, opts)
    end)
  end

  @spec upsert_target_status(TargetStatus.t()) :: :ok | {:error, term()}
  def upsert_target_status(%TargetStatus{} = status) do
    optional_adapter_call(:upsert_target_status, [status], :target_statuses_not_supported)
  end

  @spec get_target_status(String.t(), TargetStatus.target_kind(), String.t()) ::
          {:ok, TargetStatus.t()} | {:error, term()}
  def get_target_status(manifest_version_id, target_kind, target_id)
      when is_binary(manifest_version_id) and target_kind in [:asset, :pipeline] and
             is_binary(target_id) do
    optional_adapter_call(
      :get_target_status,
      [manifest_version_id, target_kind, target_id],
      :target_statuses_not_supported
    )
  end

  @spec list_target_statuses(String.t(), TargetStatus.target_kind(), [String.t()]) ::
          {:ok, %{String.t() => TargetStatus.t()}} | {:error, term()}
  def list_target_statuses(manifest_version_id, target_kind, target_ids)
      when is_binary(manifest_version_id) and target_kind in [:asset, :pipeline] and
             is_list(target_ids) do
    optional_adapter_call(
      :list_target_statuses,
      [manifest_version_id, target_kind, target_ids],
      :target_statuses_not_supported
    )
  end

  @spec replace_target_statuses(target_status_scope(), [TargetStatus.t()]) ::
          :ok | {:error, term()}
  def replace_target_statuses(scope, statuses) when is_list(statuses) do
    optional_adapter_call(
      :replace_target_statuses,
      [scope, statuses],
      :target_statuses_not_supported
    )
  end

  @spec delete_target_statuses(target_status_scope()) :: :ok | {:error, term()}
  def delete_target_statuses(scope) do
    optional_adapter_call(:delete_target_statuses, [scope], :target_statuses_not_supported)
  end

  @spec replace_backfill_read_models(
          read_model_replacement_scope(),
          [CoverageBaseline.t()],
          [BackfillWindow.t()],
          [AssetWindowState.t()]
        ) :: :ok | {:error, term()}
  def replace_backfill_read_models(
        scope,
        coverage_baselines,
        backfill_windows,
        asset_window_states
      )
      when is_list(coverage_baselines) and is_list(backfill_windows) and
             is_list(asset_window_states) do
    with {:ok, scope} <- normalize_replacement_scope(scope),
         :ok <-
           validate_replacement_rows(
             scope,
             coverage_baselines,
             backfill_windows,
             asset_window_states
           ) do
      adapter_call(fn adapter, opts ->
        adapter.replace_backfill_read_models(
          scope,
          coverage_baselines,
          backfill_windows,
          asset_window_states,
          opts
        )
      end)
    end
  end

  @spec put_auth_actor(map()) :: :ok | {:error, term()}
  def put_auth_actor(actor) when is_map(actor) do
    adapter_call(fn adapter, opts -> adapter.put_auth_actor(actor, opts) end)
  end

  @spec put_auth_actor_with_credential(map(), map()) :: :ok | {:error, term()}
  def put_auth_actor_with_credential(actor, credential)
      when is_map(actor) and is_map(credential) do
    adapter_call(fn adapter, opts ->
      adapter.put_auth_actor_with_credential(actor, credential, opts)
    end)
  end

  @spec get_auth_actor(String.t()) :: {:ok, map()} | {:error, term()}
  def get_auth_actor(actor_id) when is_binary(actor_id) do
    adapter_call(fn adapter, opts -> adapter.get_auth_actor(actor_id, opts) end)
  end

  @spec get_auth_actor_by_username(String.t()) :: {:ok, map()} | {:error, term()}
  def get_auth_actor_by_username(username) when is_binary(username) do
    adapter_call(fn adapter, opts -> adapter.get_auth_actor_by_username(username, opts) end)
  end

  @spec list_auth_actors() :: {:ok, [map()]} | {:error, term()}
  def list_auth_actors do
    adapter_call(fn adapter, opts -> adapter.list_auth_actors(opts) end)
  end

  @spec put_auth_credential(String.t(), map()) :: :ok | {:error, term()}
  def put_auth_credential(actor_id, credential) when is_binary(actor_id) and is_map(credential) do
    adapter_call(fn adapter, opts -> adapter.put_auth_credential(actor_id, credential, opts) end)
  end

  @spec update_auth_actor_password(String.t(), map(), map(), DateTime.t()) ::
          :ok | {:error, term()}
  def update_auth_actor_password(actor_id, actor, credential, %DateTime{} = revoked_at)
      when is_binary(actor_id) and is_map(actor) and is_map(credential) do
    adapter_call(fn adapter, opts ->
      adapter.update_auth_actor_password(actor_id, actor, credential, revoked_at, opts)
    end)
  end

  @spec get_auth_credential(String.t()) :: {:ok, map()} | {:error, term()}
  def get_auth_credential(actor_id) when is_binary(actor_id) do
    adapter_call(fn adapter, opts -> adapter.get_auth_credential(actor_id, opts) end)
  end

  @spec put_auth_session(map()) :: :ok | {:error, term()}
  def put_auth_session(session) when is_map(session) do
    adapter_call(fn adapter, opts -> adapter.put_auth_session(session, opts) end)
  end

  @spec get_auth_session(String.t()) :: {:ok, map()} | {:error, term()}
  def get_auth_session(session_id) when is_binary(session_id) do
    adapter_call(fn adapter, opts -> adapter.get_auth_session(session_id, opts) end)
  end

  @spec get_auth_session_by_token_hash(String.t()) :: {:ok, map()} | {:error, term()}
  def get_auth_session_by_token_hash(token_hash) when is_binary(token_hash) do
    adapter_call(fn adapter, opts -> adapter.get_auth_session_by_token_hash(token_hash, opts) end)
  end

  @spec revoke_auth_session(String.t(), DateTime.t()) :: :ok | {:error, term()}
  def revoke_auth_session(session_id, %DateTime{} = revoked_at) when is_binary(session_id) do
    adapter_call(fn adapter, opts ->
      adapter.revoke_auth_session(session_id, revoked_at, opts)
    end)
  end

  @spec revoke_auth_sessions_for_actor(String.t(), DateTime.t()) :: :ok | {:error, term()}
  def revoke_auth_sessions_for_actor(actor_id, %DateTime{} = revoked_at)
      when is_binary(actor_id) do
    adapter_call(fn adapter, opts ->
      adapter.revoke_auth_sessions_for_actor(actor_id, revoked_at, opts)
    end)
  end

  @spec put_auth_audit(map()) :: :ok | {:error, term()}
  def put_auth_audit(entry) when is_map(entry) do
    adapter_call(fn adapter, opts -> adapter.put_auth_audit(entry, opts) end)
  end

  @spec list_auth_audit(keyword()) :: {:ok, [map()]} | {:error, term()}
  def list_auth_audit(opts \\ []) when is_list(opts) do
    adapter_call(fn adapter, adapter_opts -> adapter.list_auth_audit(opts, adapter_opts) end)
  end

  @spec reserve_idempotency_record(map()) ::
          {:ok, {:reserved, map()} | {:replay, map()}}
          | {:error, :idempotency_conflict | :operation_in_progress | term()}
  def reserve_idempotency_record(record) when is_map(record) do
    optional_adapter_call(:reserve_idempotency_record, [record])
  end

  @spec complete_idempotency_record(String.t(), map()) :: :ok | {:error, term()}
  def complete_idempotency_record(record_id, attrs)
      when is_binary(record_id) and is_map(attrs) do
    optional_adapter_call(:complete_idempotency_record, [record_id, attrs])
  end

  @spec get_idempotency_record(String.t()) :: {:ok, map()} | {:error, term()}
  def get_idempotency_record(record_id) when is_binary(record_id) do
    optional_adapter_call(:get_idempotency_record, [record_id])
  end

  @spec adapter_module() :: module()
  def adapter_module do
    RuntimeConfig.current().storage_adapter
  end

  @spec adapter_opts() :: keyword()
  def adapter_opts do
    RuntimeConfig.current().storage_adapter_opts
  end

  defp redact_log_entries(entries) do
    policy = RuntimeConfig.current().log_redaction_policy

    entries
    |> Enum.reduce_while({:ok, []}, fn entry, {:ok, acc} ->
      case redact_log_entry(entry, policy) do
        {:ok, redacted} -> {:cont, {:ok, [redacted | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, redacted} -> {:ok, Enum.reverse(redacted)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp redact_log_entry(entry, policy) do
    with {:module, Favn.Log.Redactor} <- Code.ensure_loaded(Favn.Log.Redactor),
         true <- function_exported?(Favn.Log.Redactor, :redact, 2) do
      case Favn.Log.Redactor.redact(entry, policy) do
        {redacted_entry, _redacted?} -> {:ok, redacted_entry}
        redacted_entry -> {:ok, redacted_entry}
      end
    else
      _other -> {:ok, entry}
    end
  rescue
    error -> {:error, {:invalid_log_entry, error}}
  catch
    kind, reason -> {:error, {:invalid_log_entry, {kind, reason}}}
  end

  defp filter_run_events(events, opts) do
    events
    |> Enum.filter(fn event ->
      case Keyword.get(opts, :after_sequence) do
        sequence when is_integer(sequence) and sequence >= 0 ->
          Map.get(event, :sequence) > sequence

        _other ->
          true
      end
    end)
    |> maybe_limit_run_events(opts)
  end

  defp maybe_limit_run_events(events, opts) do
    case Keyword.get(opts, :limit) do
      limit when is_integer(limit) and limit > 0 -> Enum.take(events, limit)
      _other -> events
    end
  end

  defp normalize_freshness_state_keys(keys) do
    keys
    |> Enum.reduce_while({:ok, MapSet.new()}, fn
      {module, name, freshness_key} = key, {:ok, acc}
      when is_atom(module) and is_atom(name) and is_binary(freshness_key) ->
        {:cont, {:ok, MapSet.put(acc, key)}}

      key, {:ok, _acc} ->
        {:halt, {:error, {:invalid_freshness_state_key, key}}}
    end)
    |> case do
      {:ok, set} -> {:ok, MapSet.to_list(set)}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec validate_adapter(module()) :: :ok | {:error, term()}
  def validate_adapter(adapter) when is_atom(adapter) do
    with {:module, ^adapter} <- Code.ensure_loaded(adapter),
         callbacks <- required_adapter_callbacks(),
         true <-
           Enum.all?(callbacks, fn {name, arity} -> function_exported?(adapter, name, arity) end) do
      :ok
    else
      _ -> {:error, {:invalid_storage_adapter, adapter}}
    end
  end

  defp adapter_call(fun) when is_function(fun, 2) do
    runtime_config = RuntimeConfig.current()
    adapter = runtime_config.storage_adapter

    with :ok <- validate_adapter(adapter) do
      fun.(adapter, runtime_config.storage_adapter_opts)
    end
  rescue
    error -> {:error, {:raised, error}}
  catch
    :throw, reason -> {:error, {:thrown, reason}}
    :exit, reason -> {:error, {:exited, reason}}
  end

  defp optional_adapter_call(function, args) when is_atom(function) and is_list(args) do
    optional_adapter_call(function, args, :idempotency_not_supported)
  end

  defp optional_adapter_call(function, args, unsupported_reason)
       when is_atom(function) and is_list(args) do
    adapter_call(fn adapter, opts ->
      if function_exported?(adapter, function, length(args) + 1) do
        apply(adapter, function, args ++ [opts])
      else
        {:error, unsupported_reason}
      end
    end)
  end

  defp paginated_adapter_call(filters, fun) when is_list(filters) and is_function(fun, 3) do
    with {:ok, page_opts} <- Page.normalize_opts(filters) do
      adapter_call(fn adapter, opts -> fun.(adapter, Keyword.merge(filters, page_opts), opts) end)
    end
  end

  defp cursor_adapter_call(filters, scan_opts, fun)
       when is_list(filters) and is_list(scan_opts) and is_function(fun, 4) do
    with {:ok, scan_opts} <- CursorPage.normalize_opts(scan_opts) do
      adapter_call(fn adapter, opts -> fun.(adapter, filters, scan_opts, opts) end)
    end
  end

  defp put_all_adapter(items, fun) when is_list(items) and is_function(fun, 1) do
    Enum.reduce_while(items, :ok, fn item, :ok ->
      case fun.(item) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp normalize_replacement_scope(:all), do: {:ok, :all}

  defp normalize_replacement_scope({:backfill_run, id}) when is_binary(id) and id != "",
    do: {:ok, {:backfill_run, id}}

  defp normalize_replacement_scope({:pipeline, module}) when is_atom(module),
    do: {:ok, {:pipeline, module}}

  defp normalize_replacement_scope(scope),
    do: {:error, {:unsupported_replacement_scope, scope}}

  defp validate_replacement_rows(:all, _coverage_baselines, _backfill_windows, _asset_states),
    do: :ok

  defp validate_replacement_rows({:pipeline, module} = scope, baselines, windows, states) do
    if Enum.all?(baselines ++ windows ++ states, &match_pipeline_scope?(&1, module)) do
      :ok
    else
      {:error, {:replacement_row_out_of_scope, scope}}
    end
  end

  defp validate_replacement_rows({:backfill_run, id} = scope, baselines, windows, states) do
    valid? =
      Enum.all?(baselines, &(&1.created_by_run_id == id)) and
        Enum.all?(windows, &(&1.backfill_run_id == id)) and
        Enum.all?(states, &(&1.latest_parent_run_id == id))

    if valid?, do: :ok, else: {:error, {:replacement_row_out_of_scope, scope}}
  end

  defp match_pipeline_scope?(%{pipeline_module: module}, module), do: true
  defp match_pipeline_scope?(_value, _module), do: false

  defp maybe_child_to_list(:none), do: []
  defp maybe_child_to_list(value), do: [value]

  defp normalize_child_spec_result(:none), do: {:ok, :none}
  defp normalize_child_spec_result({:ok, child_spec}), do: {:ok, child_spec}
  defp normalize_child_spec_result({:error, reason}), do: {:error, reason}
  defp normalize_child_spec_result(other), do: {:error, {:invalid_child_spec_response, other}}

  defp required_adapter_callbacks do
    StorageAdapter.behaviour_info(:callbacks) --
      StorageAdapter.behaviour_info(:optional_callbacks)
  end
end
