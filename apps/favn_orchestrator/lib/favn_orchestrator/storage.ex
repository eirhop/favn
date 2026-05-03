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
  alias FavnOrchestrator.Backfill.AssetWindowState
  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.Backfill.CoverageBaseline
  alias FavnOrchestrator.Page
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage.Adapter.Memory

  @spec child_specs() :: {:ok, [Supervisor.child_spec()]} | {:error, term()}
  def child_specs do
    adapter = adapter_module()

    with :ok <- validate_adapter(adapter),
         child_spec_result <- adapter.child_spec(adapter_opts()),
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
        {:ok, %{status: :ready, ready?: true, adapter: adapter}}
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
          {:ok, %{status: :ready, ready?: true, adapter: adapter}}
      end
    end)
  end

  @spec put_manifest_version(Version.t()) :: :ok | {:error, term()}
  def put_manifest_version(%Version{} = version) do
    adapter_call(fn adapter, opts -> adapter.put_manifest_version(version, opts) end)
  end

  @spec get_manifest_version(String.t()) :: {:ok, Version.t()} | {:error, term()}
  def get_manifest_version(manifest_version_id) when is_binary(manifest_version_id) do
    adapter_call(fn adapter, opts -> adapter.get_manifest_version(manifest_version_id, opts) end)
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

  @spec get_run(String.t()) :: {:ok, RunState.t()} | {:error, term()}
  def get_run(run_id) when is_binary(run_id) do
    adapter_call(fn adapter, opts -> adapter.get_run(run_id, opts) end)
  end

  @spec list_runs(keyword()) :: {:ok, [RunState.t()]} | {:error, term()}
  def list_runs(run_opts \\ []) when is_list(run_opts) do
    adapter_call(fn adapter, opts -> adapter.list_runs(run_opts, opts) end)
  end

  @spec append_run_event(String.t(), map()) :: :ok | {:error, term()}
  def append_run_event(run_id, event) when is_binary(run_id) and is_map(event) do
    adapter_call(fn adapter, opts -> adapter.append_run_event(run_id, event, opts) end)
  end

  @spec list_run_events(String.t()) :: {:ok, [map()]} | {:error, term()}
  def list_run_events(run_id) when is_binary(run_id) do
    adapter_call(fn adapter, opts -> adapter.list_run_events(run_id, opts) end)
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

  @spec put_asset_window_state(AssetWindowState.t()) :: :ok | {:error, term()}
  def put_asset_window_state(%AssetWindowState{} = state) do
    adapter_call(fn adapter, opts -> adapter.put_asset_window_state(state, opts) end)
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

  @spec replace_backfill_read_models(
          keyword(),
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
      when is_list(scope) and is_list(coverage_baselines) and is_list(backfill_windows) and
             is_list(asset_window_states) do
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
    Application.get_env(:favn_orchestrator, :storage_adapter, Memory)
  end

  @spec adapter_opts() :: keyword()
  def adapter_opts do
    Application.get_env(:favn_orchestrator, :storage_adapter_opts, [])
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
    adapter = adapter_module()

    with :ok <- validate_adapter(adapter) do
      fun.(adapter, adapter_opts())
    end
  rescue
    error -> {:error, {:raised, error}}
  catch
    :throw, reason -> {:error, {:thrown, reason}}
    :exit, reason -> {:error, {:exited, reason}}
  end

  defp optional_adapter_call(function, args) when is_atom(function) and is_list(args) do
    adapter_call(fn adapter, opts ->
      if function_exported?(adapter, function, length(args) + 1) do
        apply(adapter, function, args ++ [opts])
      else
        {:error, :idempotency_not_supported}
      end
    end)
  end

  defp paginated_adapter_call(filters, fun) when is_list(filters) and is_function(fun, 3) do
    with {:ok, page_opts} <- Page.normalize_opts(filters) do
      adapter_call(fn adapter, opts -> fun.(adapter, Keyword.merge(filters, page_opts), opts) end)
    end
  end

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
