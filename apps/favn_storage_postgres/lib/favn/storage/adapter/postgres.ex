defmodule Favn.Storage.Adapter.Postgres do
  @moduledoc """
  Postgres-backed storage adapter implementing `Favn.Storage.Adapter`.
  """

  @behaviour Favn.Storage.Adapter

  alias Ecto.Adapters.SQL
  alias Favn.Manifest.Version
  alias FavnOrchestrator.Backfill.AssetWindowState
  alias FavnOrchestrator.Backfill.BackfillWindow
  alias FavnOrchestrator.Backfill.CoverageBaseline
  alias FavnOrchestrator.Page
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage.ManifestCodec
  alias FavnOrchestrator.Storage.PayloadCodec
  alias FavnOrchestrator.Storage.RunEventCodec
  alias FavnOrchestrator.Storage.RunSnapshotCodec
  alias FavnOrchestrator.Storage.RunStateCodec
  alias FavnOrchestrator.Storage.SchedulerStateCodec
  alias FavnOrchestrator.Storage.WriteSemantics
  alias FavnStoragePostgres.Migrations
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.Supervisor, as: PostgresSupervisor

  @active_manifest_key "active_manifest_version_id"
  @nil_schedule_id "__nil__"
  @schema_ready_cache_key {__MODULE__, :external_schema_ready}

  @impl true
  def child_spec(opts) when is_list(opts) do
    with {:ok, normalized} <- normalize_opts(opts) do
      case Keyword.fetch!(normalized, :repo_mode) do
        :managed ->
          supervisor_name = Keyword.fetch!(normalized, :supervisor_name)

          if Process.whereis(supervisor_name) do
            :none
          else
            {:ok,
             Supervisor.child_spec(
               {PostgresSupervisor,
                [
                  name: supervisor_name,
                  repo_config: Keyword.fetch!(normalized, :repo_config),
                  migration_mode: Keyword.fetch!(normalized, :migration_mode)
                ]},
               id: supervisor_name,
               restart: :permanent,
               shutdown: 5_000,
               type: :supervisor
             )}
          end

        :external ->
          :none
      end
    end
  end

  @impl true
  def put_manifest_version(%Version{} = version, opts) when is_list(opts) do
    with {:ok, repo} <- resolve_repo(opts),
         {:ok, record} <- ManifestCodec.to_record(version),
         {:ok, existing} <- fetch_manifest_hash(repo, record.manifest_version_id) do
      case existing do
        nil -> insert_manifest_record(repo, record)
        hash when hash == record.content_hash -> :ok
        _other -> {:error, :manifest_version_conflict}
      end
    end
  end

  @impl true
  def get_manifest_version(manifest_version_id, opts)
      when is_binary(manifest_version_id) and is_list(opts) do
    with {:ok, repo} <- resolve_repo(opts),
         {:ok, row} <- fetch_manifest_record(repo, manifest_version_id) do
      case row do
        nil -> {:error, :manifest_version_not_found}
        record -> ManifestCodec.from_record(record)
      end
    end
  end

  @impl true
  def list_manifest_versions(opts) when is_list(opts) do
    with {:ok, repo} <- resolve_repo(opts),
         {:ok, rows} <- fetch_manifest_records(repo) do
      rows
      |> Enum.reduce_while({:ok, []}, fn record, {:ok, acc} ->
        case ManifestCodec.from_record(record) do
          {:ok, version} -> {:cont, {:ok, [version | acc]}}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)
      |> case do
        {:ok, versions} -> {:ok, Enum.reverse(versions)}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def set_active_manifest_version(manifest_version_id, opts)
      when is_binary(manifest_version_id) and is_list(opts) do
    with {:ok, repo} <- resolve_repo(opts),
         {:ok, existing} <- fetch_manifest_hash(repo, manifest_version_id),
         false <- is_nil(existing) do
      sql =
        """
        INSERT INTO favn_runtime_settings (key, value_text, updated_at)
        VALUES ($1, $2, $3)
        ON CONFLICT(key) DO UPDATE SET value_text = EXCLUDED.value_text, updated_at = EXCLUDED.updated_at
        """

      case SQL.query(repo, sql, [@active_manifest_key, manifest_version_id, DateTime.utc_now()]) do
        {:ok, _} -> :ok
        {:error, reason} -> {:error, reason}
      end
    else
      true -> {:error, :manifest_version_not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def get_active_manifest_version(opts) when is_list(opts) do
    with {:ok, repo} <- resolve_repo(opts) do
      sql = "SELECT value_text FROM favn_runtime_settings WHERE key = $1 LIMIT 1"

      case SQL.query(repo, sql, [@active_manifest_key]) do
        {:ok, %{rows: [[manifest_version_id]]}}
        when is_binary(manifest_version_id) and manifest_version_id != "" ->
          {:ok, manifest_version_id}

        {:ok, %{rows: []}} ->
          {:error, :active_manifest_not_set}

        {:ok, _} ->
          {:error, :active_manifest_not_set}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @impl true
  def put_run(%RunState{} = run, opts) when is_list(opts) do
    with {:ok, repo} <- resolve_repo(opts),
         {:ok, normalized} <- RunStateCodec.normalize(run) do
      persist_run(repo, normalized)
    end
  end

  @impl true
  def persist_run_transition(%RunState{} = run, event, opts)
      when is_map(event) and is_list(opts) do
    with {:ok, repo} <- resolve_repo(opts),
         {:ok, normalized_run} <- RunStateCodec.normalize(run),
         {:ok, normalized_event} <- RunEventCodec.normalize(run.id, event),
         :ok <- validate_transition_alignment(normalized_run, normalized_event) do
      repo.transact(fn ->
        case guarded_put_run(repo, normalized_run) do
          :ok ->
            case guarded_append_run_event(repo, run.id, normalized_event) do
              result when result in [:ok, :idempotent] -> {:ok, result}
              {:error, reason} -> repo.rollback(reason)
            end

          {:error, reason} ->
            repo.rollback(reason)
        end
      end)
      |> case do
        {:ok, :ok} -> :ok
        {:ok, :idempotent} -> :idempotent
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def get_run(run_id, opts) when is_binary(run_id) and is_list(opts) do
    with {:ok, repo} <- resolve_repo(opts) do
      sql = run_snapshot_select() <> " WHERE r.run_id = $1 LIMIT 1"

      case SQL.query(repo, sql, [run_id]) do
        {:ok, %{rows: [row]}} -> decode_run_row(row)
        {:ok, %{rows: []}} -> {:error, :not_found}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def list_runs(run_opts, opts) when is_list(run_opts) and is_list(opts) do
    with {:ok, repo} <- resolve_repo(opts) do
      {sql, params} = list_runs_query(run_opts)

      case SQL.query(repo, sql, params) do
        {:ok, %{rows: rows}} ->
          rows
          |> Enum.reduce_while({:ok, []}, fn row, {:ok, acc} ->
            case decode_run_row(row) do
              {:ok, run} -> {:cont, {:ok, [run | acc]}}
              {:error, reason} -> {:halt, {:error, reason}}
            end
          end)
          |> case do
            {:ok, runs} -> {:ok, Enum.reverse(runs)}
            {:error, reason} -> {:error, reason}
          end

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @impl true
  def append_run_event(run_id, event, opts)
      when is_binary(run_id) and is_map(event) and is_list(opts) do
    with {:ok, repo} <- resolve_repo(opts),
         {:ok, normalized} <- RunEventCodec.normalize(run_id, event) do
      case guarded_append_run_event(repo, run_id, normalized) do
        :ok -> :ok
        :idempotent -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def list_run_events(run_id, opts) when is_binary(run_id) and is_list(opts) do
    with {:ok, repo} <- resolve_repo(opts) do
      sql =
        "SELECT global_sequence, event_blob FROM favn_run_events WHERE run_id = $1 ORDER BY sequence ASC"

      case SQL.query(repo, sql, [run_id]) do
        {:ok, %{rows: rows}} ->
          rows
          |> decode_event_rows()
          |> case do
            {:ok, events} -> {:ok, Enum.reverse(events)}
            {:error, reason} -> {:error, reason}
          end

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @impl true
  def list_global_run_events(run_event_opts, opts)
      when is_list(run_event_opts) and is_list(opts) do
    with {:ok, repo} <- resolve_repo(opts),
         {:ok, query, params} <- global_run_events_query(repo, run_event_opts) do
      case SQL.query(repo, query, params) do
        {:ok, %{rows: rows}} ->
          rows
          |> decode_event_rows()
          |> case do
            {:ok, events} -> {:ok, Enum.reverse(events)}
            {:error, reason} -> {:error, reason}
          end

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @impl true
  def put_scheduler_state(key, state, opts) when is_map(state) and is_list(opts) do
    with {:ok, repo} <- resolve_repo(opts),
         {:ok, normalized_key} <- SchedulerStateCodec.normalize_key(key),
         {:ok, normalized_state} <- SchedulerStateCodec.normalize_state(state) do
      persist_scheduler_state(repo, normalized_key, normalized_state)
    end
  end

  @impl true
  def get_scheduler_state(key, opts) when is_list(opts) do
    with {:ok, repo} <- resolve_repo(opts),
         {:ok, {pipeline_module, schedule_id}} <- SchedulerStateCodec.normalize_key(key) do
      sql =
        """
        SELECT version, state_blob
        FROM favn_scheduler_cursors
        WHERE pipeline_module = $1 AND schedule_id = $2
        LIMIT 1
        """

      params = [Atom.to_string(pipeline_module), encode_schedule_id(schedule_id)]

      case SQL.query(repo, sql, params) do
        {:ok, %{rows: []}} ->
          {:ok, nil}

        {:ok, %{rows: [[version, payload]]}} ->
          with {:ok, decoded} <- decode_payload(payload),
               true <- is_map(decoded) do
            {:ok,
             struct(
               Favn.Scheduler.State,
               Map.merge(decoded, %{
                 pipeline_module: pipeline_module,
                 schedule_id: schedule_id,
                 version: version
               })
             )}
          else
            false -> {:error, :invalid_scheduler_payload}
            {:error, reason} -> {:error, reason}
          end

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @impl true
  def put_coverage_baseline(%CoverageBaseline{} = baseline, opts) when is_list(opts) do
    with {:ok, repo} <- resolve_repo(opts) do
      sql =
        """
        INSERT INTO favn_pipeline_coverage_baselines (
          baseline_id, pipeline_module, source_key, segment_key_hash, segment_key_redacted,
          window_kind, timezone, coverage_start_at, coverage_until, created_by_run_id,
          manifest_version_id, status, errors_payload, metadata_payload, created_at, updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
        ON CONFLICT(baseline_id) DO UPDATE SET
          pipeline_module = EXCLUDED.pipeline_module,
          source_key = EXCLUDED.source_key,
          segment_key_hash = EXCLUDED.segment_key_hash,
          segment_key_redacted = EXCLUDED.segment_key_redacted,
          window_kind = EXCLUDED.window_kind,
          timezone = EXCLUDED.timezone,
          coverage_start_at = EXCLUDED.coverage_start_at,
          coverage_until = EXCLUDED.coverage_until,
          created_by_run_id = EXCLUDED.created_by_run_id,
          manifest_version_id = EXCLUDED.manifest_version_id,
          status = EXCLUDED.status,
          errors_payload = EXCLUDED.errors_payload,
          metadata_payload = EXCLUDED.metadata_payload,
          created_at = EXCLUDED.created_at,
          updated_at = EXCLUDED.updated_at
        """

      case SQL.query(repo, sql, coverage_baseline_params(baseline)) do
        {:ok, _} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def get_coverage_baseline(baseline_id, opts) when is_binary(baseline_id) and is_list(opts) do
    with {:ok, repo} <- resolve_repo(opts) do
      sql = coverage_baseline_select() <> "\nWHERE baseline_id = $1\nLIMIT 1"

      case SQL.query(repo, sql, [baseline_id]) do
        {:ok, %{rows: [row]}} -> decode_coverage_baseline_row(row)
        {:ok, %{rows: []}} -> {:error, :not_found}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def list_coverage_baselines(filters, opts) when is_list(filters) and is_list(opts) do
    with {:ok, repo} <- resolve_repo(opts),
         {:ok, page_opts} <- page_opts(filters),
         {:ok, sql, params} <-
           build_select_query(
             coverage_baseline_select(),
             read_filters(filters),
             coverage_baseline_filter_specs(),
             "ORDER BY updated_at DESC, baseline_id ASC"
           ) do
      query_and_decode_page(repo, sql, params, page_opts, &decode_coverage_baseline_row/1)
    end
  end

  @impl true
  def put_backfill_window(%BackfillWindow{} = window, opts) when is_list(opts) do
    with {:ok, repo} <- resolve_repo(opts) do
      sql =
        """
        INSERT INTO favn_backfill_windows (
          backfill_run_id, child_run_id, pipeline_module, manifest_version_id, coverage_baseline_id,
          window_kind, window_start_at, window_end_at, timezone, window_key, status,
          attempt_count, latest_attempt_run_id, last_success_run_id, last_error_payload,
          errors_payload, metadata_payload, started_at, finished_at, created_at, updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
        ON CONFLICT(backfill_run_id, pipeline_module, window_key) DO UPDATE SET
          child_run_id = EXCLUDED.child_run_id,
          manifest_version_id = EXCLUDED.manifest_version_id,
          coverage_baseline_id = EXCLUDED.coverage_baseline_id,
          window_kind = EXCLUDED.window_kind,
          window_start_at = EXCLUDED.window_start_at,
          window_end_at = EXCLUDED.window_end_at,
          timezone = EXCLUDED.timezone,
          status = EXCLUDED.status,
          attempt_count = EXCLUDED.attempt_count,
          latest_attempt_run_id = EXCLUDED.latest_attempt_run_id,
          last_success_run_id = EXCLUDED.last_success_run_id,
          last_error_payload = EXCLUDED.last_error_payload,
          errors_payload = EXCLUDED.errors_payload,
          metadata_payload = EXCLUDED.metadata_payload,
          started_at = EXCLUDED.started_at,
          finished_at = EXCLUDED.finished_at,
          created_at = EXCLUDED.created_at,
          updated_at = EXCLUDED.updated_at
        """

      case SQL.query(repo, sql, backfill_window_params(window)) do
        {:ok, _} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def get_backfill_window(backfill_run_id, pipeline_module, window_key, opts)
      when is_binary(backfill_run_id) and is_atom(pipeline_module) and is_binary(window_key) and
             is_list(opts) do
    with {:ok, repo} <- resolve_repo(opts) do
      sql =
        backfill_window_select() <>
          "\nWHERE backfill_run_id = $1 AND pipeline_module = $2 AND window_key = $3\nLIMIT 1"

      params = [backfill_run_id, Atom.to_string(pipeline_module), window_key]

      case SQL.query(repo, sql, params) do
        {:ok, %{rows: [row]}} -> decode_backfill_window_row(row)
        {:ok, %{rows: []}} -> {:error, :not_found}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def list_backfill_windows(filters, opts) when is_list(filters) and is_list(opts) do
    with {:ok, repo} <- resolve_repo(opts),
         {:ok, page_opts} <- page_opts(filters),
         {:ok, sql, params} <-
           build_select_query(
             backfill_window_select(),
             read_filters(filters),
             backfill_window_filter_specs(),
             "ORDER BY window_start_at ASC, backfill_run_id ASC, pipeline_module ASC, window_key ASC"
           ) do
      query_and_decode_page(repo, sql, params, page_opts, &decode_backfill_window_row/1)
    end
  end

  @impl true
  def put_asset_window_state(%AssetWindowState{} = state, opts) when is_list(opts) do
    with {:ok, repo} <- resolve_repo(opts) do
      sql =
        """
        INSERT INTO favn_asset_window_states (
          asset_ref_module, asset_ref_name, pipeline_module, manifest_version_id, window_kind,
          window_start_at, window_end_at, timezone, window_key, status, latest_run_id,
          latest_parent_run_id, latest_success_run_id, latest_error_payload, rows_written,
          errors_payload, metadata_payload, updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
        ON CONFLICT(asset_ref_module, asset_ref_name, window_key) DO UPDATE SET
          pipeline_module = EXCLUDED.pipeline_module,
          manifest_version_id = EXCLUDED.manifest_version_id,
          window_kind = EXCLUDED.window_kind,
          window_start_at = EXCLUDED.window_start_at,
          window_end_at = EXCLUDED.window_end_at,
          timezone = EXCLUDED.timezone,
          status = EXCLUDED.status,
          latest_run_id = EXCLUDED.latest_run_id,
          latest_parent_run_id = EXCLUDED.latest_parent_run_id,
          latest_success_run_id = EXCLUDED.latest_success_run_id,
          latest_error_payload = EXCLUDED.latest_error_payload,
          rows_written = EXCLUDED.rows_written,
          errors_payload = EXCLUDED.errors_payload,
          metadata_payload = EXCLUDED.metadata_payload,
          updated_at = EXCLUDED.updated_at
        """

      case SQL.query(repo, sql, asset_window_state_params(state)) do
        {:ok, _} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def get_asset_window_state(asset_ref_module, asset_ref_name, window_key, opts)
      when is_atom(asset_ref_module) and is_atom(asset_ref_name) and is_binary(window_key) and
             is_list(opts) do
    with {:ok, repo} <- resolve_repo(opts) do
      sql =
        asset_window_state_select() <>
          "\nWHERE asset_ref_module = $1 AND asset_ref_name = $2 AND window_key = $3\nLIMIT 1"

      params = [Atom.to_string(asset_ref_module), Atom.to_string(asset_ref_name), window_key]

      case SQL.query(repo, sql, params) do
        {:ok, %{rows: [row]}} -> decode_asset_window_state_row(row)
        {:ok, %{rows: []}} -> {:error, :not_found}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def list_asset_window_states(filters, opts) when is_list(filters) and is_list(opts) do
    with {:ok, repo} <- resolve_repo(opts),
         {:ok, page_opts} <- page_opts(filters),
         {:ok, sql, params} <-
           build_select_query(
             asset_window_state_select(),
             read_filters(filters),
             asset_window_state_filter_specs(),
             "ORDER BY updated_at DESC, asset_ref_module ASC, asset_ref_name ASC, window_key ASC"
           ) do
      query_and_decode_page(repo, sql, params, page_opts, &decode_asset_window_state_row/1)
    end
  end

  @impl true
  def replace_backfill_read_models(
        scope,
        coverage_baselines,
        backfill_windows,
        asset_window_states,
        opts
      )
      when is_list(scope) and is_list(coverage_baselines) and is_list(backfill_windows) and
             is_list(asset_window_states) and is_list(opts) do
    with {:ok, repo} <- resolve_repo(opts) do
      repo.transact(fn ->
        with :ok <-
               delete_scoped(
                 repo,
                 "favn_pipeline_coverage_baselines",
                 scope,
                 coverage_baseline_filter_specs()
               ),
             :ok <-
               delete_scoped(repo, "favn_backfill_windows", scope, backfill_window_filter_specs()),
             :ok <-
               delete_scoped(
                 repo,
                 "favn_asset_window_states",
                 scope,
                 asset_window_state_filter_specs()
               ),
             :ok <- put_all(coverage_baselines, &put_coverage_baseline(&1, opts)),
             :ok <- put_all(backfill_windows, &put_backfill_window(&1, opts)),
             :ok <- put_all(asset_window_states, &put_asset_window_state(&1, opts)) do
          {:ok, :ok}
        else
          {:error, reason} -> repo.rollback(reason)
        end
      end)
      |> case do
        {:ok, :ok} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def put_auth_actor(_actor, _opts), do: auth_persistence_not_supported()

  @impl true
  def put_auth_actor_with_credential(_actor, _credential, _opts),
    do: auth_persistence_not_supported()

  @impl true
  def get_auth_actor(_actor_id, _opts), do: auth_persistence_not_supported()

  @impl true
  def get_auth_actor_by_username(_username, _opts), do: auth_persistence_not_supported()

  @impl true
  def list_auth_actors(_opts), do: auth_persistence_not_supported()

  @impl true
  def put_auth_credential(_actor_id, _credential, _opts), do: auth_persistence_not_supported()

  @impl true
  def update_auth_actor_password(_actor_id, _actor, _credential, _revoked_at, _opts),
    do: auth_persistence_not_supported()

  @impl true
  def get_auth_credential(_actor_id, _opts), do: auth_persistence_not_supported()

  @impl true
  def put_auth_session(_session, _opts), do: auth_persistence_not_supported()

  @impl true
  def get_auth_session(_session_id, _opts), do: auth_persistence_not_supported()

  @impl true
  def get_auth_session_by_token_hash(_token_hash, _opts), do: auth_persistence_not_supported()

  @impl true
  def revoke_auth_session(_session_id, _revoked_at, _opts), do: auth_persistence_not_supported()

  @impl true
  def revoke_auth_sessions_for_actor(_actor_id, _revoked_at, _opts),
    do: auth_persistence_not_supported()

  @impl true
  def put_auth_audit(_entry, _opts), do: auth_persistence_not_supported()

  @impl true
  def list_auth_audit(_audit_opts, _opts), do: auth_persistence_not_supported()

  defp coverage_baseline_params(%CoverageBaseline{} = baseline) do
    [
      baseline.baseline_id,
      Atom.to_string(baseline.pipeline_module),
      baseline.source_key,
      baseline.segment_key_hash,
      baseline.segment_key_redacted,
      Atom.to_string(baseline.window_kind),
      baseline.timezone,
      baseline.coverage_start_at,
      baseline.coverage_until,
      baseline.created_by_run_id,
      baseline.manifest_version_id,
      Atom.to_string(baseline.status),
      encode_payload(baseline.errors),
      encode_payload(baseline.metadata),
      baseline.created_at,
      baseline.updated_at
    ]
  end

  defp backfill_window_params(%BackfillWindow{} = window) do
    [
      window.backfill_run_id,
      window.child_run_id,
      Atom.to_string(window.pipeline_module),
      window.manifest_version_id,
      window.coverage_baseline_id,
      Atom.to_string(window.window_kind),
      window.window_start_at,
      window.window_end_at,
      window.timezone,
      window.window_key,
      Atom.to_string(window.status),
      window.attempt_count,
      window.latest_attempt_run_id,
      window.last_success_run_id,
      encode_payload(window.last_error),
      encode_payload(window.errors),
      encode_payload(window.metadata),
      window.started_at,
      window.finished_at,
      window.created_at,
      window.updated_at
    ]
  end

  defp asset_window_state_params(%AssetWindowState{} = state) do
    [
      Atom.to_string(state.asset_ref_module),
      Atom.to_string(state.asset_ref_name),
      Atom.to_string(state.pipeline_module),
      state.manifest_version_id,
      Atom.to_string(state.window_kind),
      state.window_start_at,
      state.window_end_at,
      state.timezone,
      state.window_key,
      Atom.to_string(state.status),
      state.latest_run_id,
      state.latest_parent_run_id,
      state.latest_success_run_id,
      encode_payload(state.latest_error),
      state.rows_written,
      encode_payload(state.errors),
      encode_payload(state.metadata),
      state.updated_at
    ]
  end

  defp coverage_baseline_select do
    """
    SELECT baseline_id, pipeline_module, source_key, segment_key_hash, segment_key_redacted,
           window_kind, timezone, coverage_start_at, coverage_until, created_by_run_id,
           manifest_version_id, status, errors_payload, metadata_payload, created_at, updated_at
    FROM favn_pipeline_coverage_baselines
    """
    |> String.trim_trailing()
  end

  defp backfill_window_select do
    """
    SELECT backfill_run_id, child_run_id, pipeline_module, manifest_version_id, coverage_baseline_id,
           window_kind, window_start_at, window_end_at, timezone, window_key, status,
           attempt_count, latest_attempt_run_id, last_success_run_id, last_error_payload,
           errors_payload, metadata_payload, started_at, finished_at, created_at, updated_at
    FROM favn_backfill_windows
    """
    |> String.trim_trailing()
  end

  defp asset_window_state_select do
    """
    SELECT asset_ref_module, asset_ref_name, pipeline_module, manifest_version_id, window_kind,
           window_start_at, window_end_at, timezone, window_key, status, latest_run_id,
           latest_parent_run_id, latest_success_run_id, latest_error_payload, rows_written,
           errors_payload, metadata_payload, updated_at
    FROM favn_asset_window_states
    """
    |> String.trim_trailing()
  end

  defp coverage_baseline_filter_specs do
    %{
      baseline_id: {"baseline_id", & &1},
      pipeline_module: {"pipeline_module", &Atom.to_string/1},
      source_key: {"source_key", & &1},
      segment_key_hash: {"segment_key_hash", & &1},
      window_kind: {"window_kind", &Atom.to_string/1},
      status: {"status", &Atom.to_string/1},
      manifest_version_id: {"manifest_version_id", & &1}
    }
  end

  defp backfill_window_filter_specs do
    %{
      backfill_run_id: {"backfill_run_id", & &1},
      pipeline_module: {"pipeline_module", &Atom.to_string/1},
      window_key: {"window_key", & &1},
      window_kind: {"window_kind", &Atom.to_string/1},
      status: {"status", &Atom.to_string/1},
      coverage_baseline_id: {"coverage_baseline_id", & &1},
      manifest_version_id: {"manifest_version_id", & &1}
    }
  end

  defp asset_window_state_filter_specs do
    %{
      asset_ref_module: {"asset_ref_module", &Atom.to_string/1},
      asset_ref_name: {"asset_ref_name", &Atom.to_string/1},
      pipeline_module: {"pipeline_module", &Atom.to_string/1},
      window_key: {"window_key", & &1},
      window_kind: {"window_kind", &Atom.to_string/1},
      status: {"status", &Atom.to_string/1},
      manifest_version_id: {"manifest_version_id", & &1}
    }
  end

  defp build_select_query(select_sql, filters, specs, order_sql) do
    filters
    |> Enum.reduce_while({:ok, [], []}, fn {key, value}, {:ok, clauses, params} ->
      case Map.fetch(specs, key) do
        {:ok, {column, encoder}} ->
          placeholder = "$#{length(params) + 1}"
          {:cont, {:ok, ["#{column} = #{placeholder}" | clauses], params ++ [encoder.(value)]}}

        :error ->
          {:halt, {:error, {:unsupported_filter, key}}}
      end
    end)
    |> case do
      {:ok, clauses, params} ->
        sql =
          case Enum.reverse(clauses) do
            [] ->
              select_sql <> "\n" <> order_sql

            clauses ->
              select_sql <> "\nWHERE " <> Enum.join(clauses, " AND ") <> "\n" <> order_sql
          end

        {:ok, sql, params}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp query_and_decode_rows(repo, sql, params, decoder) do
    case SQL.query(repo, sql, params) do
      {:ok, %{rows: rows}} ->
        rows
        |> Enum.reduce_while({:ok, []}, fn row, {:ok, acc} ->
          case decoder.(row) do
            {:ok, value} -> {:cont, {:ok, [value | acc]}}
            {:error, reason} -> {:halt, {:error, reason}}
          end
        end)
        |> case do
          {:ok, values} -> {:ok, Enum.reverse(values)}
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp query_and_decode_page(repo, sql, params, page_opts, decoder) do
    limit_placeholder = "$#{length(params) + 1}"
    offset_placeholder = "$#{length(params) + 2}"
    sql = sql <> "\nLIMIT #{limit_placeholder} OFFSET #{offset_placeholder}"
    params = params ++ [Keyword.fetch!(page_opts, :limit) + 1, Keyword.fetch!(page_opts, :offset)]

    with {:ok, rows} <- query_and_decode_rows(repo, sql, params, decoder) do
      {:ok, Page.from_fetched(rows, page_opts)}
    end
  end

  defp read_filters(filters), do: Keyword.drop(filters, [:limit, :offset])

  defp put_all(items, fun) when is_list(items) and is_function(fun, 1) do
    Enum.reduce_while(items, :ok, fn item, :ok ->
      case fun.(item) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp delete_scoped(repo, table, [], _specs) do
    case SQL.query(repo, "DELETE FROM #{table}", []) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp delete_scoped(repo, table, scope, specs) do
    filters = Enum.filter(scope, fn {key, _value} -> Map.has_key?(specs, key) end)

    if filters == [] do
      :ok
    else
      with {:ok, clauses, params} <- delete_filter_clauses(filters, specs) do
        case SQL.query(repo, "DELETE FROM #{table} WHERE #{Enum.join(clauses, " AND ")}", params) do
          {:ok, _} -> :ok
          {:error, reason} -> {:error, reason}
        end
      end
    end
  end

  defp delete_filter_clauses(filters, specs) do
    filters
    |> Enum.reduce_while({:ok, [], []}, fn {key, value}, {:ok, clauses, params} ->
      case Map.fetch(specs, key) do
        {:ok, {column, encoder}} ->
          placeholder = "$#{length(params) + 1}"
          {:cont, {:ok, ["#{column} = #{placeholder}" | clauses], params ++ [encoder.(value)]}}

        :error ->
          {:halt, {:error, {:unsupported_filter, key}}}
      end
    end)
    |> case do
      {:ok, clauses, params} -> {:ok, Enum.reverse(clauses), params}
      {:error, reason} -> {:error, reason}
    end
  end

  defp page_opts(filters), do: Page.normalize_opts(filters)

  defp decode_coverage_baseline_row([
         baseline_id,
         pipeline_module,
         source_key,
         segment_key_hash,
         segment_key_redacted,
         window_kind,
         timezone,
         coverage_start_at,
         coverage_until,
         created_by_run_id,
         manifest_version_id,
         status,
         errors_payload,
         metadata_payload,
         created_at,
         updated_at
       ]) do
    with {:ok, pipeline_module} <- existing_atom(pipeline_module),
         {:ok, errors} <- decode_payload(errors_payload),
         {:ok, metadata} <- decode_payload(metadata_payload) do
      CoverageBaseline.new(%{
        baseline_id: baseline_id,
        pipeline_module: pipeline_module,
        source_key: source_key,
        segment_key_hash: segment_key_hash,
        segment_key_redacted: segment_key_redacted,
        window_kind: window_kind,
        timezone: timezone,
        coverage_start_at: coverage_start_at,
        coverage_until: coverage_until,
        created_by_run_id: created_by_run_id,
        manifest_version_id: manifest_version_id,
        status: status,
        errors: errors,
        metadata: metadata,
        created_at: created_at,
        updated_at: updated_at
      })
    end
  end

  defp decode_backfill_window_row([
         backfill_run_id,
         child_run_id,
         pipeline_module,
         manifest_version_id,
         coverage_baseline_id,
         window_kind,
         window_start_at,
         window_end_at,
         timezone,
         window_key,
         status,
         attempt_count,
         latest_attempt_run_id,
         last_success_run_id,
         last_error_payload,
         errors_payload,
         metadata_payload,
         started_at,
         finished_at,
         created_at,
         updated_at
       ]) do
    with {:ok, pipeline_module} <- existing_atom(pipeline_module),
         {:ok, last_error} <- decode_payload(last_error_payload),
         {:ok, errors} <- decode_payload(errors_payload),
         {:ok, metadata} <- decode_payload(metadata_payload) do
      BackfillWindow.new(%{
        backfill_run_id: backfill_run_id,
        child_run_id: child_run_id,
        pipeline_module: pipeline_module,
        manifest_version_id: manifest_version_id,
        coverage_baseline_id: coverage_baseline_id,
        window_kind: window_kind,
        window_start_at: window_start_at,
        window_end_at: window_end_at,
        timezone: timezone,
        window_key: window_key,
        status: status,
        attempt_count: attempt_count,
        latest_attempt_run_id: latest_attempt_run_id,
        last_success_run_id: last_success_run_id,
        last_error: last_error,
        errors: errors,
        metadata: metadata,
        started_at: started_at,
        finished_at: finished_at,
        created_at: created_at,
        updated_at: updated_at
      })
    end
  end

  defp decode_asset_window_state_row([
         asset_ref_module,
         asset_ref_name,
         pipeline_module,
         manifest_version_id,
         window_kind,
         window_start_at,
         window_end_at,
         timezone,
         window_key,
         status,
         latest_run_id,
         latest_parent_run_id,
         latest_success_run_id,
         latest_error_payload,
         rows_written,
         errors_payload,
         metadata_payload,
         updated_at
       ]) do
    with {:ok, asset_ref_module} <- existing_atom(asset_ref_module),
         {:ok, asset_ref_name} <- existing_atom(asset_ref_name),
         {:ok, pipeline_module} <- existing_atom(pipeline_module),
         {:ok, latest_error} <- decode_payload(latest_error_payload),
         {:ok, errors} <- decode_payload(errors_payload),
         {:ok, metadata} <- decode_payload(metadata_payload) do
      AssetWindowState.new(%{
        asset_ref_module: asset_ref_module,
        asset_ref_name: asset_ref_name,
        pipeline_module: pipeline_module,
        manifest_version_id: manifest_version_id,
        window_kind: window_kind,
        window_start_at: window_start_at,
        window_end_at: window_end_at,
        timezone: timezone,
        window_key: window_key,
        status: status,
        latest_run_id: latest_run_id,
        latest_parent_run_id: latest_parent_run_id,
        latest_success_run_id: latest_success_run_id,
        latest_error: latest_error,
        rows_written: rows_written,
        errors: errors,
        metadata: metadata,
        updated_at: updated_at
      })
    end
  end

  defp persist_run(repo, run) do
    repo.transact(fn ->
      case guarded_put_run(repo, run) do
        :ok -> {:ok, :ok}
        {:error, reason} -> repo.rollback(reason)
      end
    end)
    |> case do
      {:ok, :ok} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp guarded_put_run(repo, run) do
    with {:ok, updated_seq} <- next_updated_seq(repo) do
      sql =
        """
        INSERT INTO favn_runs (
          run_id,
          manifest_version_id,
          manifest_content_hash,
          status,
          event_seq,
          snapshot_hash,
          updated_seq,
          inserted_at,
          updated_at,
          run_blob
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT(run_id) DO UPDATE SET
          manifest_version_id = EXCLUDED.manifest_version_id,
          manifest_content_hash = EXCLUDED.manifest_content_hash,
          status = EXCLUDED.status,
          event_seq = EXCLUDED.event_seq,
          snapshot_hash = EXCLUDED.snapshot_hash,
          updated_seq = EXCLUDED.updated_seq,
          inserted_at = EXCLUDED.inserted_at,
          updated_at = EXCLUDED.updated_at,
          run_blob = EXCLUDED.run_blob
        WHERE EXCLUDED.event_seq > favn_runs.event_seq
        """

      params = [
        run.id,
        run.manifest_version_id,
        run.manifest_content_hash,
        to_string(run.status),
        run.event_seq,
        run.snapshot_hash,
        updated_seq,
        run.inserted_at,
        run.updated_at,
        encode_payload(run)
      ]

      case SQL.query(repo, sql, params) do
        {:ok, %{num_rows: num_rows}} when num_rows > 0 -> :ok
        {:ok, _} -> classify_run_write_result(repo, run)
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp classify_run_write_result(repo, run) do
    with {:ok, existing} <- fetch_run_head(repo, run.id) do
      case existing do
        {existing_event_seq, existing_hash} ->
          case WriteSemantics.decide(
                 existing_event_seq,
                 existing_hash,
                 run.event_seq,
                 run.snapshot_hash
               ) do
            :idempotent -> :ok
            {:error, reason} -> {:error, reason}
            other -> {:error, {:unexpected_run_write_decision, other}}
          end

        nil ->
          {:error, :run_write_not_applied}
      end
    end
  end

  defp guarded_append_run_event(repo, run_id, event) do
    with {:ok, global_sequence} <- next_run_event_global_sequence(repo) do
      event = Map.put(event, :global_sequence, global_sequence)

      sql =
        """
        INSERT INTO favn_run_events (run_id, sequence, global_sequence, occurred_at, event_blob)
        VALUES ($1, $2, $3, $4, $5)
        """

      case SQL.query(repo, sql, [
             run_id,
             event.sequence,
             global_sequence,
             event.occurred_at,
             encode_payload(event)
           ]) do
        {:ok, _} ->
          :ok

        {:error, %Postgrex.Error{postgres: %{code: :unique_violation}}} ->
          resolve_existing_event_conflict(repo, run_id, event)

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp resolve_existing_event_conflict(repo, run_id, event) do
    with {:ok, existing} <- fetch_event_by_sequence(repo, run_id, event.sequence) do
      case WriteSemantics.decide_run_event_append(existing, event) do
        :idempotent -> :idempotent
        :insert -> {:error, :missing_conflicting_event}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp persist_scheduler_state(repo, key, normalized_state) do
    repo.transact(fn ->
      case guarded_put_scheduler_state(repo, key, normalized_state) do
        :ok -> {:ok, :ok}
        {:error, reason} -> repo.rollback(reason)
      end
    end)
    |> case do
      {:ok, :ok} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp guarded_put_scheduler_state(repo, {pipeline_module, schedule_id} = key, normalized_state) do
    encoded_state = normalized_state |> Map.delete(:version) |> encode_payload()
    encoded_schedule_id = encode_schedule_id(schedule_id)
    updated_at = DateTime.utc_now()

    {sql, params} =
      scheduler_upsert_query(
        Atom.to_string(pipeline_module),
        encoded_schedule_id,
        normalized_state,
        updated_at,
        encoded_state
      )

    case SQL.query(repo, sql, params) do
      {:ok, %{num_rows: num_rows}} when num_rows > 0 -> :ok
      {:ok, _} -> classify_scheduler_write_result(repo, key, normalized_state[:version])
      {:error, reason} -> {:error, reason}
    end
  end

  defp scheduler_upsert_query(
         pipeline_module,
         encoded_schedule_id,
         normalized_state,
         updated_at,
         encoded_state
       ) do
    incoming_version = Map.fetch!(normalized_state, :version)
    expected_previous = incoming_version - 1

    {
      """
      INSERT INTO favn_scheduler_cursors (pipeline_module, schedule_id, version, updated_at, state_blob)
      SELECT $1, $2, $3, $4, $5
      WHERE $3 = 1 OR EXISTS (
        SELECT 1
        FROM favn_scheduler_cursors
        WHERE pipeline_module = $1 AND schedule_id = $2
      )
      ON CONFLICT(pipeline_module, schedule_id) DO UPDATE SET
        version = EXCLUDED.version,
        updated_at = EXCLUDED.updated_at,
        state_blob = EXCLUDED.state_blob
      WHERE favn_scheduler_cursors.version = $6
      """,
      [
        pipeline_module,
        encoded_schedule_id,
        incoming_version,
        updated_at,
        encoded_state,
        expected_previous
      ]
    }
  end

  defp classify_scheduler_write_result(repo, key, incoming_version) do
    with {:ok, existing_version} <- fetch_scheduler_version(repo, key) do
      case resolve_scheduler_version(existing_version, incoming_version) do
        {:ok, _version} -> {:error, :scheduler_write_not_applied}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp next_updated_seq(repo) do
    sql = "SELECT nextval('favn_run_write_seq')"

    case SQL.query(repo, sql, []) do
      {:ok, %{rows: [[value]]}} when is_integer(value) -> {:ok, value}
      {:ok, _} -> {:error, :invalid_counter_value}
      {:error, reason} -> {:error, reason}
    end
  end

  defp next_run_event_global_sequence(repo) do
    sql = "SELECT nextval('favn_run_event_global_seq')"

    case SQL.query(repo, sql, []) do
      {:ok, %{rows: [[value]]}} when is_integer(value) -> {:ok, value}
      {:ok, _} -> {:error, :invalid_counter_value}
      {:error, reason} -> {:error, reason}
    end
  end

  defp fetch_manifest_hash(repo, manifest_version_id) do
    sql = "SELECT content_hash FROM favn_manifest_versions WHERE manifest_version_id = $1 LIMIT 1"

    case SQL.query(repo, sql, [manifest_version_id]) do
      {:ok, %{rows: [[hash]]}} -> {:ok, hash}
      {:ok, %{rows: []}} -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  defp fetch_manifest_record(repo, manifest_version_id) do
    sql =
      """
      SELECT manifest_version_id, content_hash, schema_version, runner_contract_version, serialization_format, manifest_json, inserted_at
      FROM favn_manifest_versions
      WHERE manifest_version_id = $1
      LIMIT 1
      """

    case SQL.query(repo, sql, [manifest_version_id]) do
      {:ok, %{rows: [[id, hash, schema, runner_contract, format, manifest_json, inserted_at]]}} ->
        {:ok,
         %{
           manifest_version_id: id,
           content_hash: hash,
           schema_version: schema,
           runner_contract_version: runner_contract,
           serialization_format: format,
           manifest_json: manifest_json,
           inserted_at: inserted_at
         }}

      {:ok, %{rows: []}} ->
        {:ok, nil}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp fetch_manifest_records(repo) do
    sql =
      """
      SELECT manifest_version_id, content_hash, schema_version, runner_contract_version, serialization_format, manifest_json, inserted_at
      FROM favn_manifest_versions
      ORDER BY manifest_version_id ASC
      """

    case SQL.query(repo, sql, []) do
      {:ok, %{rows: rows}} ->
        mapped =
          Enum.map(rows, fn [
                              id,
                              hash,
                              schema,
                              runner_contract,
                              format,
                              manifest_json,
                              inserted_at
                            ] ->
            %{
              manifest_version_id: id,
              content_hash: hash,
              schema_version: schema,
              runner_contract_version: runner_contract,
              serialization_format: format,
              manifest_json: manifest_json,
              inserted_at: inserted_at
            }
          end)

        {:ok, mapped}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp fetch_run_head(repo, run_id) do
    sql = "SELECT event_seq, snapshot_hash FROM favn_runs WHERE run_id = $1 LIMIT 1"

    case SQL.query(repo, sql, [run_id]) do
      {:ok, %{rows: [[event_seq, snapshot_hash]]}} -> {:ok, {event_seq, snapshot_hash}}
      {:ok, %{rows: []}} -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  defp fetch_event_by_sequence(repo, run_id, sequence) do
    sql =
      "SELECT global_sequence, event_blob FROM favn_run_events WHERE run_id = $1 AND sequence = $2 LIMIT 1"

    case SQL.query(repo, sql, [run_id, sequence]) do
      {:ok, %{rows: [[global_sequence, blob]]}} ->
        decode_event_row(global_sequence, blob)

      {:ok, %{rows: []}} ->
        {:ok, nil}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp global_run_events_query(repo, opts) do
    after_sequence = Keyword.get(opts, :after_global_sequence)
    limit = Keyword.get(opts, :limit, 200)

    cond do
      not (is_integer(limit) and limit > 0) ->
        {:error, :cursor_invalid}

      is_nil(after_sequence) ->
        {:ok,
         """
         SELECT global_sequence, event_blob
         FROM (
           SELECT global_sequence, event_blob
           FROM favn_run_events
           WHERE global_sequence IS NOT NULL
           ORDER BY global_sequence DESC
           LIMIT $1
         ) recent
         ORDER BY global_sequence ASC
         """, [limit]}

      is_integer(after_sequence) and after_sequence == 0 ->
        {:ok,
         """
         SELECT global_sequence, event_blob
         FROM favn_run_events
         WHERE global_sequence IS NOT NULL
         ORDER BY global_sequence ASC
         LIMIT $1
         """, [limit]}

      is_integer(after_sequence) and after_sequence > 0 ->
        case global_sequence_exists?(repo, after_sequence) do
          true ->
            {:ok,
             """
             SELECT global_sequence, event_blob
             FROM favn_run_events
             WHERE global_sequence > $1
             ORDER BY global_sequence ASC
             LIMIT $2
             """, [after_sequence, limit]}

          false ->
            {:error, :cursor_invalid}

          {:error, reason} ->
            {:error, reason}
        end

      true ->
        {:error, :cursor_invalid}
    end
  end

  defp global_sequence_exists?(repo, sequence) do
    case SQL.query(repo, "SELECT 1 FROM favn_run_events WHERE global_sequence = $1 LIMIT 1", [
           sequence
         ]) do
      {:ok, %{rows: [[1]]}} -> true
      {:ok, %{rows: []}} -> false
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_event_rows(rows) do
    Enum.reduce_while(rows, {:ok, []}, fn [global_sequence, payload], {:ok, acc} ->
      case decode_event_row(global_sequence, payload) do
        {:ok, event} -> {:cont, {:ok, [event | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp decode_event_row(global_sequence, payload) do
    case decode_payload(payload) do
      {:ok, event} when is_map(event) -> {:ok, Map.put(event, :global_sequence, global_sequence)}
      {:ok, other} -> {:error, {:invalid_event_payload, other}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp fetch_scheduler_version(repo, {pipeline_module, schedule_id}) do
    sql =
      """
      SELECT version
      FROM favn_scheduler_cursors
      WHERE pipeline_module = $1 AND schedule_id = $2
      LIMIT 1
      """

    params = [Atom.to_string(pipeline_module), encode_schedule_id(schedule_id)]

    case SQL.query(repo, sql, params) do
      {:ok, %{rows: [[version]]}} when is_integer(version) -> {:ok, version}
      {:ok, %{rows: []}} -> {:ok, nil}
      {:ok, _} -> {:error, :invalid_scheduler_version}
      {:error, reason} -> {:error, reason}
    end
  end

  defp resolve_scheduler_version(nil, 1), do: {:ok, 1}
  defp resolve_scheduler_version(nil, _incoming), do: {:error, :invalid_scheduler_version}

  defp resolve_scheduler_version(existing, incoming)
       when is_integer(existing) and is_integer(incoming) and incoming == existing + 1,
       do: {:ok, incoming}

  defp resolve_scheduler_version(_existing, _incoming), do: {:error, :stale_scheduler_state}

  defp insert_manifest_record(repo, record) do
    sql =
      """
      INSERT INTO favn_manifest_versions (
        manifest_version_id,
        content_hash,
        schema_version,
        runner_contract_version,
        serialization_format,
        manifest_json,
        inserted_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7)
      """

    params = [
      record.manifest_version_id,
      record.content_hash,
      record.schema_version,
      record.runner_contract_version,
      record.serialization_format,
      record.manifest_json,
      record.inserted_at
    ]

    case SQL.query(repo, sql, params) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp list_runs_query(run_opts) do
    status = Keyword.get(run_opts, :status)
    limit = Keyword.get(run_opts, :limit)

    cond do
      is_nil(status) and is_nil(limit) ->
        {run_snapshot_select() <> " ORDER BY r.updated_seq DESC, r.run_id DESC", []}

      is_nil(status) ->
        {run_snapshot_select() <> " ORDER BY r.updated_seq DESC, r.run_id DESC LIMIT $1", [limit]}

      is_nil(limit) ->
        {
          run_snapshot_select() <>
            " WHERE r.status = $1 ORDER BY r.updated_seq DESC, r.run_id DESC",
          [Atom.to_string(status)]
        }

      true ->
        {
          run_snapshot_select() <>
            " WHERE r.status = $1 ORDER BY r.updated_seq DESC, r.run_id DESC LIMIT $2",
          [Atom.to_string(status), limit]
        }
    end
  end

  defp run_snapshot_select do
    """
    SELECT r.run_blob, r.manifest_version_id, m.manifest_version_id, m.content_hash, m.schema_version, m.runner_contract_version, m.serialization_format, m.manifest_json, m.inserted_at
    FROM favn_runs AS r
    LEFT JOIN favn_manifest_versions AS m ON m.manifest_version_id = r.manifest_version_id
    """
  end

  defp decode_run_row([
         run_blob,
         run_manifest_version_id,
         manifest_version_id,
         content_hash,
         schema_version,
         runner_contract_version,
         serialization_format,
         manifest_json,
         inserted_at
       ]) do
    run_record = %{run_blob: run_blob, manifest_version_id: run_manifest_version_id}

    manifest_record =
      manifest_record(
        manifest_version_id,
        content_hash,
        schema_version,
        runner_contract_version,
        serialization_format,
        manifest_json,
        inserted_at
      )

    RunSnapshotCodec.decode_run(run_record, manifest_record)
  end

  defp manifest_record(
         nil,
         _hash,
         _schema,
         _runner_contract,
         _format,
         _manifest_json,
         _inserted_at
       ),
       do: nil

  defp manifest_record(
         manifest_version_id,
         content_hash,
         schema_version,
         runner_contract_version,
         serialization_format,
         manifest_json,
         inserted_at
       ) do
    %{
      manifest_version_id: manifest_version_id,
      content_hash: content_hash,
      schema_version: schema_version,
      runner_contract_version: runner_contract_version,
      serialization_format: serialization_format,
      manifest_json: manifest_json,
      inserted_at: inserted_at
    }
  end

  defp validate_transition_alignment(%RunState{} = run, event) when is_map(event) do
    cond do
      Map.get(event, :run_id) != run.id -> {:error, :invalid_run_event_run_id}
      Map.get(event, :sequence) != run.event_seq -> {:error, :invalid_run_event_sequence}
      true -> :ok
    end
  end

  defp encode_payload(value) do
    case PayloadCodec.encode(value) do
      {:ok, payload} -> payload
      {:error, reason} -> raise ArgumentError, "invalid storage payload: #{inspect(reason)}"
    end
  end

  defp decode_payload(payload) when is_binary(payload), do: PayloadCodec.decode(payload)

  defp auth_persistence_not_supported, do: {:error, :auth_persistence_not_supported}

  defp existing_atom(value) when is_binary(value) do
    {:ok, String.to_existing_atom(value)}
  rescue
    ArgumentError -> {:error, {:unknown_atom, value}}
  end

  defp resolve_repo(opts) do
    with {:ok, normalized} <- normalize_opts(opts) do
      case Keyword.fetch!(normalized, :repo_mode) do
        :managed ->
          {:ok, Repo}

        :external ->
          repo = Keyword.fetch!(normalized, :repo)

          case schema_ready?(repo) do
            true -> {:ok, repo}
            false -> {:error, :schema_not_ready}
          end
      end
    end
  end

  defp schema_ready?(repo) when is_atom(repo) do
    cache_key = {@schema_ready_cache_key, repo}

    case :persistent_term.get(cache_key, :unknown) do
      true ->
        true

      :unknown ->
        if Migrations.schema_ready?(repo) do
          :persistent_term.put(cache_key, true)
          true
        else
          false
        end
    end
  end

  defp normalize_opts(opts) do
    repo_mode = Keyword.get(opts, :repo_mode, :managed)

    case repo_mode do
      :managed ->
        with {:ok, repo_config} <- validate_repo_config(Keyword.get(opts, :repo_config, [])),
             {:ok, migration_mode} <-
               validate_migration_mode(Keyword.get(opts, :migration_mode, :manual)) do
          {:ok,
           [
             repo_mode: :managed,
             repo_config: repo_config,
             migration_mode: migration_mode,
             supervisor_name: Keyword.get(opts, :supervisor_name, FavnStoragePostgres.Supervisor)
           ]}
        end

      :external ->
        with {:ok, repo} <- validate_external_repo(Keyword.get(opts, :repo)),
             :ok <- validate_external_migration_mode(Keyword.get(opts, :migration_mode, :manual)) do
          {:ok, [repo_mode: :external, repo: repo, migration_mode: :manual]}
        end

      other ->
        {:error, {:invalid_repo_mode, other}}
    end
  end

  defp validate_repo_config(repo_config) when is_list(repo_config) do
    required = [:hostname, :database, :username, :password]

    case Enum.find(required, fn key ->
           value = Keyword.get(repo_config, key)
           not (is_binary(value) and value != "")
         end) do
      nil -> {:ok, repo_config}
      missing -> {:error, {:invalid_repo_config, missing}}
    end
  end

  defp validate_repo_config(_other), do: {:error, {:invalid_repo_config, :not_keyword}}

  defp validate_migration_mode(:manual), do: {:ok, :manual}
  defp validate_migration_mode(:auto), do: {:ok, :auto}
  defp validate_migration_mode(other), do: {:error, {:invalid_migration_mode, other}}

  defp validate_external_migration_mode(:manual), do: :ok

  defp validate_external_migration_mode(other),
    do: {:error, {:invalid_external_migration_mode, other}}

  defp validate_external_repo(repo) when is_atom(repo) do
    with {:module, ^repo} <- Code.ensure_loaded(repo),
         true <- function_exported?(repo, :__adapter__, 0),
         Ecto.Adapters.Postgres <- repo.__adapter__() do
      {:ok, repo}
    else
      _ -> {:error, {:invalid_external_repo, repo}}
    end
  end

  defp validate_external_repo(other), do: {:error, {:invalid_external_repo, other}}

  defp encode_schedule_id(nil), do: @nil_schedule_id
  defp encode_schedule_id(schedule_id) when is_atom(schedule_id), do: Atom.to_string(schedule_id)
end
