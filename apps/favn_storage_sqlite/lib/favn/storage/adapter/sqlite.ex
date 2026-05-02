defmodule Favn.Storage.Adapter.SQLite do
  @moduledoc """
  SQLite-backed storage adapter implementing `Favn.Storage.Adapter`.
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
  alias FavnOrchestrator.Storage.RunStateCodec
  alias FavnOrchestrator.Storage.SchedulerStateCodec
  alias FavnOrchestrator.Storage.WriteSemantics
  alias FavnStorageSqlite.Diagnostics
  alias FavnStorageSqlite.Migrations
  alias FavnStorageSqlite.Repo
  alias FavnStorageSqlite.Supervisor, as: SQLiteSupervisor

  @active_manifest_key "active_manifest_version_id"
  @write_counter_key "run_write_order"
  @nil_schedule_id "__nil__"

  @impl true
  def child_spec(opts) when is_list(opts) do
    with {:ok, normalized} <- normalize_opts(opts),
         :ok <- Diagnostics.validate_database_path(normalized) do
      supervisor_name = Keyword.fetch!(normalized, :supervisor_name)

      if Process.whereis(supervisor_name) do
        :none
      else
        {:ok,
         Supervisor.child_spec(
           {SQLiteSupervisor, normalized},
           id: supervisor_name,
           restart: :permanent,
           shutdown: 5_000,
           type: :supervisor
         )}
      end
    end
  end

  @impl true
  def readiness(opts) when is_list(opts) do
    Diagnostics.readiness(opts)
  end

  @impl true
  def diagnostics(opts) when is_list(opts) do
    Diagnostics.readiness(opts)
  end

  @impl true
  def put_manifest_version(%Version{} = version, opts) when is_list(opts) do
    with {:ok, repo} <- repo_name(opts),
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
    with {:ok, repo} <- repo_name(opts),
         {:ok, row} <- fetch_manifest_record(repo, manifest_version_id) do
      case row do
        nil -> {:error, :manifest_version_not_found}
        record -> ManifestCodec.from_record(record)
      end
    end
  end

  @impl true
  def list_manifest_versions(opts) when is_list(opts) do
    with {:ok, repo} <- repo_name(opts),
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
    with {:ok, repo} <- repo_name(opts),
         {:ok, existing} <- fetch_manifest_hash(repo, manifest_version_id),
         false <- is_nil(existing) do
      sql =
        """
        INSERT INTO favn_runtime_settings (key, value_text, updated_at)
        VALUES (?1, ?2, ?3)
        ON CONFLICT(key) DO UPDATE SET value_text = excluded.value_text, updated_at = excluded.updated_at
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
    with {:ok, repo} <- repo_name(opts) do
      sql = "SELECT value_text FROM favn_runtime_settings WHERE key = ?1 LIMIT 1"

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
    with {:ok, repo} <- repo_name(opts),
         {:ok, normalized} <- RunStateCodec.normalize(run) do
      persist_run(repo, normalized)
    end
  end

  @impl true
  def persist_run_transition(%RunState{} = run, event, opts)
      when is_map(event) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts),
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
    with {:ok, repo} <- repo_name(opts) do
      sql = "SELECT run_blob FROM favn_runs WHERE run_id = ?1 LIMIT 1"

      case SQL.query(repo, sql, [run_id]) do
        {:ok, %{rows: [[payload]]}} -> decode_run(payload)
        {:ok, %{rows: []}} -> {:error, :not_found}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def list_runs(run_opts, opts) when is_list(run_opts) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      {sql, params} = list_runs_query(run_opts)

      case SQL.query(repo, sql, params) do
        {:ok, %{rows: rows}} ->
          rows
          |> Enum.reduce_while({:ok, []}, fn [payload], {:ok, acc} ->
            case decode_run(payload) do
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
    with {:ok, repo} <- repo_name(opts),
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
    with {:ok, repo} <- repo_name(opts) do
      sql = "SELECT event_blob FROM favn_run_events WHERE run_id = ?1 ORDER BY sequence ASC"

      case SQL.query(repo, sql, [run_id]) do
        {:ok, %{rows: rows}} ->
          rows
          |> Enum.reduce_while({:ok, []}, fn [payload], {:ok, acc} ->
            case decode_payload(payload) do
              {:ok, event} when is_map(event) -> {:cont, {:ok, [event | acc]}}
              {:ok, other} -> {:halt, {:error, {:invalid_event_payload, other}}}
              {:error, reason} -> {:halt, {:error, reason}}
            end
          end)
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
    with {:ok, repo} <- repo_name(opts),
         {:ok, normalized_key} <- SchedulerStateCodec.normalize_key(key),
         {:ok, normalized_state} <- SchedulerStateCodec.normalize_state(state) do
      persist_scheduler_state(repo, normalized_key, normalized_state)
    end
  end

  @impl true
  def get_scheduler_state(key, opts) when is_list(opts) do
    with {:ok, repo} <- repo_name(opts),
         {:ok, {pipeline_module, schedule_id}} <- SchedulerStateCodec.normalize_key(key) do
      sql =
        """
        SELECT version, state_blob
        FROM favn_scheduler_cursors
        WHERE pipeline_module = ?1 AND schedule_id = ?2
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
    with {:ok, repo} <- repo_name(opts) do
      sql =
        """
        INSERT INTO favn_pipeline_coverage_baselines (
          baseline_id, pipeline_module, source_key, segment_key_hash, segment_key_redacted,
          window_kind, timezone, coverage_start_at, coverage_until, created_by_run_id,
          manifest_version_id, status, errors_blob, metadata_blob, created_at, updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)
        ON CONFLICT(baseline_id) DO UPDATE SET
          pipeline_module = excluded.pipeline_module,
          source_key = excluded.source_key,
          segment_key_hash = excluded.segment_key_hash,
          segment_key_redacted = excluded.segment_key_redacted,
          window_kind = excluded.window_kind,
          timezone = excluded.timezone,
          coverage_start_at = excluded.coverage_start_at,
          coverage_until = excluded.coverage_until,
          created_by_run_id = excluded.created_by_run_id,
          manifest_version_id = excluded.manifest_version_id,
          status = excluded.status,
          errors_blob = excluded.errors_blob,
          metadata_blob = excluded.metadata_blob,
          created_at = excluded.created_at,
          updated_at = excluded.updated_at
        """

      params = [
        baseline.baseline_id,
        encode_atom(baseline.pipeline_module),
        baseline.source_key,
        baseline.segment_key_hash,
        baseline.segment_key_redacted,
        encode_atom(baseline.window_kind),
        baseline.timezone,
        encode_datetime(baseline.coverage_start_at),
        encode_datetime(baseline.coverage_until),
        baseline.created_by_run_id,
        baseline.manifest_version_id,
        encode_atom(baseline.status),
        encode_payload(baseline.errors),
        encode_payload(baseline.metadata),
        encode_datetime(baseline.created_at),
        encode_datetime(baseline.updated_at)
      ]

      case SQL.query(repo, sql, params) do
        {:ok, _} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def get_coverage_baseline(baseline_id, opts) when is_binary(baseline_id) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      sql =
        "SELECT #{coverage_baseline_columns()} FROM favn_pipeline_coverage_baselines WHERE baseline_id = ?1 LIMIT 1"

      case SQL.query(repo, sql, [baseline_id]) do
        {:ok, %{rows: [row]}} -> decode_coverage_baseline_row(row)
        {:ok, %{rows: []}} -> {:error, :not_found}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def list_coverage_baselines(filters, opts) when is_list(filters) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts),
         {:ok, page_opts} <- page_opts(filters),
         {:ok, {where_sql, params}} <-
           build_filter_sql(read_filters(filters), coverage_baseline_filter_columns()) do
      sql =
        "SELECT #{coverage_baseline_columns()} FROM favn_pipeline_coverage_baselines#{where_sql} ORDER BY updated_at DESC, baseline_id ASC LIMIT ?#{length(params) + 1} OFFSET ?#{length(params) + 2}"

      decode_page(repo, sql, params, page_opts, &decode_coverage_baseline_row/1)
    end
  end

  @impl true
  def put_backfill_window(%BackfillWindow{} = window, opts) when is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      sql =
        """
        INSERT INTO favn_backfill_windows (
          backfill_run_id, child_run_id, pipeline_module, manifest_version_id,
          coverage_baseline_id, window_kind, window_start_at, window_end_at, timezone,
          window_key, status, attempt_count, latest_attempt_run_id, last_success_run_id,
          last_error_blob, errors_blob, metadata_blob, started_at, finished_at, created_at, updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21)
        ON CONFLICT(backfill_run_id, pipeline_module, window_key) DO UPDATE SET
          child_run_id = excluded.child_run_id,
          manifest_version_id = excluded.manifest_version_id,
          coverage_baseline_id = excluded.coverage_baseline_id,
          window_kind = excluded.window_kind,
          window_start_at = excluded.window_start_at,
          window_end_at = excluded.window_end_at,
          timezone = excluded.timezone,
          status = excluded.status,
          attempt_count = excluded.attempt_count,
          latest_attempt_run_id = excluded.latest_attempt_run_id,
          last_success_run_id = excluded.last_success_run_id,
          last_error_blob = excluded.last_error_blob,
          errors_blob = excluded.errors_blob,
          metadata_blob = excluded.metadata_blob,
          started_at = excluded.started_at,
          finished_at = excluded.finished_at,
          created_at = excluded.created_at,
          updated_at = excluded.updated_at
        """

      params = [
        window.backfill_run_id,
        window.child_run_id,
        encode_atom(window.pipeline_module),
        window.manifest_version_id,
        window.coverage_baseline_id,
        encode_atom(window.window_kind),
        encode_datetime(window.window_start_at),
        encode_datetime(window.window_end_at),
        window.timezone,
        window.window_key,
        encode_atom(window.status),
        window.attempt_count,
        window.latest_attempt_run_id,
        window.last_success_run_id,
        encode_payload(window.last_error),
        encode_payload(window.errors),
        encode_payload(window.metadata),
        encode_datetime(window.started_at),
        encode_datetime(window.finished_at),
        encode_datetime(window.created_at),
        encode_datetime(window.updated_at)
      ]

      case SQL.query(repo, sql, params) do
        {:ok, _} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def get_backfill_window(backfill_run_id, pipeline_module, window_key, opts)
      when is_binary(backfill_run_id) and is_atom(pipeline_module) and is_binary(window_key) and
             is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      sql =
        "SELECT #{backfill_window_columns()} FROM favn_backfill_windows WHERE backfill_run_id = ?1 AND pipeline_module = ?2 AND window_key = ?3 LIMIT 1"

      params = [backfill_run_id, encode_atom(pipeline_module), window_key]

      case SQL.query(repo, sql, params) do
        {:ok, %{rows: [row]}} -> decode_backfill_window_row(row)
        {:ok, %{rows: []}} -> {:error, :not_found}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def list_backfill_windows(filters, opts) when is_list(filters) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts),
         {:ok, page_opts} <- page_opts(filters),
         {:ok, {where_sql, params}} <-
           build_filter_sql(read_filters(filters), backfill_window_filter_columns()) do
      sql =
        "SELECT #{backfill_window_columns()} FROM favn_backfill_windows#{where_sql} ORDER BY window_start_at ASC, backfill_run_id ASC, pipeline_module ASC, window_key ASC LIMIT ?#{length(params) + 1} OFFSET ?#{length(params) + 2}"

      decode_page(repo, sql, params, page_opts, &decode_backfill_window_row/1)
    end
  end

  @impl true
  def put_asset_window_state(%AssetWindowState{} = state, opts) when is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      sql =
        """
        INSERT INTO favn_asset_window_states (
          asset_ref_module, asset_ref_name, pipeline_module, manifest_version_id,
          window_kind, window_start_at, window_end_at, timezone, window_key, status,
          latest_run_id, latest_parent_run_id, latest_success_run_id, latest_error_blob,
          rows_written, errors_blob, metadata_blob, updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)
        ON CONFLICT(asset_ref_module, asset_ref_name, window_key) DO UPDATE SET
          pipeline_module = excluded.pipeline_module,
          manifest_version_id = excluded.manifest_version_id,
          window_kind = excluded.window_kind,
          window_start_at = excluded.window_start_at,
          window_end_at = excluded.window_end_at,
          timezone = excluded.timezone,
          status = excluded.status,
          latest_run_id = excluded.latest_run_id,
          latest_parent_run_id = excluded.latest_parent_run_id,
          latest_success_run_id = excluded.latest_success_run_id,
          latest_error_blob = excluded.latest_error_blob,
          rows_written = excluded.rows_written,
          errors_blob = excluded.errors_blob,
          metadata_blob = excluded.metadata_blob,
          updated_at = excluded.updated_at
        """

      params = [
        encode_atom(state.asset_ref_module),
        encode_atom(state.asset_ref_name),
        encode_atom(state.pipeline_module),
        state.manifest_version_id,
        encode_atom(state.window_kind),
        encode_datetime(state.window_start_at),
        encode_datetime(state.window_end_at),
        state.timezone,
        state.window_key,
        encode_atom(state.status),
        state.latest_run_id,
        state.latest_parent_run_id,
        state.latest_success_run_id,
        encode_payload(state.latest_error),
        state.rows_written,
        encode_payload(state.errors),
        encode_payload(state.metadata),
        encode_datetime(state.updated_at)
      ]

      case SQL.query(repo, sql, params) do
        {:ok, _} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def get_asset_window_state(asset_ref_module, asset_ref_name, window_key, opts)
      when is_atom(asset_ref_module) and is_atom(asset_ref_name) and is_binary(window_key) and
             is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      sql =
        "SELECT #{asset_window_state_columns()} FROM favn_asset_window_states WHERE asset_ref_module = ?1 AND asset_ref_name = ?2 AND window_key = ?3 LIMIT 1"

      params = [encode_atom(asset_ref_module), encode_atom(asset_ref_name), window_key]

      case SQL.query(repo, sql, params) do
        {:ok, %{rows: [row]}} -> decode_asset_window_state_row(row)
        {:ok, %{rows: []}} -> {:error, :not_found}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def list_asset_window_states(filters, opts) when is_list(filters) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts),
         {:ok, page_opts} <- page_opts(filters),
         {:ok, {where_sql, params}} <-
           build_filter_sql(read_filters(filters), asset_window_state_filter_columns()) do
      sql =
        "SELECT #{asset_window_state_columns()} FROM favn_asset_window_states#{where_sql} ORDER BY updated_at DESC, asset_ref_module ASC, asset_ref_name ASC, window_key ASC LIMIT ?#{length(params) + 1} OFFSET ?#{length(params) + 2}"

      decode_page(repo, sql, params, page_opts, &decode_asset_window_state_row/1)
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
    with {:ok, repo} <- repo_name(opts) do
      repo.transact(fn ->
        with :ok <-
               delete_scoped(
                 repo,
                 "favn_pipeline_coverage_baselines",
                 scope,
                 coverage_baseline_filter_columns()
               ),
             :ok <-
               delete_scoped(
                 repo,
                 "favn_backfill_windows",
                 scope,
                 backfill_window_filter_columns()
               ),
             :ok <-
               delete_scoped(
                 repo,
                 "favn_asset_window_states",
                 scope,
                 asset_window_state_filter_columns()
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

  defp coverage_baseline_columns do
    "baseline_id, pipeline_module, source_key, segment_key_hash, segment_key_redacted, window_kind, timezone, coverage_start_at, coverage_until, created_by_run_id, manifest_version_id, status, errors_blob, metadata_blob, created_at, updated_at"
  end

  defp backfill_window_columns do
    "backfill_run_id, child_run_id, pipeline_module, manifest_version_id, coverage_baseline_id, window_kind, window_start_at, window_end_at, timezone, window_key, status, attempt_count, latest_attempt_run_id, last_success_run_id, last_error_blob, errors_blob, metadata_blob, started_at, finished_at, created_at, updated_at"
  end

  defp asset_window_state_columns do
    "asset_ref_module, asset_ref_name, pipeline_module, manifest_version_id, window_kind, window_start_at, window_end_at, timezone, window_key, status, latest_run_id, latest_parent_run_id, latest_success_run_id, latest_error_blob, rows_written, errors_blob, metadata_blob, updated_at"
  end

  defp coverage_baseline_filter_columns do
    %{
      baseline_id: {:text, "baseline_id"},
      pipeline_module: {:atom, "pipeline_module"},
      source_key: {:text, "source_key"},
      segment_key_hash: {:text, "segment_key_hash"},
      window_kind: {:atom, "window_kind"},
      timezone: {:text, "timezone"},
      created_by_run_id: {:text, "created_by_run_id"},
      manifest_version_id: {:text, "manifest_version_id"},
      status: {:atom, "status"}
    }
  end

  defp backfill_window_filter_columns do
    %{
      backfill_run_id: {:text, "backfill_run_id"},
      child_run_id: {:text, "child_run_id"},
      pipeline_module: {:atom, "pipeline_module"},
      manifest_version_id: {:text, "manifest_version_id"},
      coverage_baseline_id: {:text, "coverage_baseline_id"},
      window_kind: {:atom, "window_kind"},
      timezone: {:text, "timezone"},
      window_key: {:text, "window_key"},
      status: {:atom, "status"},
      latest_attempt_run_id: {:text, "latest_attempt_run_id"},
      last_success_run_id: {:text, "last_success_run_id"}
    }
  end

  defp asset_window_state_filter_columns do
    %{
      asset_ref_module: {:atom, "asset_ref_module"},
      asset_ref_name: {:atom, "asset_ref_name"},
      pipeline_module: {:atom, "pipeline_module"},
      manifest_version_id: {:text, "manifest_version_id"},
      window_kind: {:atom, "window_kind"},
      timezone: {:text, "timezone"},
      window_key: {:text, "window_key"},
      status: {:atom, "status"},
      latest_run_id: {:text, "latest_run_id"},
      latest_parent_run_id: {:text, "latest_parent_run_id"},
      latest_success_run_id: {:text, "latest_success_run_id"}
    }
  end

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
         errors_blob,
         metadata_blob,
         created_at,
         updated_at
       ]) do
    with {:ok, pipeline_module} <- existing_atom(pipeline_module),
         {:ok, errors} <- decode_payload(errors_blob),
         {:ok, metadata} <- decode_payload(metadata_blob) do
      CoverageBaseline.new(%{
        baseline_id: baseline_id,
        pipeline_module: pipeline_module,
        source_key: source_key,
        segment_key_hash: segment_key_hash,
        segment_key_redacted: segment_key_redacted,
        window_kind: window_kind,
        timezone: timezone,
        coverage_start_at: decode_datetime(coverage_start_at),
        coverage_until: decode_datetime(coverage_until),
        created_by_run_id: created_by_run_id,
        manifest_version_id: manifest_version_id,
        status: status,
        errors: errors,
        metadata: metadata,
        created_at: decode_datetime(created_at),
        updated_at: decode_datetime(updated_at)
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
         last_error_blob,
         errors_blob,
         metadata_blob,
         started_at,
         finished_at,
         created_at,
         updated_at
       ]) do
    with {:ok, pipeline_module} <- existing_atom(pipeline_module),
         {:ok, last_error} <- decode_payload(last_error_blob),
         {:ok, errors} <- decode_payload(errors_blob),
         {:ok, metadata} <- decode_payload(metadata_blob) do
      BackfillWindow.new(%{
        backfill_run_id: backfill_run_id,
        child_run_id: child_run_id,
        pipeline_module: pipeline_module,
        manifest_version_id: manifest_version_id,
        coverage_baseline_id: coverage_baseline_id,
        window_kind: window_kind,
        window_start_at: decode_datetime(window_start_at),
        window_end_at: decode_datetime(window_end_at),
        timezone: timezone,
        window_key: window_key,
        status: status,
        attempt_count: attempt_count,
        latest_attempt_run_id: latest_attempt_run_id,
        last_success_run_id: last_success_run_id,
        last_error: last_error,
        errors: errors,
        metadata: metadata,
        started_at: decode_datetime(started_at),
        finished_at: decode_datetime(finished_at),
        created_at: decode_datetime(created_at),
        updated_at: decode_datetime(updated_at)
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
         latest_error_blob,
         rows_written,
         errors_blob,
         metadata_blob,
         updated_at
       ]) do
    with {:ok, asset_ref_module} <- existing_atom(asset_ref_module),
         {:ok, asset_ref_name} <- existing_atom(asset_ref_name),
         {:ok, pipeline_module} <- existing_atom(pipeline_module),
         {:ok, latest_error} <- decode_payload(latest_error_blob),
         {:ok, errors} <- decode_payload(errors_blob),
         {:ok, metadata} <- decode_payload(metadata_blob) do
      AssetWindowState.new(%{
        asset_ref_module: asset_ref_module,
        asset_ref_name: asset_ref_name,
        pipeline_module: pipeline_module,
        manifest_version_id: manifest_version_id,
        window_kind: window_kind,
        window_start_at: decode_datetime(window_start_at),
        window_end_at: decode_datetime(window_end_at),
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
        updated_at: decode_datetime(updated_at)
      })
    end
  end

  defp decode_rows(repo, sql, params, decoder) when is_function(decoder, 1) do
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

  defp decode_page(repo, sql, params, page_opts, decoder) when is_function(decoder, 1) do
    params = params ++ [Keyword.fetch!(page_opts, :limit) + 1, Keyword.fetch!(page_opts, :offset)]

    with {:ok, rows} <- decode_rows(repo, sql, params, decoder) do
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

  defp delete_scoped(repo, table, [], _columns) do
    case SQL.query(repo, "DELETE FROM #{table}", []) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp delete_scoped(repo, table, scope, columns) do
    filters = Enum.filter(scope, fn {key, _value} -> Map.has_key?(columns, key) end)

    if filters == [] do
      :ok
    else
      with {:ok, {where_sql, params}} <- build_filter_sql(filters, columns) do
        case SQL.query(repo, "DELETE FROM #{table}#{where_sql}", params) do
          {:ok, _} -> :ok
          {:error, reason} -> {:error, reason}
        end
      end
    end
  end

  defp page_opts(filters), do: Page.normalize_opts(filters)

  defp build_filter_sql([], _columns), do: {:ok, {"", []}}

  defp build_filter_sql(filters, columns) do
    filters
    |> Enum.reduce_while({:ok, [], []}, fn {key, value}, {:ok, clauses, params} ->
      case Map.fetch(columns, key) do
        {:ok, {type, column}} ->
          placeholder = "?#{length(params) + 1}"
          clause = "#{column} = #{placeholder}"
          {:cont, {:ok, [clause | clauses], params ++ [encode_filter_value(type, value)]}}

        :error ->
          {:halt, {:error, {:unsupported_filter, key}}}
      end
    end)
    |> case do
      {:ok, clauses, params} ->
        {:ok, {" WHERE " <> Enum.join(Enum.reverse(clauses), " AND "), params}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp encode_filter_value(:atom, value) when is_atom(value), do: encode_atom(value)
  defp encode_filter_value(:atom, value) when is_binary(value), do: value
  defp encode_filter_value(:text, value), do: value

  defp encode_atom(value) when is_atom(value), do: Atom.to_string(value)

  defp existing_atom(value) when is_binary(value) do
    {:ok, String.to_existing_atom(value)}
  rescue
    ArgumentError -> {:error, {:unknown_atom, value}}
  end

  defp encode_datetime(nil), do: nil
  defp encode_datetime(%DateTime{} = value), do: DateTime.to_iso8601(value)

  defp decode_datetime(nil), do: nil

  defp decode_datetime(%DateTime{} = value), do: value

  defp decode_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> datetime
      {:error, _reason} -> value
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
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
        ON CONFLICT(run_id) DO UPDATE SET
          manifest_version_id = excluded.manifest_version_id,
          manifest_content_hash = excluded.manifest_content_hash,
          status = excluded.status,
          event_seq = excluded.event_seq,
          snapshot_hash = excluded.snapshot_hash,
          updated_seq = excluded.updated_seq,
          inserted_at = excluded.inserted_at,
          updated_at = excluded.updated_at,
          run_blob = excluded.run_blob
        WHERE excluded.event_seq > favn_runs.event_seq
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
    sql =
      """
      INSERT INTO favn_run_events (run_id, sequence, occurred_at, event_blob)
      VALUES (?1, ?2, ?3, ?4)
      """

    case SQL.query(repo, sql, [run_id, event.sequence, event.occurred_at, encode_payload(event)]) do
      {:ok, _} ->
        :ok

      {:error, %{sqlite: %{code: :constraint_failed}}} ->
        resolve_existing_event_conflict(repo, run_id, event)

      {:error, %Exqlite.Error{message: message} = reason} when is_binary(message) ->
        if String.contains?(message, "UNIQUE constraint failed") do
          resolve_existing_event_conflict(repo, run_id, event)
        else
          {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
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
        pipeline_module,
        encoded_schedule_id,
        normalized_state,
        updated_at,
        encoded_state
      )

    case SQL.query(repo, sql, params) do
      {:ok, %{num_rows: num_rows}} when num_rows > 0 ->
        :ok

      {:ok, _} ->
        classify_scheduler_write_result(repo, key, normalized_state[:version])

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp scheduler_upsert_query(
         pipeline_module,
         encoded_schedule_id,
         normalized_state,
         updated_at,
         encoded_state
       ) do
    pipeline_module_name = Atom.to_string(pipeline_module)

    incoming_version = Map.fetch!(normalized_state, :version)
    expected_previous = incoming_version - 1

    {
      """
      INSERT INTO favn_scheduler_cursors (pipeline_module, schedule_id, version, updated_at, state_blob)
      SELECT ?1, ?2, ?3, ?4, ?5
      WHERE ?3 = 1 OR EXISTS (
        SELECT 1
        FROM favn_scheduler_cursors
        WHERE pipeline_module = ?1 AND schedule_id = ?2
      )
      ON CONFLICT(pipeline_module, schedule_id) DO UPDATE SET
        version = excluded.version,
        updated_at = excluded.updated_at,
        state_blob = excluded.state_blob
      WHERE favn_scheduler_cursors.version = ?6
      """,
      [
        pipeline_module_name,
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
    sql =
      """
      INSERT INTO favn_counters (name, value)
      VALUES (?1, 1)
      ON CONFLICT(name) DO UPDATE SET value = value + 1
      RETURNING value
      """

    case SQL.query(repo, sql, [@write_counter_key]) do
      {:ok, %{rows: [[value]]}} when is_integer(value) -> {:ok, value}
      {:ok, _} -> {:error, :invalid_counter_value}
      {:error, reason} -> {:error, reason}
    end
  end

  defp fetch_manifest_hash(repo, manifest_version_id) do
    sql = "SELECT content_hash FROM favn_manifest_versions WHERE manifest_version_id = ?1 LIMIT 1"

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
      WHERE manifest_version_id = ?1
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
    sql = "SELECT event_seq, snapshot_hash FROM favn_runs WHERE run_id = ?1 LIMIT 1"

    case SQL.query(repo, sql, [run_id]) do
      {:ok, %{rows: [[event_seq, snapshot_hash]]}} -> {:ok, {event_seq, snapshot_hash}}
      {:ok, %{rows: []}} -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  defp fetch_event_by_sequence(repo, run_id, sequence) do
    sql =
      "SELECT event_blob FROM favn_run_events WHERE run_id = ?1 AND sequence = ?2 LIMIT 1"

    case SQL.query(repo, sql, [run_id, sequence]) do
      {:ok, %{rows: [[blob]]}} ->
        case decode_payload(blob) do
          {:ok, event} when is_map(event) -> {:ok, event}
          {:ok, other} -> {:error, {:invalid_event_payload, other}}
          {:error, reason} -> {:error, reason}
        end

      {:ok, %{rows: []}} ->
        {:ok, nil}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp fetch_scheduler_version(repo, {pipeline_module, schedule_id}) do
    sql =
      """
      SELECT version
      FROM favn_scheduler_cursors
      WHERE pipeline_module = ?1 AND schedule_id = ?2
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
      ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
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
        {"SELECT run_blob FROM favn_runs ORDER BY updated_seq DESC, run_id DESC", []}

      is_nil(status) ->
        {"SELECT run_blob FROM favn_runs ORDER BY updated_seq DESC, run_id DESC LIMIT ?1",
         [limit]}

      is_nil(limit) ->
        {
          "SELECT run_blob FROM favn_runs WHERE status = ?1 ORDER BY updated_seq DESC, run_id DESC",
          [Atom.to_string(status)]
        }

      true ->
        {
          "SELECT run_blob FROM favn_runs WHERE status = ?1 ORDER BY updated_seq DESC, run_id DESC LIMIT ?2",
          [Atom.to_string(status), limit]
        }
    end
  end

  defp decode_run(payload) do
    with {:ok, decoded} <- decode_payload(payload),
         %RunState{} = run_state <- decoded,
         {:ok, normalized} <- RunStateCodec.normalize(run_state) do
      {:ok, normalized}
    else
      {:error, reason} -> {:error, reason}
      other -> {:error, {:invalid_run_payload, other}}
    end
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

  defp repo_name(opts) do
    with {:ok, normalized} <- normalize_opts(opts),
         :ok <- ensure_schema_ready(normalized) do
      {:ok, Repo}
    end
  end

  defp normalize_opts(opts) do
    database = Keyword.get(opts, :database)

    if is_binary(database) and database != "" do
      with {:ok, migration_mode} <-
             validate_migration_mode(Keyword.get(opts, :migration_mode, :auto)) do
        {:ok,
         [
           database: database,
           pool_size: Keyword.get(opts, :pool_size, 1),
           busy_timeout: Keyword.get(opts, :busy_timeout, 5_000),
           migration_mode: migration_mode,
           initialize_empty?: Keyword.get(opts, :initialize_empty?, false),
           require_absolute_path: Keyword.get(opts, :require_absolute_path, false),
           supervisor_name: Keyword.get(opts, :supervisor_name, FavnStorageSqlite.Supervisor)
         ]}
      end
    else
      {:error, :sqlite_database_required}
    end
  end

  defp validate_migration_mode(mode) when mode in [:auto, :manual], do: {:ok, mode}
  defp validate_migration_mode(mode), do: {:error, {:invalid_migration_mode, mode}}

  defp ensure_schema_ready(normalized_opts) do
    case Keyword.fetch!(normalized_opts, :migration_mode) do
      :auto -> :ok
      :manual -> if(Migrations.schema_ready?(Repo), do: :ok, else: {:error, :schema_not_ready})
    end
  end

  defp encode_schedule_id(nil), do: @nil_schedule_id
  defp encode_schedule_id(schedule_id) when is_atom(schedule_id), do: Atom.to_string(schedule_id)
end
