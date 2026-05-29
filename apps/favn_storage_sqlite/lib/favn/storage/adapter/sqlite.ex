defmodule Favn.Storage.Adapter.SQLite do
  @moduledoc """
  SQLite-backed storage adapter implementing `Favn.Storage.Adapter`.
  """

  @behaviour Favn.Storage.Adapter
  @behaviour Favn.Storage.MaintenanceAdapter

  alias Ecto.Adapters.SQL
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
  alias FavnOrchestrator.Storage.AuthCodec
  alias FavnOrchestrator.Storage.Backfill.AssetWindowStateCodec
  alias FavnOrchestrator.Storage.Backfill.BackfillWindowCodec
  alias FavnOrchestrator.Storage.Backfill.CoverageBaselineCodec
  alias FavnOrchestrator.Storage.Backfill.ProgressCodec
  alias FavnOrchestrator.Storage.ExecutionAdmissionWaiterCodec
  alias FavnOrchestrator.Storage.ExecutionGroupSummary
  alias FavnOrchestrator.Storage.ExecutionLeaseCodec
  alias FavnOrchestrator.Storage.ExecutionOwnershipCodec
  alias FavnOrchestrator.Storage.Freshness.AssetFreshnessStateCodec
  alias FavnOrchestrator.Storage.IdempotencyResponseCodec
  alias FavnOrchestrator.Storage.LogEntryCodec
  alias FavnOrchestrator.Storage.ManifestCodec
  alias FavnOrchestrator.Storage.MaterializationClaimCodec
  alias FavnOrchestrator.Storage.JsonSafe
  alias FavnOrchestrator.Storage.RunEventCodec
  alias FavnOrchestrator.Storage.RunQuery
  alias FavnOrchestrator.Storage.RunSnapshotCodec
  alias FavnOrchestrator.Storage.RunStateCodec
  alias FavnOrchestrator.Storage.SchedulerStateCodec
  alias FavnOrchestrator.Storage.TargetStatusCodec
  alias FavnOrchestrator.Storage.WriteSemantics
  alias FavnOrchestrator.TargetStatus
  alias FavnStorageSqlite.Diagnostics
  alias FavnStorageSqlite.Maintenance, as: SQLiteMaintenance
  alias FavnStorageSqlite.Migrations
  alias FavnStorageSqlite.Repo
  alias FavnStorageSqlite.Supervisor, as: SQLiteSupervisor

  @active_manifest_key "active_manifest_version_id"
  @write_counter_key "run_write_order"
  @run_event_global_sequence_key "run_event_global_sequence"
  @log_global_sequence_key "log_global_sequence"
  @nil_schedule_id "__nil__"
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
    :node_key
  ]
  @read_model_chunk_size 50

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

  @impl Favn.Storage.MaintenanceAdapter
  def maintenance_status(opts) when is_list(opts) do
    SQLiteMaintenance.status(opts)
  end

  @impl Favn.Storage.MaintenanceAdapter
  def migrate_storage(opts, command_opts) when is_list(opts) and is_list(command_opts) do
    SQLiteMaintenance.migrate(opts, command_opts)
  end

  @impl Favn.Storage.MaintenanceAdapter
  def backup_storage(opts, command_opts) when is_list(opts) and is_list(command_opts) do
    SQLiteMaintenance.backup(opts, command_opts)
  end

  @impl Favn.Storage.MaintenanceAdapter
  def verify_storage_backup(opts, command_opts) when is_list(opts) and is_list(command_opts) do
    SQLiteMaintenance.verify_backup(opts, command_opts)
  end

  @impl true
  def put_manifest_version(%Version{} = version, opts) when is_list(opts) do
    with {:ok, repo} <- repo_name(opts),
         {:ok, record} <- ManifestCodec.to_record(version),
         {:ok, existing_id_hash} <- fetch_manifest_hash(repo, record.manifest_version_id),
         {:ok, existing_content_hash} <-
           fetch_manifest_record_by_content_hash(repo, record.content_hash) do
      cond do
        match?(%{}, existing_content_hash) -> :ok
        is_nil(existing_id_hash) -> insert_manifest_record(repo, record)
        existing_id_hash == record.content_hash -> :ok
        true -> {:error, :manifest_version_conflict}
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
  def get_manifest_version_by_content_hash(content_hash, opts)
      when is_binary(content_hash) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts),
         {:ok, row} <- fetch_manifest_record_by_content_hash(repo, content_hash) do
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
              result when result in [:ok, :idempotent] ->
                case refresh_execution_group_summary(
                       repo,
                       RunQuery.root_execution_group_id(normalized_run)
                     ) do
                  :ok -> {:ok, result}
                  {:error, reason} -> repo.rollback(reason)
                end

              {:error, reason} ->
                repo.rollback(reason)
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
  def put_execution_ownership(ownership, opts) when is_list(opts) do
    with {:ok, repo} <- repo_name(opts),
         {:ok, normalized} <- ExecutionOwnershipCodec.normalize(ownership),
         {:ok, payload} <- ExecutionOwnershipCodec.encode(normalized) do
      sql = """
      INSERT INTO favn_execution_ownerships (
        ownership_id, run_id, asset_step_id, runner_execution_id, status,
        inserted_at, updated_at, ownership_payload
      ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
      ON CONFLICT(ownership_id) DO UPDATE SET
        run_id = excluded.run_id,
        asset_step_id = excluded.asset_step_id,
        runner_execution_id = excluded.runner_execution_id,
        status = excluded.status,
        inserted_at = excluded.inserted_at,
        updated_at = excluded.updated_at,
        ownership_payload = excluded.ownership_payload
      """

      case SQL.query(repo, sql, execution_ownership_params(normalized, payload)) do
        {:ok, _} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def get_execution_ownership(ownership_id, opts)
      when is_binary(ownership_id) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      case SQL.query(
             repo,
             "SELECT ownership_payload FROM favn_execution_ownerships WHERE ownership_id = ?1 LIMIT 1",
             [ownership_id]
           ) do
        {:ok, %{rows: [row]}} -> decode_execution_ownership_row(row)
        {:ok, %{rows: []}} -> {:error, :not_found}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def list_execution_ownerships(run_id, opts) when is_binary(run_id) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      query_and_decode_rows(
        repo,
        "SELECT ownership_payload FROM favn_execution_ownerships WHERE run_id = ?1 ORDER BY inserted_at ASC, ownership_id ASC",
        [run_id],
        &decode_execution_ownership_row/1
      )
    end
  end

  @impl true
  def list_active_execution_ownerships(run_id, opts)
      when is_binary(run_id) and is_list(opts) do
    statuses = execution_ownership_active_statuses()

    placeholders =
      statuses
      |> Enum.with_index(2)
      |> Enum.map_join(", ", fn {_status, index} -> "?#{index}" end)

    with {:ok, repo} <- repo_name(opts) do
      query_and_decode_rows(
        repo,
        "SELECT ownership_payload FROM favn_execution_ownerships WHERE run_id = ?1 AND status IN (#{placeholders}) ORDER BY inserted_at ASC, ownership_id ASC",
        [run_id | statuses],
        &decode_execution_ownership_row/1
      )
    end
  end

  @impl true
  def get_run(run_id, opts) when is_binary(run_id) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      sql = run_snapshot_select() <> " WHERE r.run_id = ?1 LIMIT 1"

      case SQL.query(repo, sql, [run_id]) do
        {:ok, %{rows: [row]}} -> decode_run_row(row)
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
  def list_target_runs(manifest_version_id, target_kind, target_ref, run_opts, opts)
      when is_binary(manifest_version_id) and target_kind in [:asset, :pipeline] and
             is_list(run_opts) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts),
         :ok <- repair_missing_run_query_metadata(repo) do
      {sql, params} =
        list_target_runs_query(manifest_version_id, target_kind, target_ref, run_opts)

      case SQL.query(repo, sql, params) do
        {:ok, %{rows: rows}} -> decode_run_rows(rows)
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def list_execution_group_runs(group_id, opts) when is_binary(group_id) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts),
         :ok <- repair_missing_run_query_metadata(repo) do
      sql =
        run_snapshot_select() <>
          " WHERE r.root_execution_group_id = ?1 ORDER BY CASE WHEN r.run_id = r.root_execution_group_id THEN 0 ELSE 1 END, r.inserted_at ASC, r.run_id ASC"

      case SQL.query(repo, sql, [group_id]) do
        {:ok, %{rows: rows}} -> decode_run_rows(rows)
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def list_execution_group_run_ids(group_id, opts) when is_binary(group_id) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts),
         :ok <- repair_missing_run_query_metadata(repo) do
      sql =
        "SELECT run_id FROM favn_runs WHERE root_execution_group_id = ?1 ORDER BY CASE WHEN run_id = root_execution_group_id THEN 0 ELSE 1 END, inserted_at ASC, run_id ASC"

      case SQL.query(repo, sql, [group_id]) do
        {:ok, %{rows: rows}} -> {:ok, Enum.map(rows, fn [run_id] -> run_id end)}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def list_execution_groups(group_opts, opts) when is_list(group_opts) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts),
         :ok <- repair_missing_run_query_metadata(repo),
         {:ok, query, params, page_opts} <- execution_groups_query(group_opts) do
      case SQL.query(repo, query, params) do
        {:ok, %{rows: rows}} ->
          rows
          |> Enum.map(fn [group_id] -> group_id end)
          |> Page.from_fetched(page_opts)
          |> then(&{:ok, &1})

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @impl true
  def list_execution_group_summaries(group_opts, opts)
      when is_list(group_opts) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts),
         {:ok, query, params, page_opts} <- execution_group_summaries_query(group_opts) do
      case SQL.query(repo, query, params) do
        {:ok, %{rows: rows}} ->
          rows
          |> Enum.reduce_while({:ok, []}, fn [payload], {:ok, acc} ->
            case ExecutionGroupSummary.decode(payload) do
              {:ok, summary} -> {:cont, {:ok, [summary | acc]}}
              {:error, reason} -> {:halt, {:error, reason}}
            end
          end)
          |> case do
            {:ok, summaries} -> {:ok, Page.from_fetched(Enum.reverse(summaries), page_opts)}
            {:error, reason} -> {:error, reason}
          end

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @impl true
  def rebuild_execution_group_summaries(opts) when is_list(opts) do
    with {:ok, repo} <- repo_name(opts),
         {:ok, group_ids} <- execution_group_ids_for_rebuild(repo) do
      case refresh_execution_group_summaries(repo, group_ids) do
        :ok -> {:ok, length(group_ids)}
        {:error, reason} -> {:error, reason}
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
      sql =
        "SELECT global_sequence, event_blob FROM favn_run_events WHERE run_id = ?1 ORDER BY sequence ASC"

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
  def list_run_events(run_id, run_event_opts, opts)
      when is_binary(run_id) and is_list(run_event_opts) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts),
         {:ok, query, params} <- run_events_query(run_id, run_event_opts) do
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
  def list_execution_group_events(group_id, run_event_opts, opts)
      when is_binary(group_id) and is_list(run_event_opts) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts),
         {:ok, query, params} <- execution_group_events_query(group_id, run_event_opts) do
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
  def list_global_run_events(run_event_opts, opts)
      when is_list(run_event_opts) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts),
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
  def try_acquire_execution_lease(lease, opts) when is_map(lease) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts),
         {:ok, normalized} <- ExecutionLeaseCodec.normalize(lease),
         {:ok, payload} <- ExecutionLeaseCodec.encode(normalized) do
      repo.transact(fn ->
        with {:ok, _expired} <- delete_expired_execution_leases(repo, normalized.acquired_at),
             :ok <-
               ensure_execution_lease_capacity(repo, normalized.scopes, normalized.acquired_at),
             :ok <- insert_execution_lease(repo, normalized, payload),
             :ok <- insert_execution_lease_scopes(repo, normalized) do
          {:ok, normalized}
        else
          {:error, reason} -> repo.rollback(reason)
        end
      end)
      |> case do
        {:ok, lease} -> {:ok, lease}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def release_execution_lease(lease_id, opts) when is_binary(lease_id) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      repo.transact(fn ->
        with :ok <- delete_execution_lease_scopes(repo, lease_id),
             :ok <- delete_execution_lease(repo, lease_id) do
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
  def release_execution_leases_for_run(run_id, opts) when is_binary(run_id) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      repo.transact(fn ->
        with {:ok, leases} <- list_execution_leases_for_run(repo, run_id),
             lease_ids <- Enum.map(leases, & &1.lease_id),
             :ok <- delete_execution_lease_scopes(repo, lease_ids),
             {:ok, released_count} <- delete_execution_leases_by_ids(repo, lease_ids) do
          {:ok, LeaseRelease.new(run_id, released_count, released_execution_scopes(leases))}
        else
          {:error, reason} -> repo.rollback(reason)
        end
      end)
      |> case do
        {:ok, release} -> {:ok, release}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def expire_execution_leases(%DateTime{} = now, opts) when is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      delete_expired_execution_leases(repo, now)
    end
  end

  @impl true
  def list_execution_leases(opts) when is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      sql = "SELECT lease_payload FROM favn_execution_leases ORDER BY lease_id ASC"

      case SQL.query(repo, sql, []) do
        {:ok, %{rows: rows}} ->
          rows
          |> Enum.reduce_while({:ok, []}, fn [payload], {:ok, acc} ->
            case ExecutionLeaseCodec.decode(payload) do
              {:ok, lease} -> {:cont, {:ok, [lease | acc]}}
              {:error, reason} -> {:halt, {:error, reason}}
            end
          end)
          |> case do
            {:ok, leases} -> {:ok, Enum.reverse(leases)}
            {:error, reason} -> {:error, reason}
          end

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @impl true
  def upsert_execution_admission_waiter(waiter, opts) when is_map(waiter) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts),
         {:ok, normalized} <- ExecutionAdmissionWaiterCodec.normalize(waiter) do
      repo.transact(fn ->
        with {:ok, existing} <- fetch_execution_admission_waiter(repo, normalized.waiter_id),
             next <- next_execution_admission_waiter(normalized, existing),
             {:ok, payload} <- ExecutionAdmissionWaiterCodec.encode(next),
             :ok <- upsert_execution_admission_waiter_row(repo, next, payload) do
          {:ok, next}
        else
          {:error, reason} -> repo.rollback(reason)
        end
      end)
      |> case do
        {:ok, waiter} -> {:ok, waiter}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def delete_execution_admission_waiter(waiter_id, opts)
      when is_binary(waiter_id) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      case SQL.query(repo, "DELETE FROM favn_execution_admission_waiters WHERE waiter_id = ?1", [
             waiter_id
           ]) do
        {:ok, _result} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def delete_execution_admission_waiters_for_run(run_id, opts)
      when is_binary(run_id) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      case SQL.query(repo, "DELETE FROM favn_execution_admission_waiters WHERE run_id = ?1", [
             run_id
           ]) do
        {:ok, result} -> {:ok, Map.get(result, :num_rows, 0)}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def list_execution_admission_waiters_for_scope(scope, waiter_opts, opts)
      when is_map(scope) and is_list(waiter_opts) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts),
         {:ok, normalized_scope} <- ExecutionLeaseCodec.normalize_scope(scope) do
      {scope_kind, scope_key} = ExecutionLeaseCodec.scope_identity(normalized_scope)
      limit = waiter_limit(waiter_opts)

      sql = """
      SELECT waiter_payload
      FROM favn_execution_admission_waiters
      WHERE blocked_scope_kind = ?1 AND blocked_scope_key = ?2
      ORDER BY inserted_at ASC, waiter_id ASC
      LIMIT ?3
      """

      decode_rows(
        repo,
        sql,
        [scope_kind, scope_key, limit],
        &decode_execution_admission_waiter_row/1
      )
    end
  end

  @impl true
  def expire_execution_admission_waiters(%DateTime{} = now, opts) when is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      case SQL.query(
             repo,
             "DELETE FROM favn_execution_admission_waiters WHERE deadline_at IS NOT NULL AND deadline_at <= ?1",
             [DateTime.to_iso8601(now)]
           ) do
        {:ok, result} -> {:ok, Map.get(result, :num_rows, 0)}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def try_acquire_materialization_claim(claim, opts) when is_map(claim) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts),
         {:ok, normalized} <- MaterializationClaimCodec.normalize(claim) do
      repo.transact(fn ->
        with {:ok, _expired} <-
               expire_materialization_claims_in_transaction(repo, normalized.claimed_at),
             {:ok, existing} <- fetch_materialization_claim(repo, normalized.claim_key) do
          case materialization_claim_acquire_decision(existing, normalized.claimed_at) do
            :insert ->
              case insert_materialization_claim_if_absent(repo, normalized) do
                :ok -> {:ok, {:ok, normalized}}
                :conflict -> materialization_claim_conflict_result(repo, normalized)
                {:error, reason} -> repo.rollback(reason)
              end

            :reclaim ->
              case reclaim_materialization_claim(repo, normalized) do
                :ok -> {:ok, {:ok, normalized}}
                :conflict -> materialization_claim_conflict_result(repo, normalized)
                {:error, reason} -> repo.rollback(reason)
              end

            result ->
              {:ok, result}
          end
        else
          {:error, reason} -> repo.rollback(reason)
        end
      end)
      |> case do
        {:ok, result} -> result
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def complete_materialization_claim(claim_key, completion, opts)
      when is_binary(claim_key) and is_map(completion) and is_list(opts) do
    guarded_materialization_claim_transition(claim_key, opts, fn claim ->
      apply_materialization_completion(claim, completion)
    end)
  end

  @impl true
  def fail_materialization_claim(claim_key, failure, opts)
      when is_binary(claim_key) and is_map(failure) and is_list(opts) do
    guarded_materialization_claim_transition(claim_key, opts, fn claim ->
      apply_materialization_failure(claim, failure)
    end)
  end

  @impl true
  def expire_materialization_claims(%DateTime{} = now, opts) when is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      expire_materialization_claims_in_transaction(repo, now)
    end
  end

  @impl true
  def get_materialization_claim(claim_key, opts) when is_binary(claim_key) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts),
         {:ok, claim} <- fetch_materialization_claim(repo, claim_key) do
      case claim do
        nil -> {:error, :not_found}
        %MaterializationClaim{} -> {:ok, claim}
      end
    end
  end

  @impl true
  def list_materialization_claims(filters, opts) when is_list(filters) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts),
         {:ok, {where_sql, params}} <-
           build_filter_sql(read_filters(filters), materialization_claim_filter_columns()) do
      sql =
        "SELECT record_payload FROM favn_materialization_claims#{where_sql} ORDER BY claimed_at DESC, claim_key ASC"

      decode_rows(repo, sql, params, &decode_materialization_claim_row/1)
    end
  end

  @impl true
  def persist_log_entries(entries, opts) when is_list(entries) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts),
         {:ok, normalized} <- normalize_log_entries(entries) do
      repo.transact(fn ->
        case persist_log_entries_in_transaction(repo, normalized) do
          {:ok, persisted} -> {:ok, persisted}
          {:error, reason} -> repo.rollback(reason)
        end
      end)
    end
  end

  @impl true
  def list_logs(filter, opts, adapter_opts) when is_list(opts) and is_list(adapter_opts) do
    with {:ok, repo} <- repo_name(adapter_opts),
         {:ok, page_opts} <- page_opts(opts),
         {:ok, sql, params} <- list_logs_query(filter, opts) do
      decode_page(repo, sql, params, page_opts, &decode_log_entry_row/1)
    end
  end

  @impl true
  def scan_logs(filter, scan_opts, adapter_opts)
      when is_list(scan_opts) and is_list(adapter_opts) do
    with {:ok, repo} <- repo_name(adapter_opts),
         {:ok, after_sequence} <- log_cursor_sequence(Keyword.get(scan_opts, :after)),
         {:ok, sql, params} <- scan_logs_query(filter, after_sequence) do
      decode_cursor_page(
        repo,
        sql,
        params,
        scan_opts,
        &decode_log_entry_row/1,
        &log_entry_cursor!/1
      )
    end
  end

  @impl true
  def replay_logs_after(cursor, filter, opts, adapter_opts)
      when is_list(opts) and is_list(adapter_opts) do
    with {:ok, repo} <- repo_name(adapter_opts),
         {:ok, after_sequence} <- log_cursor_sequence(cursor),
         :ok <- validate_log_replay_limit(Keyword.get(opts, :limit, 200)),
         {:ok, sql, params} <- replay_logs_query(filter, after_sequence, opts) do
      case SQL.query(repo, sql, params) do
        {:ok, %{rows: rows}} -> decode_log_entry_rows(rows)
        {:error, reason} -> {:error, reason}
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
          with {:ok, decoded} <- SchedulerStateCodec.decode_state(payload),
               do:
                 SchedulerStateCodec.build_state({pipeline_module, schedule_id}, version, decoded)

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @impl true
  def put_coverage_baseline(%CoverageBaseline{} = baseline, opts) when is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      upsert_coverage_baselines(repo, [baseline])
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
      with :ok <- upsert_backfill_windows(repo, [window]) do
        refresh_execution_group_summary(repo, window.backfill_run_id)
      end
    end
  end

  @impl true
  def put_backfill_windows(windows, opts) when is_list(windows) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      with :ok <- upsert_backfill_windows(repo, windows) do
        refresh_execution_group_summaries(repo, Enum.map(windows, & &1.backfill_run_id))
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
  def scan_backfill_windows(filters, scan_opts, opts)
      when is_list(filters) and is_list(scan_opts) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts),
         {:ok, {where_sql, params}} <-
           build_filter_sql(read_filters(filters), backfill_window_filter_columns()),
         {:ok, {cursor_sql, cursor_params}} <-
           backfill_window_cursor_sql(Keyword.get(scan_opts, :after), length(params)),
         {where_sql, params} <- append_cursor_sql(where_sql, params, cursor_sql, cursor_params) do
      sql =
        "SELECT #{backfill_window_columns()} FROM favn_backfill_windows#{where_sql} ORDER BY window_start_at ASC, backfill_run_id ASC, pipeline_module ASC, window_key ASC LIMIT ?#{length(params) + 1}"

      decode_cursor_page(
        repo,
        sql,
        params,
        scan_opts,
        &decode_backfill_window_row/1,
        &backfill_window_cursor!/1
      )
    end
  end

  @impl true
  def apply_backfill_child_projection(%BackfillWindow{} = window, asset_window_states, opts)
      when is_list(asset_window_states) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      repo.transact(fn ->
        with :ok <- lock_backfill_window(repo, window),
             old_status <- fetch_backfill_window_status(repo, window),
             :ok <- upsert_backfill_windows(repo, [window]),
             :ok <- upsert_asset_window_states(repo, asset_window_states),
             {:ok, progress} <-
               upsert_backfill_progress_after_window_change(repo, window, old_status) do
          {:ok, progress}
        else
          {:error, reason} -> repo.rollback(reason)
        end
      end)
      |> case do
        {:ok, %BackfillProgress{} = progress} -> {:ok, progress}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def get_backfill_progress(backfill_run_id, opts)
      when is_binary(backfill_run_id) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      get_backfill_progress_with_repo(repo, backfill_run_id)
    end
  end

  @impl true
  def rebuild_backfill_progress(backfill_run_id, opts)
      when is_binary(backfill_run_id) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      rebuild_backfill_progress_with_repo(repo, backfill_run_id)
    end
  end

  @impl true
  def put_asset_window_state(%AssetWindowState{} = state, opts) when is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      upsert_asset_window_states(repo, [state])
    end
  end

  @impl true
  def put_asset_window_states(states, opts) when is_list(states) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      upsert_asset_window_states(repo, states)
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
  def put_asset_freshness_state(%AssetFreshnessState{} = state, opts) when is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      sql =
        """
        INSERT INTO favn_asset_freshness_states (
          asset_ref_module, asset_ref_name, freshness_key, status, freshness_version,
          latest_success_run_id, latest_attempt_run_id, latest_attempt_status,
          manifest_version_id, manifest_content_hash, record_payload, updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
        ON CONFLICT(asset_ref_module, asset_ref_name, freshness_key) DO UPDATE SET
          status = excluded.status,
          freshness_version = excluded.freshness_version,
          latest_success_run_id = excluded.latest_success_run_id,
          latest_attempt_run_id = excluded.latest_attempt_run_id,
          latest_attempt_status = excluded.latest_attempt_status,
          manifest_version_id = excluded.manifest_version_id,
          manifest_content_hash = excluded.manifest_content_hash,
          record_payload = excluded.record_payload,
          updated_at = excluded.updated_at
        """

      params = [
        encode_atom(state.asset_ref_module),
        encode_atom(state.asset_ref_name),
        state.freshness_key,
        encode_atom(state.status),
        state.freshness_version,
        state.latest_success_run_id,
        state.latest_attempt_run_id,
        encode_optional_atom(state.latest_attempt_status),
        state.manifest_version_id,
        state.manifest_content_hash,
        encode_asset_freshness_state(state),
        encode_datetime(state.updated_at)
      ]

      case SQL.query(repo, sql, params) do
        {:ok, _} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def get_asset_freshness_state(asset_ref_module, asset_ref_name, freshness_key, opts)
      when is_atom(asset_ref_module) and is_atom(asset_ref_name) and is_binary(freshness_key) and
             is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      sql =
        "SELECT #{asset_freshness_state_columns()} FROM favn_asset_freshness_states WHERE asset_ref_module = ?1 AND asset_ref_name = ?2 AND freshness_key = ?3 LIMIT 1"

      params = [encode_atom(asset_ref_module), encode_atom(asset_ref_name), freshness_key]

      case SQL.query(repo, sql, params) do
        {:ok, %{rows: [row]}} -> decode_asset_freshness_state_row(row)
        {:ok, %{rows: []}} -> {:error, :not_found}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def list_asset_freshness_states(filters, opts) when is_list(filters) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts),
         {:ok, page_opts} <- page_opts(filters),
         {:ok, {where_sql, params}} <-
           build_filter_sql(read_filters(filters), asset_freshness_state_filter_columns()) do
      sql =
        "SELECT #{asset_freshness_state_columns()} FROM favn_asset_freshness_states#{where_sql} ORDER BY updated_at DESC, asset_ref_module ASC, asset_ref_name ASC, freshness_key ASC LIMIT ?#{length(params) + 1} OFFSET ?#{length(params) + 2}"

      decode_page(repo, sql, params, page_opts, &decode_asset_freshness_state_row/1)
    end
  end

  @impl true
  def scan_asset_freshness_states(filters, scan_opts, opts)
      when is_list(filters) and is_list(scan_opts) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts),
         {:ok, {where_sql, params}} <-
           build_filter_sql(read_filters(filters), asset_freshness_state_filter_columns()),
         {:ok, {cursor_sql, cursor_params}} <-
           asset_freshness_cursor_sql(Keyword.get(scan_opts, :after), length(params)),
         {where_sql, params} <- append_cursor_sql(where_sql, params, cursor_sql, cursor_params) do
      sql =
        "SELECT #{asset_freshness_state_columns()} FROM favn_asset_freshness_states#{where_sql} ORDER BY updated_at DESC, asset_ref_module ASC, asset_ref_name ASC, freshness_key ASC LIMIT ?#{length(params) + 1}"

      decode_cursor_page(
        repo,
        sql,
        params,
        scan_opts,
        &decode_asset_freshness_state_row/1,
        &asset_freshness_cursor!/1
      )
    end
  end

  @impl true
  def get_asset_freshness_states_by_keys(keys, opts) when is_list(keys) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      keys
      |> Enum.uniq()
      |> Enum.chunk_every(250)
      |> Enum.reduce_while({:ok, %{}}, fn chunk, {:ok, acc} ->
        case get_asset_freshness_state_key_chunk(repo, chunk) do
          {:ok, rows} -> {:cont, {:ok, Map.merge(acc, rows)}}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)
    end
  end

  @impl true
  def upsert_target_status(%TargetStatus{} = status, opts) when is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      upsert_target_statuses(repo, [status])
    end
  end

  @impl true
  def get_target_status(manifest_version_id, target_kind, target_id, opts)
      when is_binary(manifest_version_id) and target_kind in [:asset, :pipeline] and
             is_binary(target_id) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      sql =
        "SELECT #{target_status_columns()} FROM favn_target_statuses WHERE manifest_version_id = ?1 AND target_kind = ?2 AND target_id = ?3 LIMIT 1"

      params = [manifest_version_id, encode_atom(target_kind), target_id]

      case SQL.query(repo, sql, params) do
        {:ok, %{rows: [row]}} -> decode_target_status_row(row)
        {:ok, %{rows: []}} -> {:error, :not_found}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def list_target_statuses(manifest_version_id, target_kind, target_ids, opts)
      when is_binary(manifest_version_id) and target_kind in [:asset, :pipeline] and
             is_list(target_ids) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      target_ids
      |> Enum.uniq()
      |> Enum.chunk_every(250)
      |> Enum.reduce_while({:ok, %{}}, fn chunk, {:ok, acc} ->
        case list_target_status_chunk(repo, manifest_version_id, target_kind, chunk) do
          {:ok, rows} -> {:cont, {:ok, Map.merge(acc, rows)}}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)
    end
  end

  @impl true
  def replace_target_statuses(scope, statuses, opts) when is_list(statuses) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts),
         {:ok, scope} <- target_status_scope(scope),
         :ok <- validate_target_status_scope_rows(scope, statuses) do
      repo.transact(fn ->
        with :ok <- delete_target_status_scope(repo, scope),
             :ok <- upsert_target_statuses(repo, statuses) do
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
  def delete_target_statuses(scope, opts) when is_list(opts) do
    with {:ok, repo} <- repo_name(opts),
         {:ok, scope} <- target_status_scope(scope) do
      delete_target_status_scope(repo, scope)
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
      when (scope == :all or is_tuple(scope)) and is_list(coverage_baselines) and
             is_list(backfill_windows) and
             is_list(asset_window_states) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts),
         {:ok, scope} <- replacement_scope(scope) do
      repo.transact(fn ->
        with {:ok, affected_ids} <- affected_backfill_ids_for_scope(repo, scope),
             :ok <-
               delete_replacement_scope(
                 repo,
                 "favn_pipeline_coverage_baselines",
                 :coverage,
                 scope
               ),
             :ok <- delete_replacement_scope(repo, "favn_backfill_windows", :window, scope),
             :ok <-
               delete_replacement_scope(repo, "favn_asset_window_states", :asset_state, scope),
             :ok <- delete_replacement_progress(repo, scope),
             :ok <- upsert_coverage_baselines(repo, coverage_baselines),
             :ok <- upsert_backfill_windows(repo, backfill_windows),
             :ok <- upsert_asset_window_states(repo, asset_window_states),
             :ok <-
               rebuild_backfill_progress_for_ids(
                 repo,
                 affected_ids ++ Enum.map(backfill_windows, & &1.backfill_run_id)
               ) do
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
  def put_auth_actor(actor, opts) when is_map(actor) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts),
         {:ok, roles_payload} <- AuthCodec.encode_roles(Map.fetch!(actor, :roles)) do
      sql = """
      INSERT INTO favn_auth_actors (actor_id, username, display_name, roles_blob, status, inserted_at, updated_at)
      VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
      ON CONFLICT(actor_id) DO UPDATE SET
        username = excluded.username,
        display_name = excluded.display_name,
        roles_blob = excluded.roles_blob,
        status = excluded.status,
        updated_at = excluded.updated_at
      """

      params = [
        actor.id,
        actor.username,
        actor.display_name,
        roles_payload,
        encode_atom(actor.status),
        encode_datetime(actor.inserted_at),
        encode_datetime(actor.updated_at)
      ]

      query_ok(repo, sql, params)
    end
  end

  @impl true
  def put_auth_actor_with_credential(actor, credential, opts)
      when is_map(actor) and is_map(credential) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      repo.transact(fn ->
        with :ok <- put_auth_actor(actor, opts),
             :ok <- put_auth_credential(actor.id, credential, opts) do
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
  def get_auth_actor(actor_id, opts) when is_binary(actor_id) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      fetch_auth_actor(repo, "actor_id = ?1", [actor_id])
    end
  end

  @impl true
  def get_auth_actor_by_username(username, opts) when is_binary(username) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      fetch_auth_actor(repo, "username = ?1", [username])
    end
  end

  @impl true
  def list_auth_actors(opts) when is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      sql = "SELECT #{auth_actor_columns()} FROM favn_auth_actors ORDER BY username ASC"
      decode_rows(repo, sql, [], &decode_auth_actor_row/1)
    end
  end

  @impl true
  def put_auth_credential(actor_id, credential, opts)
      when is_binary(actor_id) and is_map(credential) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts),
         {:ok, credential_payload} <- AuthCodec.encode_credential(credential) do
      sql = """
      INSERT INTO favn_auth_credentials (actor_id, credential_blob, updated_at)
      VALUES (?1, ?2, ?3)
      ON CONFLICT(actor_id) DO UPDATE SET
        credential_blob = excluded.credential_blob,
        updated_at = excluded.updated_at
      """

      query_ok(repo, sql, [
        actor_id,
        credential_payload,
        encode_datetime(DateTime.utc_now())
      ])
    end
  end

  @impl true
  def update_auth_actor_password(actor_id, actor, credential, revoked_at, opts)
      when is_binary(actor_id) and is_map(actor) and is_map(credential) and
             is_struct(revoked_at, DateTime) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      repo.transact(fn ->
        with :ok <- put_auth_actor(actor, opts),
             :ok <- put_auth_credential(actor_id, credential, opts),
             :ok <- revoke_auth_sessions_for_actor(actor_id, revoked_at, opts) do
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
  def get_auth_credential(actor_id, opts) when is_binary(actor_id) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      sql = "SELECT credential_blob FROM favn_auth_credentials WHERE actor_id = ?1 LIMIT 1"

      case SQL.query(repo, sql, [actor_id]) do
        {:ok, %{rows: [[payload]]}} -> AuthCodec.decode_credential(payload)
        {:ok, %{rows: []}} -> {:error, :not_found}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def put_auth_session(session, opts) when is_map(session) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      sql = """
      INSERT INTO favn_auth_sessions (session_id, token_hash, actor_id, provider, issued_at, expires_at, revoked_at)
      VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
      ON CONFLICT(session_id) DO UPDATE SET
        token_hash = excluded.token_hash,
        actor_id = excluded.actor_id,
        provider = excluded.provider,
        issued_at = excluded.issued_at,
        expires_at = excluded.expires_at,
        revoked_at = excluded.revoked_at
      """

      params = [
        session.id,
        session.token_hash,
        session.actor_id,
        session.provider,
        encode_datetime(session.issued_at),
        encode_datetime(session.expires_at),
        encode_datetime(session.revoked_at)
      ]

      query_ok(repo, sql, params)
    end
  end

  @impl true
  def get_auth_session(session_id, opts) when is_binary(session_id) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      fetch_auth_session(repo, "session_id = ?1", [session_id])
    end
  end

  @impl true
  def get_auth_session_by_token_hash(token_hash, opts)
      when is_binary(token_hash) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      fetch_auth_session(repo, "token_hash = ?1", [token_hash])
    end
  end

  @impl true
  def revoke_auth_session(session_id, revoked_at, opts)
      when is_binary(session_id) and is_struct(revoked_at, DateTime) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      sql =
        "UPDATE favn_auth_sessions SET revoked_at = ?1 WHERE session_id = ?2 AND revoked_at IS NULL"

      query_ok(repo, sql, [encode_datetime(revoked_at), session_id])
    end
  end

  @impl true
  def revoke_auth_sessions_for_actor(actor_id, revoked_at, opts)
      when is_binary(actor_id) and is_struct(revoked_at, DateTime) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      sql =
        "UPDATE favn_auth_sessions SET revoked_at = ?1 WHERE actor_id = ?2 AND revoked_at IS NULL"

      query_ok(repo, sql, [encode_datetime(revoked_at), actor_id])
    end
  end

  @impl true
  def put_auth_audit(entry, opts) when is_map(entry) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts),
         {:ok, entry_payload} <- AuthCodec.encode_audit(entry) do
      sql = """
      INSERT INTO favn_auth_audits (audit_id, occurred_at, action, actor_id, session_id, outcome, entry_blob)
      VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
      """

      params = [
        Map.fetch!(entry, :id),
        encode_datetime(Map.fetch!(entry, :occurred_at)),
        Map.get(entry, :action),
        Map.get(entry, :actor_id),
        Map.get(entry, :session_id),
        Map.get(entry, :outcome),
        entry_payload
      ]

      query_ok(repo, sql, params)
    end
  end

  @impl true
  def list_auth_audit(audit_opts, opts) when is_list(audit_opts) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      limit = audit_opts |> Keyword.get(:limit, 100) |> max(1) |> min(500)

      sql =
        "SELECT entry_blob FROM favn_auth_audits ORDER BY occurred_at DESC, audit_id DESC LIMIT ?1"

      case SQL.query(repo, sql, [limit]) do
        {:ok, %{rows: rows}} ->
          rows
          |> Enum.reduce_while({:ok, []}, fn [payload], {:ok, acc} ->
            case AuthCodec.decode_audit(payload) do
              {:ok, entry} when is_map(entry) -> {:cont, {:ok, [entry | acc]}}
              {:error, reason} -> {:halt, {:error, reason}}
            end
          end)
          |> case do
            {:ok, entries} -> {:ok, Enum.reverse(entries)}
            {:error, reason} -> {:error, reason}
          end

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @impl true
  def reserve_idempotency_record(record, opts) when is_map(record) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts) do
      repo.transact(fn ->
        reserve_idempotency_record_in_transaction(repo, record)
        |> case do
          {:ok, result} -> {:ok, result}
          {:error, reason} -> repo.rollback(reason)
        end
      end)
      |> case do
        {:ok, result} -> {:ok, result}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp reserve_idempotency_record_in_transaction(repo, record) do
    case insert_idempotency_record(repo, record, :ignore_conflict) do
      {:ok, {:reserved, _record}} = reserved ->
        reserved

      {:ok, :ignored} ->
        classify_or_replace_existing_idempotency_record(repo, record)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp classify_or_replace_existing_idempotency_record(repo, record) do
    case fetch_idempotency_record(repo, record.id) do
      {:ok, stored} ->
        reserve_existing_idempotency_record(repo, stored, record)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp reserve_existing_idempotency_record(repo, stored, record) do
    if expired_idempotency_record?(stored) do
      case delete_expired_idempotency_record(repo, stored.id) do
        :ok -> insert_idempotency_record(repo, record)
        :not_deleted -> classify_or_replace_existing_idempotency_record(repo, record)
        {:error, reason} -> {:error, reason}
      end
    else
      classify_idempotency_record(stored, record.request_fingerprint)
    end
  end

  @impl true
  def complete_idempotency_record(record_id, attrs, opts)
      when is_binary(record_id) and is_map(attrs) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts),
         {:ok, stored} <- fetch_idempotency_record(repo, record_id) do
      sql = """
      UPDATE favn_idempotency_records
      SET status = ?1,
          response_status = ?2,
          response_body_blob = ?3,
          resource_type = ?4,
          resource_id = ?5,
          updated_at = ?6,
          completed_at = ?7
      WHERE idempotency_record_id = ?8
      """

      params = [
        encode_atom(Map.fetch!(attrs, :status)),
        Map.get(attrs, :response_status),
        encode_optional_idempotency_response(
          Map.get(attrs, :response_body),
          stored.operation
        ),
        Map.get(attrs, :resource_type),
        Map.get(attrs, :resource_id),
        encode_datetime(Map.fetch!(attrs, :updated_at)),
        encode_datetime(Map.fetch!(attrs, :completed_at)),
        record_id
      ]

      query_ok(repo, sql, params)
    end
  end

  @impl true
  def get_idempotency_record(record_id, opts) when is_binary(record_id) and is_list(opts) do
    with {:ok, repo} <- repo_name(opts), do: fetch_idempotency_record(repo, record_id)
  end

  defp coverage_baseline_columns do
    "record_payload"
  end

  defp auth_actor_columns do
    "actor_id, username, display_name, roles_blob, status, inserted_at, updated_at"
  end

  defp auth_session_columns do
    "session_id, token_hash, actor_id, provider, issued_at, expires_at, revoked_at"
  end

  defp idempotency_record_columns do
    "idempotency_record_id, operation, idempotency_key_hash, actor_id, session_id, service_identity, request_fingerprint, status, response_status, response_body_blob, resource_type, resource_id, created_at, updated_at, expires_at, completed_at"
  end

  defp fetch_idempotency_record(repo, record_id) do
    sql =
      "SELECT #{idempotency_record_columns()} FROM favn_idempotency_records WHERE idempotency_record_id = ?1 LIMIT 1"

    case SQL.query(repo, sql, [record_id]) do
      {:ok, %{rows: [row]}} -> decode_idempotency_record_row(row)
      {:ok, %{rows: []}} -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  defp insert_idempotency_record(repo, record) do
    sql = """
    INSERT INTO favn_idempotency_records (
      idempotency_record_id, operation, idempotency_key_hash, actor_id, session_id,
      service_identity, request_fingerprint, status, response_status, response_body_blob,
      resource_type, resource_id, created_at, updated_at, expires_at, completed_at
    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)
    """

    case SQL.query(repo, sql, idempotency_record_params(record)) do
      {:ok, _result} -> {:ok, {:reserved, record}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp insert_idempotency_record(repo, record, :ignore_conflict) do
    sql = """
    INSERT OR IGNORE INTO favn_idempotency_records (
      idempotency_record_id, operation, idempotency_key_hash, actor_id, session_id,
      service_identity, request_fingerprint, status, response_status, response_body_blob,
      resource_type, resource_id, created_at, updated_at, expires_at, completed_at
    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)
    """

    case SQL.query(repo, sql, idempotency_record_params(record)) do
      {:ok, %{num_rows: 1}} -> {:ok, {:reserved, record}}
      {:ok, _result} -> {:ok, :ignored}
      {:error, reason} -> {:error, reason}
    end
  end

  defp delete_expired_idempotency_record(repo, record_id) do
    sql =
      "DELETE FROM favn_idempotency_records WHERE idempotency_record_id = ?1 AND expires_at <= ?2"

    case SQL.query(repo, sql, [record_id, encode_datetime(DateTime.utc_now())]) do
      {:ok, %{num_rows: 1}} -> :ok
      {:ok, _result} -> :not_deleted
      {:error, reason} -> {:error, reason}
    end
  end

  defp expired_idempotency_record?(%{expires_at: %DateTime{} = expires_at}) do
    DateTime.compare(expires_at, DateTime.utc_now()) != :gt
  end

  defp expired_idempotency_record?(_record), do: false

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

  defp fetch_auth_actor(repo, where_sql, params) do
    sql = "SELECT #{auth_actor_columns()} FROM favn_auth_actors WHERE #{where_sql} LIMIT 1"

    case SQL.query(repo, sql, params) do
      {:ok, %{rows: [row]}} -> decode_auth_actor_row(row)
      {:ok, %{rows: []}} -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  defp fetch_auth_session(repo, where_sql, params) do
    sql = "SELECT #{auth_session_columns()} FROM favn_auth_sessions WHERE #{where_sql} LIMIT 1"

    case SQL.query(repo, sql, params) do
      {:ok, %{rows: [row]}} -> decode_auth_session_row(row)
      {:ok, %{rows: []}} -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_auth_actor_row([
         actor_id,
         username,
         display_name,
         roles_blob,
         status,
         inserted_at,
         updated_at
       ]) do
    with {:ok, roles} <- AuthCodec.decode_roles(roles_blob),
         {:ok, status} <- existing_atom(status) do
      {:ok,
       %{
         id: actor_id,
         username: username,
         display_name: display_name,
         roles: roles,
         status: status,
         inserted_at: decode_datetime(inserted_at),
         updated_at: decode_datetime(updated_at)
       }}
    end
  end

  defp decode_auth_session_row([
         session_id,
         token_hash,
         actor_id,
         provider,
         issued_at,
         expires_at,
         revoked_at
       ]) do
    {:ok,
     %{
       id: session_id,
       token_hash: token_hash,
       actor_id: actor_id,
       provider: provider,
       issued_at: decode_datetime(issued_at),
       expires_at: decode_datetime(expires_at),
       revoked_at: decode_datetime(revoked_at)
     }}
  end

  defp decode_idempotency_record_row([
         record_id,
         operation,
         key_hash,
         actor_id,
         session_id,
         service_identity,
         request_fingerprint,
         status,
         response_status,
         response_body_blob,
         resource_type,
         resource_id,
         created_at,
         updated_at,
         expires_at,
         completed_at
       ]) do
    with {:ok, status} <- existing_atom(status),
         {:ok, response_body} <- decode_optional_idempotency_response(response_body_blob) do
      {:ok,
       %{
         id: record_id,
         operation: operation,
         idempotency_key_hash: key_hash,
         actor_id: decode_scope_value(actor_id),
         session_id: decode_scope_value(session_id),
         service_identity: decode_scope_value(service_identity),
         request_fingerprint: request_fingerprint,
         status: status,
         response_status: response_status,
         response_body: response_body,
         resource_type: resource_type,
         resource_id: resource_id,
         created_at: decode_datetime(created_at),
         updated_at: decode_datetime(updated_at),
         expires_at: decode_datetime(expires_at),
         completed_at: decode_datetime(completed_at)
       }}
    end
  end

  defp backfill_window_columns do
    "record_payload"
  end

  defp asset_window_state_columns do
    "record_payload"
  end

  defp asset_freshness_state_columns do
    "record_payload"
  end

  defp target_status_columns do
    "record_payload"
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

  defp asset_freshness_state_filter_columns do
    %{
      asset_ref_module: {:atom, "asset_ref_module"},
      asset_ref_name: {:atom, "asset_ref_name"},
      freshness_key: {:text, "freshness_key"},
      status: {:atom, "status"},
      freshness_version: {:text, "freshness_version"},
      latest_success_run_id: {:text, "latest_success_run_id"},
      latest_attempt_run_id: {:text, "latest_attempt_run_id"},
      latest_attempt_status: {:atom, "latest_attempt_status"},
      manifest_version_id: {:text, "manifest_version_id"},
      manifest_content_hash: {:text, "manifest_content_hash"}
    }
  end

  defp materialization_claim_filter_columns do
    %{
      claim_key: {:text, "claim_key"},
      asset_ref_module: {:atom, "asset_ref_module"},
      asset_ref_name: {:atom, "asset_ref_name"},
      freshness_key: {:text, "freshness_key"},
      input_fingerprint: {:text, "input_fingerprint"},
      run_id: {:text, "run_id"},
      asset_step_id: {:text, "asset_step_id"},
      node_key: {:text, "node_key"},
      runner_execution_id: {:text, "runner_execution_id"},
      manifest_version_id: {:text, "manifest_version_id"},
      manifest_content_hash: {:text, "manifest_content_hash"},
      freshness_version: {:text, "freshness_version"},
      status: {:atom, "status"}
    }
  end

  defp decode_coverage_baseline_row([record_payload]),
    do: CoverageBaselineCodec.decode(record_payload)

  defp decode_backfill_window_row([record_payload]),
    do: BackfillWindowCodec.decode(record_payload)

  defp decode_asset_window_state_row([record_payload]),
    do: AssetWindowStateCodec.decode(record_payload)

  defp decode_asset_freshness_state_row([record_payload]),
    do: AssetFreshnessStateCodec.decode(record_payload)

  defp decode_target_status_row([record_payload]), do: TargetStatusCodec.decode(record_payload)

  defp decode_backfill_progress_row([record_payload]),
    do: ProgressCodec.decode(record_payload)

  defp get_asset_freshness_state_key_chunk(_repo, []), do: {:ok, %{}}

  defp get_asset_freshness_state_key_chunk(repo, keys) do
    clauses =
      keys
      |> Enum.with_index()
      |> Enum.map(fn {_key, index} ->
        base = index * 3

        "(asset_ref_module = ?#{base + 1} AND asset_ref_name = ?#{base + 2} AND freshness_key = ?#{base + 3})"
      end)

    params =
      Enum.flat_map(keys, fn {module, name, freshness_key} ->
        [encode_atom(module), encode_atom(name), freshness_key]
      end)

    sql =
      "SELECT #{asset_freshness_state_columns()} FROM favn_asset_freshness_states WHERE " <>
        Enum.join(clauses, " OR ")

    with {:ok, states} <- decode_rows(repo, sql, params, &decode_asset_freshness_state_row/1) do
      {:ok,
       Map.new(states, fn %AssetFreshnessState{} = state ->
         {{state.asset_ref_module, state.asset_ref_name, state.freshness_key}, state}
       end)}
    end
  end

  defp list_target_status_chunk(_repo, _manifest_version_id, _target_kind, []), do: {:ok, %{}}

  defp list_target_status_chunk(repo, manifest_version_id, target_kind, target_ids) do
    placeholders =
      target_ids
      |> Enum.with_index(3)
      |> Enum.map(fn {_target_id, index} -> "?#{index}" end)
      |> Enum.join(", ")

    sql =
      "SELECT #{target_status_columns()} FROM favn_target_statuses WHERE manifest_version_id = ?1 AND target_kind = ?2 AND target_id IN (#{placeholders})"

    params = [manifest_version_id, encode_atom(target_kind) | target_ids]

    with {:ok, statuses} <- decode_rows(repo, sql, params, &decode_target_status_row/1) do
      {:ok, Map.new(statuses, &{&1.target_id, &1})}
    end
  end

  defp fetch_backfill_window_status(repo, %BackfillWindow{} = window) do
    sql =
      "SELECT status FROM favn_backfill_windows WHERE backfill_run_id = ?1 AND pipeline_module = ?2 AND window_key = ?3 LIMIT 1"

    params = [window.backfill_run_id, encode_atom(window.pipeline_module), window.window_key]

    case SQL.query(repo, sql, params) do
      {:ok, %{rows: [[status]]}} when is_binary(status) -> String.to_existing_atom(status)
      _other -> nil
    end
  end

  defp lock_backfill_window(repo, %BackfillWindow{} = window) do
    sql =
      "UPDATE favn_backfill_windows SET updated_at = updated_at WHERE backfill_run_id = ?1 AND pipeline_module = ?2 AND window_key = ?3"

    params = [window.backfill_run_id, encode_atom(window.pipeline_module), window.window_key]

    case SQL.query(repo, sql, params) do
      {:ok, _result} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp get_backfill_progress_with_repo(repo, backfill_run_id) do
    sql = "SELECT record_payload FROM favn_backfill_progress WHERE backfill_run_id = ?1 LIMIT 1"

    case SQL.query(repo, sql, [backfill_run_id]) do
      {:ok, %{rows: [row]}} -> decode_backfill_progress_row(row)
      {:ok, %{rows: []}} -> rebuild_backfill_progress_with_repo(repo, backfill_run_id)
      {:error, reason} -> {:error, reason}
    end
  end

  defp upsert_backfill_progress_after_window_change(repo, %BackfillWindow{} = window, old_status) do
    case get_backfill_progress_with_repo(repo, window.backfill_run_id) do
      {:ok, %BackfillProgress{} = progress} ->
        with {:ok, next_progress} <-
               BackfillProgress.apply_status_change(
                 progress,
                 old_status,
                 window.status,
                 DateTime.utc_now()
               ),
             :ok <- put_backfill_progress(repo, next_progress) do
          {:ok, next_progress}
        end

      {:error, reason} ->
        case reason do
          {:stale_backfill_progress, _old_status, _new_status, _counts} ->
            rebuild_backfill_progress_with_repo(repo, window.backfill_run_id)

          _other ->
            {:error, reason}
        end
    end
  end

  defp rebuild_backfill_progress_with_repo(repo, backfill_run_id) do
    sql =
      "SELECT status, COUNT(*) FROM favn_backfill_windows WHERE backfill_run_id = ?1 GROUP BY status"

    case SQL.query(repo, sql, [backfill_run_id]) do
      {:ok, %{rows: rows}} ->
        if rows == [] do
          {:error, :not_found}
        else
          counts =
            Map.new(rows, fn [status, count] -> {String.to_existing_atom(status), count} end)

          with {:ok, progress} <-
                 BackfillProgress.from_counts(backfill_run_id, counts, DateTime.utc_now()),
               :ok <- put_backfill_progress(repo, progress) do
            {:ok, progress}
          end
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp put_backfill_progress(repo, %BackfillProgress{} = progress) do
    sql = """
    INSERT INTO favn_backfill_progress (
      backfill_run_id, total_count, pending_count, running_count, ok_count,
      partial_count, error_count, cancelled_count, timed_out_count, status,
      record_payload, updated_at
    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
    ON CONFLICT(backfill_run_id) DO UPDATE SET
      total_count = excluded.total_count,
      pending_count = excluded.pending_count,
      running_count = excluded.running_count,
      ok_count = excluded.ok_count,
      partial_count = excluded.partial_count,
      error_count = excluded.error_count,
      cancelled_count = excluded.cancelled_count,
      timed_out_count = excluded.timed_out_count,
      status = excluded.status,
      record_payload = excluded.record_payload,
      updated_at = excluded.updated_at
    """

    params = backfill_progress_params(progress)

    case SQL.query(repo, sql, params) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_materialization_claim_row([record_payload]),
    do: MaterializationClaimCodec.decode(record_payload)

  defp decode_execution_ownership_row([ownership_payload]),
    do: ExecutionOwnershipCodec.decode(ownership_payload)

  defp decode_execution_lease_row([lease_payload]), do: ExecutionLeaseCodec.decode(lease_payload)

  defp query_and_decode_rows(repo, sql, params, decoder) do
    case SQL.query(repo, sql, params) do
      {:ok, %{rows: rows}} -> decode_rows(rows, decoder)
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_rows(rows, decoder) do
    Enum.reduce_while(rows, {:ok, []}, fn row, {:ok, acc} ->
      case decoder.(row) do
        {:ok, value} -> {:cont, {:ok, [value | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_execution_admission_waiter_row([waiter_payload]),
    do: ExecutionAdmissionWaiterCodec.decode(waiter_payload)

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

  defp decode_cursor_page(repo, sql, params, scan_opts, decoder, cursor_fun)
       when is_function(decoder, 1) and is_function(cursor_fun, 1) do
    params = params ++ [Keyword.fetch!(scan_opts, :limit) + 1]

    with {:ok, rows} <- decode_rows(repo, sql, params, decoder) do
      {:ok, CursorPage.from_fetched(rows, scan_opts, cursor_fun)}
    end
  end

  defp read_filters(filters), do: Keyword.drop(filters, [:limit, :offset])

  defp upsert_coverage_baselines(_repo, []), do: :ok

  defp upsert_coverage_baselines(repo, baselines) do
    sql = """
    INSERT INTO favn_pipeline_coverage_baselines (
      baseline_id, pipeline_module, source_key, segment_key_hash, segment_key_redacted,
      window_kind, timezone, coverage_start_at, coverage_until, created_by_run_id,
      manifest_version_id, status, record_payload, created_at, updated_at
    ) VALUES __VALUES__
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
      record_payload = excluded.record_payload,
      created_at = excluded.created_at,
      updated_at = excluded.updated_at
    """

    baselines = dedupe_last_by(baselines, & &1.baseline_id)
    bulk_query_ok(repo, sql, baselines, &coverage_baseline_params/1)
  end

  defp upsert_backfill_windows(_repo, []), do: :ok

  defp upsert_backfill_windows(repo, windows) do
    sql = """
    INSERT INTO favn_backfill_windows (
      backfill_run_id, child_run_id, pipeline_module, manifest_version_id,
      coverage_baseline_id, window_kind, window_start_at, window_end_at, timezone,
      window_key, status, attempt_count, latest_attempt_run_id, last_success_run_id,
      record_payload, started_at, finished_at, created_at, updated_at
    ) VALUES __VALUES__
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
      record_payload = excluded.record_payload,
      started_at = excluded.started_at,
      finished_at = excluded.finished_at,
      created_at = excluded.created_at,
      updated_at = excluded.updated_at
    """

    windows = dedupe_last_by(windows, &{&1.backfill_run_id, &1.pipeline_module, &1.window_key})
    bulk_query_ok(repo, sql, windows, &backfill_window_params/1)
  end

  defp upsert_asset_window_states(_repo, []), do: :ok

  defp upsert_asset_window_states(repo, states) do
    sql = """
    INSERT INTO favn_asset_window_states (
      asset_ref_module, asset_ref_name, pipeline_module, manifest_version_id,
      window_kind, window_start_at, window_end_at, timezone, window_key, status,
      latest_run_id, latest_parent_run_id, latest_success_run_id, rows_written,
      record_payload, updated_at
    ) VALUES __VALUES__
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
      rows_written = excluded.rows_written,
      record_payload = excluded.record_payload,
      updated_at = excluded.updated_at
    """

    states = dedupe_last_by(states, &{&1.asset_ref_module, &1.asset_ref_name, &1.window_key})
    bulk_query_ok(repo, sql, states, &asset_window_state_params/1)
  end

  defp upsert_target_statuses(_repo, []), do: :ok

  defp upsert_target_statuses(repo, statuses) do
    sql = """
    INSERT INTO favn_target_statuses (
      manifest_version_id, target_kind, target_id, target_ref_text, status,
      latest_run_id, latest_run_status, latest_run_at, latest_success_run_id,
      latest_success_at, latest_failure_run_id, latest_failure_at,
      in_flight_run_id, freshness_status, freshness_key, updated_at, updated_seq,
      record_payload
    ) VALUES __VALUES__
    ON CONFLICT(manifest_version_id, target_kind, target_id) DO UPDATE SET
      target_ref_text = excluded.target_ref_text,
      status = excluded.status,
      latest_run_id = excluded.latest_run_id,
      latest_run_status = excluded.latest_run_status,
      latest_run_at = excluded.latest_run_at,
      latest_success_run_id = excluded.latest_success_run_id,
      latest_success_at = excluded.latest_success_at,
      latest_failure_run_id = excluded.latest_failure_run_id,
      latest_failure_at = excluded.latest_failure_at,
      in_flight_run_id = excluded.in_flight_run_id,
      freshness_status = excluded.freshness_status,
      freshness_key = excluded.freshness_key,
      updated_at = excluded.updated_at,
      updated_seq = excluded.updated_seq,
      record_payload = excluded.record_payload
    """

    statuses = dedupe_last_by(statuses, &{&1.manifest_version_id, &1.target_kind, &1.target_id})
    bulk_query_ok(repo, sql, statuses, &target_status_params/1)
  end

  defp dedupe_last_by(rows, key_fun) when is_function(key_fun, 1) do
    rows
    |> Enum.reverse()
    |> Enum.uniq_by(key_fun)
    |> Enum.reverse()
  end

  defp bulk_query_ok(repo, sql_template, rows, params_fun) do
    rows
    |> Enum.chunk_every(@read_model_chunk_size)
    |> Enum.reduce_while(:ok, fn chunk, :ok ->
      params = Enum.map(chunk, params_fun)
      sql = String.replace(sql_template, "__VALUES__", sqlite_values_sql(params))

      case SQL.query(repo, sql, List.flatten(params)) do
        {:ok, _} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp sqlite_values_sql(rows) do
    rows
    |> Enum.map_reduce(1, fn row, index ->
      placeholders = Enum.map(index..(index + length(row) - 1), &"?#{&1}")
      {"(" <> Enum.join(placeholders, ", ") <> ")", index + length(row)}
    end)
    |> elem(0)
    |> Enum.join(", ")
  end

  defp coverage_baseline_params(%CoverageBaseline{} = baseline) do
    [
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
      encode_coverage_baseline(baseline),
      encode_datetime(baseline.created_at),
      encode_datetime(baseline.updated_at)
    ]
  end

  defp backfill_window_params(%BackfillWindow{} = window) do
    [
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
      encode_backfill_window(window),
      encode_datetime(window.started_at),
      encode_datetime(window.finished_at),
      encode_datetime(window.created_at),
      encode_datetime(window.updated_at)
    ]
  end

  defp asset_window_state_params(%AssetWindowState{} = state) do
    [
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
      state.rows_written,
      encode_asset_window_state(state),
      encode_datetime(state.updated_at)
    ]
  end

  defp target_status_params(%TargetStatus{} = status) do
    [
      status.manifest_version_id,
      encode_atom(status.target_kind),
      status.target_id,
      status.target_ref_text,
      encode_atom(status.status),
      status.latest_run_id,
      encode_optional_atom(status.latest_run_status),
      encode_datetime(status.latest_run_at),
      status.latest_success_run_id,
      encode_datetime(status.latest_success_at),
      status.latest_failure_run_id,
      encode_datetime(status.latest_failure_at),
      status.in_flight_run_id,
      encode_optional_atom(status.freshness_status),
      status.freshness_key,
      encode_datetime(status.updated_at),
      status.updated_seq,
      encode_target_status(status)
    ]
  end

  defp query_ok(repo, sql, params) do
    case SQL.query(repo, sql, params) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

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

  defp target_status_in_scope?(%TargetStatus{manifest_version_id: id}, {:manifest_version, id}),
    do: true

  defp target_status_in_scope?(
         %TargetStatus{manifest_version_id: id, target_kind: kind},
         {:manifest_version, id, kind}
       ),
       do: true

  defp target_status_in_scope?(_status, _scope), do: false

  defp delete_target_status_scope(repo, {:manifest_version, manifest_version_id}) do
    query_ok(repo, "DELETE FROM favn_target_statuses WHERE manifest_version_id = ?1", [
      manifest_version_id
    ])
  end

  defp delete_target_status_scope(repo, {:manifest_version, manifest_version_id, target_kind}) do
    query_ok(
      repo,
      "DELETE FROM favn_target_statuses WHERE manifest_version_id = ?1 AND target_kind = ?2",
      [manifest_version_id, encode_atom(target_kind)]
    )
  end

  defp delete_replacement_scope(repo, table, kind, scope) do
    case replacement_scope_filter(kind, scope) do
      {:ok, nil, []} ->
        query_ok(repo, "DELETE FROM #{table}", [])

      {:ok, where_sql, params} ->
        query_ok(repo, "DELETE FROM #{table} WHERE #{where_sql}", params)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp replacement_scope_filter(_kind, :all), do: {:ok, nil, []}

  defp replacement_scope_filter(:coverage, {:backfill_run, id}),
    do: {:ok, "created_by_run_id = ?1", [id]}

  defp replacement_scope_filter(:window, {:backfill_run, id}),
    do: {:ok, "backfill_run_id = ?1", [id]}

  defp replacement_scope_filter(:asset_state, {:backfill_run, id}),
    do: {:ok, "latest_parent_run_id = ?1", [id]}

  defp replacement_scope_filter(kind, {:pipeline, module})
       when kind in [:coverage, :window, :asset_state],
       do: {:ok, "pipeline_module = ?1", [encode_atom(module)]}

  defp replacement_scope_filter(_kind, scope),
    do: {:error, {:unsupported_replacement_scope, scope}}

  defp delete_replacement_progress(repo, :all),
    do: query_ok(repo, "DELETE FROM favn_backfill_progress", [])

  defp delete_replacement_progress(repo, {:backfill_run, id}),
    do: query_ok(repo, "DELETE FROM favn_backfill_progress WHERE backfill_run_id = ?1", [id])

  defp delete_replacement_progress(_repo, {:pipeline, _module}), do: :ok

  defp affected_backfill_ids_for_scope(repo, :all) do
    query_distinct_backfill_ids(
      repo,
      "SELECT DISTINCT backfill_run_id FROM favn_backfill_windows",
      []
    )
  end

  defp affected_backfill_ids_for_scope(_repo, {:backfill_run, id}), do: {:ok, [id]}

  defp affected_backfill_ids_for_scope(repo, {:pipeline, module}) do
    query_distinct_backfill_ids(
      repo,
      "SELECT DISTINCT backfill_run_id FROM favn_backfill_windows WHERE pipeline_module = ?1",
      [encode_atom(module)]
    )
  end

  defp query_distinct_backfill_ids(repo, sql, params) do
    case SQL.query(repo, sql, params) do
      {:ok, %{rows: rows}} -> {:ok, Enum.map(rows, fn [id] -> id end)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp rebuild_backfill_progress_for_ids(repo, ids) do
    ids
    |> Enum.uniq()
    |> Enum.reduce_while(:ok, fn backfill_run_id, :ok ->
      case rebuild_backfill_progress_with_repo(repo, backfill_run_id) do
        {:ok, %BackfillProgress{}} ->
          {:cont, :ok}

        {:error, :not_found} ->
          delete_backfill_progress(repo, backfill_run_id) |> reduce_query_result()

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
  end

  defp delete_backfill_progress(repo, backfill_run_id) do
    query_ok(repo, "DELETE FROM favn_backfill_progress WHERE backfill_run_id = ?1", [
      backfill_run_id
    ])
  end

  defp reduce_query_result(:ok), do: {:cont, :ok}
  defp reduce_query_result({:error, reason}), do: {:halt, {:error, reason}}

  defp append_cursor_sql(where_sql, params, "", []), do: {where_sql, params}

  defp append_cursor_sql("", params, cursor_sql, cursor_params),
    do: {" WHERE #{cursor_sql}", params ++ cursor_params}

  defp append_cursor_sql(where_sql, params, cursor_sql, cursor_params),
    do: {where_sql <> " AND #{cursor_sql}", params ++ cursor_params}

  defp backfill_window_cursor_sql(nil, _offset), do: {:ok, {"", []}}

  defp backfill_window_cursor_sql(
         %{
           kind: :backfill_window,
           window_start_at: %DateTime{} = window_start_at,
           backfill_run_id: backfill_run_id,
           pipeline_module: pipeline_module,
           window_key: window_key
         },
         offset
       )
       when is_binary(backfill_run_id) and is_atom(pipeline_module) and is_binary(window_key) do
    sql =
      "(window_start_at > ?#{offset + 1} OR (window_start_at = ?#{offset + 1} AND backfill_run_id > ?#{offset + 2}) OR (window_start_at = ?#{offset + 1} AND backfill_run_id = ?#{offset + 2} AND pipeline_module > ?#{offset + 3}) OR (window_start_at = ?#{offset + 1} AND backfill_run_id = ?#{offset + 2} AND pipeline_module = ?#{offset + 3} AND window_key > ?#{offset + 4}))"

    {:ok,
     {sql,
      [
        encode_datetime(window_start_at),
        backfill_run_id,
        encode_atom(pipeline_module),
        window_key
      ]}}
  end

  defp backfill_window_cursor_sql(_cursor, _offset), do: {:error, :invalid_cursor_pagination}

  defp backfill_window_cursor!(%BackfillWindow{} = window) do
    %{
      kind: :backfill_window,
      window_start_at: window.window_start_at,
      backfill_run_id: window.backfill_run_id,
      pipeline_module: window.pipeline_module,
      window_key: window.window_key
    }
  end

  defp asset_freshness_cursor_sql(nil, _offset), do: {:ok, {"", []}}

  defp asset_freshness_cursor_sql(
         %{
           kind: :asset_freshness_state,
           updated_at: %DateTime{} = updated_at,
           asset_ref_module: asset_ref_module,
           asset_ref_name: asset_ref_name,
           freshness_key: freshness_key
         },
         offset
       )
       when is_atom(asset_ref_module) and is_atom(asset_ref_name) and is_binary(freshness_key) do
    sql =
      "(updated_at < ?#{offset + 1} OR (updated_at = ?#{offset + 1} AND asset_ref_module > ?#{offset + 2}) OR (updated_at = ?#{offset + 1} AND asset_ref_module = ?#{offset + 2} AND asset_ref_name > ?#{offset + 3}) OR (updated_at = ?#{offset + 1} AND asset_ref_module = ?#{offset + 2} AND asset_ref_name = ?#{offset + 3} AND freshness_key > ?#{offset + 4}))"

    {:ok,
     {sql,
      [
        encode_datetime(updated_at),
        encode_atom(asset_ref_module),
        encode_atom(asset_ref_name),
        freshness_key
      ]}}
  end

  defp asset_freshness_cursor_sql(_cursor, _offset), do: {:error, :invalid_cursor_pagination}

  defp asset_freshness_cursor!(%AssetFreshnessState{} = state) do
    %{
      kind: :asset_freshness_state,
      updated_at: state.updated_at,
      asset_ref_module: state.asset_ref_module,
      asset_ref_name: state.asset_ref_name,
      freshness_key: state.freshness_key
    }
  end

  defp page_opts(filters), do: Page.normalize_opts(filters)

  defp waiter_limit(opts) do
    case Keyword.get(opts, :limit, 50) do
      limit when is_integer(limit) and limit > 0 -> limit
      _other -> 50
    end
  end

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

  defp encode_optional_atom(nil), do: nil
  defp encode_optional_atom(value) when is_atom(value), do: encode_atom(value)

  defp existing_atom(value) when is_binary(value) do
    {:ok, String.to_existing_atom(value)}
  rescue
    ArgumentError -> {:error, {:unknown_atom, value}}
  end

  defp encode_datetime(nil), do: nil
  defp encode_datetime(%DateTime{} = value), do: DateTime.to_iso8601(value)

  defp idempotency_record_params(record) do
    [
      Map.fetch!(record, :id),
      Map.fetch!(record, :operation),
      Map.fetch!(record, :idempotency_key_hash),
      encode_scope_value(Map.get(record, :actor_id)),
      encode_scope_value(Map.get(record, :session_id)),
      encode_scope_value(Map.get(record, :service_identity)),
      Map.fetch!(record, :request_fingerprint),
      encode_atom(Map.fetch!(record, :status)),
      Map.get(record, :response_status),
      encode_optional_idempotency_response(
        Map.get(record, :response_body),
        Map.fetch!(record, :operation)
      ),
      Map.get(record, :resource_type),
      Map.get(record, :resource_id),
      encode_datetime(Map.fetch!(record, :created_at)),
      encode_datetime(Map.fetch!(record, :updated_at)),
      encode_datetime(Map.fetch!(record, :expires_at)),
      encode_datetime(Map.get(record, :completed_at))
    ]
  end

  defp encode_scope_value(nil), do: ""
  defp encode_scope_value(value) when is_binary(value), do: value

  defp decode_scope_value(""), do: nil
  defp decode_scope_value(value), do: value

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
        :ok ->
          case refresh_execution_group_summary(repo, RunQuery.root_execution_group_id(run)) do
            :ok -> {:ok, :ok}
            {:error, reason} -> repo.rollback(reason)
          end

        {:error, reason} ->
          repo.rollback(reason)
      end
    end)
    |> case do
      {:ok, :ok} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp guarded_put_run(repo, run) do
    with {:ok, updated_seq} <- next_updated_seq(repo) do
      query_metadata = RunQuery.metadata(run)

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
          run_blob,
          root_execution_group_id,
          parent_run_id,
          root_run_id,
          submit_kind,
          trigger_type,
          asset_ref_text,
          target_refs_text,
          window_key,
          pipeline_submit_ref_text
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19)
        ON CONFLICT(run_id) DO UPDATE SET
          manifest_version_id = excluded.manifest_version_id,
          manifest_content_hash = excluded.manifest_content_hash,
          status = excluded.status,
          event_seq = excluded.event_seq,
          snapshot_hash = excluded.snapshot_hash,
          updated_seq = excluded.updated_seq,
          inserted_at = excluded.inserted_at,
          updated_at = excluded.updated_at,
          run_blob = excluded.run_blob,
          root_execution_group_id = excluded.root_execution_group_id,
          parent_run_id = excluded.parent_run_id,
          root_run_id = excluded.root_run_id,
          submit_kind = excluded.submit_kind,
          trigger_type = excluded.trigger_type,
          asset_ref_text = excluded.asset_ref_text,
          target_refs_text = excluded.target_refs_text,
          window_key = excluded.window_key,
          pipeline_submit_ref_text = excluded.pipeline_submit_ref_text
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
        encode_run_snapshot(run),
        query_metadata.root_execution_group_id,
        query_metadata.parent_run_id,
        query_metadata.root_run_id,
        query_metadata.submit_kind,
        query_metadata.trigger_type,
        query_metadata.asset_ref_text,
        query_metadata.target_refs_text,
        query_metadata.window_key,
        query_metadata.pipeline_submit_ref_text
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
        VALUES (?1, ?2, ?3, ?4, ?5)
        """

      case SQL.query(repo, sql, [
             run_id,
             event.sequence,
             global_sequence,
             event.occurred_at,
             encode_run_event(event)
           ]) do
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
    encoded_state = encode_scheduler_state(normalized_state)
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

  defp next_run_event_global_sequence(repo) do
    sql =
      """
      INSERT INTO favn_counters (name, value)
      VALUES (?1, 1)
      ON CONFLICT(name) DO UPDATE SET value = value + 1
      RETURNING value
      """

    case SQL.query(repo, sql, [@run_event_global_sequence_key]) do
      {:ok, %{rows: [[value]]}} when is_integer(value) -> {:ok, value}
      {:ok, _} -> {:error, :invalid_counter_value}
      {:error, reason} -> {:error, reason}
    end
  end

  defp next_log_global_sequence(repo) do
    sql =
      """
      INSERT INTO favn_counters (name, value)
      VALUES (?1, 1)
      ON CONFLICT(name) DO UPDATE SET value = value + 1
      RETURNING value
      """

    case SQL.query(repo, sql, [@log_global_sequence_key]) do
      {:ok, %{rows: [[value]]}} when is_integer(value) -> {:ok, value}
      {:ok, _} -> {:error, :invalid_counter_value}
      {:error, reason} -> {:error, reason}
    end
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

  defp persist_log_entries_in_transaction(repo, entries) do
    Enum.reduce_while(entries, {:ok, []}, fn entry, {:ok, acc} ->
      case persist_log_entry(repo, entry) do
        {:ok, persisted} -> {:cont, {:ok, [persisted | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, persisted} -> {:ok, Enum.reverse(persisted)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp persist_log_entry(repo, entry) do
    with {:ok, global_sequence} <- next_log_global_sequence(repo) do
      entry = LogEntryCodec.assign_global_sequence(entry, global_sequence)
      {node_key_hash, node_key_blob} = LogEntryCodec.node_key_storage(Map.get(entry, :node_key))

      {asset_ref_key, asset_ref_blob} =
        LogEntryCodec.asset_ref_storage(Map.get(entry, :asset_ref))

      now = DateTime.utc_now()

      sql =
        """
        INSERT INTO favn_log_entries (
          id, global_sequence, run_id, asset_step_id, node_key_hash, node_key_blob,
          asset_ref_key, asset_ref_blob, runner_execution_id, attempt, producer_id,
          producer_sequence, occurred_at, level, source, stream, message, metadata_blob,
          log_blob, truncated, inserted_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21)
        """

      params = [
        entry.id,
        entry.global_sequence,
        entry.run_id,
        entry.asset_step_id,
        node_key_hash,
        node_key_blob,
        asset_ref_key,
        asset_ref_blob,
        entry.runner_execution_id,
        entry.attempt,
        entry.producer_id,
        entry.producer_sequence,
        entry.occurred_at,
        encode_optional_atom(entry.level),
        encode_optional_atom(entry.source),
        encode_optional_atom(entry.stream),
        entry.message,
        Jason.encode!(JsonSafe.data(entry.metadata || %{})),
        encode_log_entry(entry),
        entry.truncated == true,
        now
      ]

      case SQL.query(repo, sql, params) do
        {:ok, _} ->
          {:ok, entry}

        {:error, %{sqlite: %{code: :constraint_failed}}} ->
          resolve_log_entry_conflict(repo, entry)

        {:error, %Exqlite.Error{message: message} = reason} when is_binary(message) ->
          if String.contains?(message, "UNIQUE constraint failed") do
            resolve_log_entry_conflict(repo, entry)
          else
            {:error, reason}
          end

        {:error, reason} ->
          {:error, reason}
      end
    else
      {:error, reason} -> {:error, reason}
      other -> {:error, {:invalid_log_entry_insert, other}}
    end
  end

  defp fetch_log_entry_by_producer(_repo, %{
         producer_id: producer_id,
         producer_sequence: producer_sequence
       })
       when not is_binary(producer_id) or not is_integer(producer_sequence),
       do: {:ok, nil}

  defp fetch_log_entry_by_producer(repo, entry) do
    sql =
      """
      SELECT log_blob
      FROM favn_log_entries
      WHERE producer_id = ?1 AND producer_sequence = ?2
      LIMIT 1
      """

    case SQL.query(repo, sql, [entry.producer_id, entry.producer_sequence]) do
      {:ok, %{rows: [[blob]]}} -> LogEntryCodec.decode(blob)
      {:ok, %{rows: []}} -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  defp resolve_log_entry_conflict(repo, entry) do
    with {:ok, existing} <- fetch_log_entry_by_producer(repo, entry) do
      case existing do
        nil -> {:error, :log_entry_conflict}
        existing -> {:ok, existing}
      end
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

  defp fetch_manifest_record_by_content_hash(repo, content_hash) do
    sql =
      """
      SELECT manifest_version_id, content_hash, schema_version, runner_contract_version, serialization_format, manifest_json, inserted_at
      FROM favn_manifest_versions
      WHERE content_hash = ?1
      LIMIT 1
      """

    case SQL.query(repo, sql, [content_hash]) do
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
      "SELECT global_sequence, event_blob FROM favn_run_events WHERE run_id = ?1 AND sequence = ?2 LIMIT 1"

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
           LIMIT ?1
         )
         ORDER BY global_sequence ASC
         """, [limit]}

      is_integer(after_sequence) and after_sequence == 0 ->
        {:ok,
         """
         SELECT global_sequence, event_blob
         FROM favn_run_events
         WHERE global_sequence IS NOT NULL
         ORDER BY global_sequence ASC
         LIMIT ?1
         """, [limit]}

      is_integer(after_sequence) and after_sequence > 0 ->
        case global_sequence_exists?(repo, after_sequence) do
          true ->
            {:ok,
             """
             SELECT global_sequence, event_blob
             FROM favn_run_events
             WHERE global_sequence > ?1
             ORDER BY global_sequence ASC
             LIMIT ?2
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
    sql = "SELECT 1 FROM favn_run_events WHERE global_sequence = ?1 LIMIT 1"

    case SQL.query(repo, sql, [sequence]) do
      {:ok, %{rows: [[1]]}} -> true
      {:ok, %{rows: []}} -> false
      {:error, reason} -> {:error, reason}
    end
  end

  defp list_logs_query(filter, opts) do
    with {:ok, {where_sql, params}} <- build_log_filter_sql(filter) do
      limit_placeholder = "?#{length(params) + 1}"
      offset_placeholder = "?#{length(params) + 2}"
      order = log_order_sql(opts)

      {:ok,
       """
       SELECT log_blob
       FROM favn_log_entries
       #{where_sql}
       ORDER BY global_sequence #{order}
       LIMIT #{limit_placeholder} OFFSET #{offset_placeholder}
       """, params}
    end
  end

  defp log_order_sql(opts) do
    case Keyword.get(opts, :order, :asc) do
      :desc -> "DESC"
      "desc" -> "DESC"
      _other -> "ASC"
    end
  end

  defp replay_logs_query(filter, after_sequence, opts) do
    with {:ok, {where_sql, params}} <- build_log_filter_sql(filter) do
      limit = Keyword.get(opts, :limit, 200)
      prefix = if where_sql == "", do: " WHERE ", else: where_sql <> " AND "
      after_placeholder = "?#{length(params) + 1}"
      limit_placeholder = "?#{length(params) + 2}"

      {:ok,
       """
       SELECT log_blob
       FROM favn_log_entries
       #{prefix}global_sequence > #{after_placeholder}
       ORDER BY global_sequence ASC
       LIMIT #{limit_placeholder}
       """, params ++ [after_sequence, limit]}
    end
  end

  defp scan_logs_query(filter, after_sequence) do
    with {:ok, {where_sql, params}} <- build_log_filter_sql(filter) do
      prefix = if where_sql == "", do: " WHERE ", else: where_sql <> " AND "
      after_placeholder = "?#{length(params) + 1}"

      {:ok,
       """
       SELECT log_blob
       FROM favn_log_entries
       #{prefix}global_sequence > #{after_placeholder}
       ORDER BY global_sequence ASC
       """, params ++ [after_sequence]}
    end
  end

  defp log_entry_cursor!(%Favn.Log.Entry{} = entry) do
    %{kind: :log_entry, global_sequence: entry.global_sequence}
  end

  defp build_log_filter_sql(filter) do
    filter
    |> normalize_log_filter()
    |> Enum.reduce_while({:ok, [], []}, fn {key, value}, {:ok, clauses, params} ->
      case log_filter_clause(key, value, length(params) + 1) do
        {:ok, clause, encoded} ->
          {:cont, {:ok, [clause | clauses], params ++ List.wrap(encoded)}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, [], []} ->
        {:ok, {"", []}}

      {:ok, clauses, params} ->
        {:ok, {"WHERE " <> Enum.join(Enum.reverse(clauses), " AND "), params}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp normalize_log_filter(filter) when is_list(filter),
    do: Keyword.drop(filter, [:limit, :offset])

  defp normalize_log_filter(%_{} = filter),
    do: filter |> Map.from_struct() |> normalize_log_filter()

  defp normalize_log_filter(filter) when is_map(filter) do
    filter
    |> Enum.map(fn {key, value} -> {normalize_log_filter_key(key), value} end)
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
  end

  defp normalize_log_filter(_filter), do: []

  defp normalize_log_filter_key(key) when is_atom(key), do: key

  defp normalize_log_filter_key(key) when is_binary(key) do
    Enum.find(@log_filter_keys, key, &(Atom.to_string(&1) == key)) || key
  end

  defp log_filter_clause(:run_id, value, next), do: {:ok, "run_id = ?#{next}", value}

  defp log_filter_clause(:asset_step_id, value, next),
    do: {:ok, "asset_step_id = ?#{next}", value}

  defp log_filter_clause(:runner_execution_id, value, next),
    do: {:ok, "runner_execution_id = ?#{next}", value}

  defp log_filter_clause(:level, value, next),
    do: {:ok, "level = ?#{next}", encode_log_filter_atom(value)}

  defp log_filter_clause(:source, value, next),
    do: {:ok, "source = ?#{next}", encode_log_filter_atom(value)}

  defp log_filter_clause(:stream, value, next),
    do: {:ok, "stream = ?#{next}", encode_log_filter_atom(value)}

  defp log_filter_clause(:levels, [], next), do: {:ok, "1 = ?#{next}", 1}

  defp log_filter_clause(:levels, values, next) when is_list(values) do
    placeholders = Enum.map_join(0..(length(values) - 1), ",", &"?#{next + &1}")
    {:ok, "level IN (#{placeholders})", Enum.map(values, &encode_log_filter_atom/1)}
  end

  defp log_filter_clause(:sources, [], next), do: {:ok, "1 = ?#{next}", 1}

  defp log_filter_clause(:sources, values, next) when is_list(values) do
    placeholders = Enum.map_join(0..(length(values) - 1), ",", &"?#{next + &1}")
    {:ok, "source IN (#{placeholders})", Enum.map(values, &encode_log_filter_atom/1)}
  end

  defp log_filter_clause(:since, %DateTime{} = value, next),
    do: {:ok, "occurred_at >= ?#{next}", value}

  defp log_filter_clause(:until, %DateTime{} = value, next),
    do: {:ok, "occurred_at <= ?#{next}", value}

  defp log_filter_clause(:asset_ref, value, next) do
    {key, _blob} = LogEntryCodec.asset_ref_storage(value)
    {:ok, "asset_ref_key = ?#{next}", key}
  end

  defp log_filter_clause(:node_key, value, next) do
    {hash, _blob} = LogEntryCodec.node_key_storage(value)
    {:ok, "node_key_hash = ?#{next}", hash}
  end

  defp log_filter_clause(key, _value, _next), do: {:error, {:unsupported_filter, key}}

  defp encode_log_filter_atom(value) when is_atom(value), do: Atom.to_string(value)
  defp encode_log_filter_atom(value), do: value

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

  defp validate_log_replay_limit(limit) when is_integer(limit) and limit > 0, do: :ok
  defp validate_log_replay_limit(_limit), do: {:error, :cursor_invalid}

  defp decode_log_entry_rows(rows) do
    Enum.reduce_while(rows, {:ok, []}, fn row, {:ok, acc} ->
      case decode_log_entry_row(row) do
        {:ok, entry} -> {:cont, {:ok, [entry | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, entries} -> {:ok, Enum.reverse(entries)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_log_entry_row([payload]), do: LogEntryCodec.decode(payload)

  defp refresh_execution_group_summaries(repo, group_ids) do
    group_ids
    |> Enum.uniq()
    |> Enum.reduce_while(:ok, fn group_id, :ok ->
      case refresh_execution_group_summary(repo, group_id) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp execution_group_ids_for_rebuild(repo) do
    with :ok <- repair_missing_run_query_metadata(repo) do
      sql =
        "SELECT DISTINCT root_execution_group_id FROM favn_runs WHERE root_execution_group_id IS NOT NULL ORDER BY root_execution_group_id"

      case SQL.query(repo, sql, []) do
        {:ok, %{rows: rows}} -> {:ok, Enum.map(rows, fn [group_id] -> group_id end)}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp refresh_execution_group_summary(_repo, nil), do: :ok

  defp refresh_execution_group_summary(repo, group_id) when is_binary(group_id) do
    with {:ok, runs} <- fetch_execution_group_runs(repo, group_id),
         {:ok, windows} <- fetch_execution_group_windows(repo, group_id) do
      case ExecutionGroupSummary.build(runs, windows) do
        {:ok, summary} -> upsert_execution_group_summary(repo, summary)
        {:error, :empty_execution_group} -> delete_execution_group_summary(repo, group_id)
      end
    else
      {:error, {:missing_manifest_version, _manifest_version_id}} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp fetch_execution_group_runs(repo, group_id) do
    sql =
      run_snapshot_select() <>
        " WHERE r.root_execution_group_id = ?1 ORDER BY CASE WHEN r.run_id = r.root_execution_group_id THEN 0 ELSE 1 END, r.inserted_at ASC, r.run_id ASC"

    case SQL.query(repo, sql, [group_id]) do
      {:ok, %{rows: rows}} -> decode_run_rows(rows)
      {:error, reason} -> {:error, reason}
    end
  end

  defp fetch_execution_group_windows(repo, group_id) do
    sql =
      "SELECT #{backfill_window_columns()} FROM favn_backfill_windows WHERE backfill_run_id = ?1"

    decode_rows(repo, sql, [group_id], &decode_backfill_window_row/1)
  end

  defp upsert_execution_group_summary(repo, summary) do
    with {:ok, activity_seq} <- execution_group_activity_seq(repo, summary.id) do
      sql = """
      INSERT INTO favn_execution_group_summaries (
        group_id, root_run_id, root_status, status, trigger_type, target_refs_text,
        has_window, failed, running, activity_seq, summary_blob, updated_at
      ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
      ON CONFLICT(group_id) DO UPDATE SET
        root_run_id = excluded.root_run_id,
        root_status = excluded.root_status,
        status = excluded.status,
        trigger_type = excluded.trigger_type,
        target_refs_text = excluded.target_refs_text,
        has_window = excluded.has_window,
        failed = excluded.failed,
        running = excluded.running,
        activity_seq = excluded.activity_seq,
        summary_blob = excluded.summary_blob,
        updated_at = excluded.updated_at
      """

      params = [
        summary.id,
        summary.root_execution_group_id,
        encode_atom(summary.root_status),
        encode_atom(summary.status),
        encode_atom(summary.trigger_type),
        Enum.join(summary.target_assets, "\n"),
        summary.total_windows > 0,
        summary.failure_count > 0,
        summary.active?,
        activity_seq,
        ExecutionGroupSummary.encode(summary),
        encode_datetime(DateTime.utc_now())
      ]

      query_ok(repo, sql, params)
    end
  end

  defp execution_group_activity_seq(repo, group_id) do
    case SQL.query(
           repo,
           "SELECT COALESCE(MAX(updated_seq), 0) FROM favn_runs WHERE root_execution_group_id = ?1",
           [group_id]
         ) do
      {:ok, %{rows: [[seq]]}} when is_integer(seq) -> {:ok, seq}
      {:error, reason} -> {:error, reason}
    end
  end

  defp delete_execution_group_summary(repo, group_id) do
    query_ok(repo, "DELETE FROM favn_execution_group_summaries WHERE group_id = ?1", [group_id])
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
    case RunEventCodec.decode(payload) do
      {:ok, event} when is_map(event) -> {:ok, Map.put(event, :global_sequence, global_sequence)}
      {:error, reason} -> {:error, reason}
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
    limit = Keyword.get(run_opts, :limit)
    filters = Keyword.take(run_opts, [:status, :manifest_version_id])

    {where_sql, params} = run_filter_sql(filters)
    limit_sql = if is_integer(limit) and limit > 0, do: " LIMIT ?#{length(params) + 1}", else: ""
    params = if limit_sql == "", do: params, else: params ++ [limit]

    {run_snapshot_select() <>
       where_sql <> " ORDER BY r.updated_seq DESC, r.run_id DESC" <> limit_sql, params}
  end

  defp list_target_runs_query(manifest_version_id, target_kind, target_ref, run_opts) do
    target_ref_text = RunQuery.public_ref(target_ref)
    limit = Keyword.get(run_opts, :limit)

    filters =
      run_opts
      |> Keyword.take([:status])
      |> Keyword.put(:manifest_version_id, manifest_version_id)

    {where_sql, params} = run_filter_sql(filters)

    {target_sql, params} =
      target_run_filter_sql(target_kind, target_ref_text, params)

    limit_sql = if is_integer(limit) and limit > 0, do: " LIMIT ?#{length(params) + 1}", else: ""
    params = if limit_sql == "", do: params, else: params ++ [limit]

    {run_snapshot_select() <>
       where_sql <> target_sql <> " ORDER BY r.updated_seq DESC, r.run_id DESC" <> limit_sql,
     params}
  end

  defp execution_groups_query(group_opts) do
    with {:ok, page_opts} <- Page.normalize_opts(group_opts) do
      filters = Keyword.drop(group_opts, [:limit, :offset, :sort])
      {where_sql, params} = execution_group_filter_sql(filters)
      sort_sql = execution_group_sort_sql(Keyword.get(group_opts, :sort, :started_desc))
      limit_placeholder = "?#{length(params) + 1}"
      offset_placeholder = "?#{length(params) + 2}"

      {:ok,
       """
       SELECT group_id
       FROM (
         SELECT
           r.root_execution_group_id AS group_id,
           MAX(r.updated_seq) AS activity_seq,
           MAX(CASE WHEN r.status IN ('error', 'partial', 'cancelled', 'timed_out') THEN 1 ELSE 0 END) AS failed,
           MAX(CASE WHEN r.status IN ('pending', 'running') THEN 1 ELSE 0 END) AS running,
           MAX(CASE WHEN r.run_id = r.root_execution_group_id THEN r.status ELSE NULL END) AS root_status,
           MAX(CASE WHEN r.run_id = r.root_execution_group_id THEN r.trigger_type ELSE NULL END) AS root_trigger_type,
           MAX(CASE WHEN r.run_id = r.root_execution_group_id THEN r.target_refs_text ELSE NULL END) AS root_targets,
           MAX(CASE WHEN r.window_key IS NOT NULL AND r.window_key != '' THEN 1 ELSE 0 END) AS has_window
         FROM favn_runs AS r
         GROUP BY r.root_execution_group_id
       ) AS groups
       #{where_sql}
       ORDER BY #{sort_sql}, group_id DESC
       LIMIT #{limit_placeholder} OFFSET #{offset_placeholder}
       """, params ++ [Keyword.fetch!(page_opts, :limit) + 1, Keyword.fetch!(page_opts, :offset)],
       page_opts}
    end
  end

  defp execution_group_summaries_query(group_opts) do
    with {:ok, page_opts} <- Page.normalize_opts(group_opts) do
      filters = Keyword.drop(group_opts, [:limit, :offset, :sort])
      {where_sql, params} = execution_group_summary_filter_sql(filters)
      sort_sql = execution_group_summary_sort_sql(Keyword.get(group_opts, :sort, :started_desc))
      limit_placeholder = "?#{length(params) + 1}"
      offset_placeholder = "?#{length(params) + 2}"

      {:ok,
       """
       SELECT summary_blob
       FROM favn_execution_group_summaries
       #{where_sql}
       ORDER BY #{sort_sql}, group_id DESC
       LIMIT #{limit_placeholder} OFFSET #{offset_placeholder}
       """, params ++ [Keyword.fetch!(page_opts, :limit) + 1, Keyword.fetch!(page_opts, :offset)],
       page_opts}
    end
  end

  defp execution_group_summary_filter_sql(filters) do
    filters
    |> Enum.reduce({[], []}, fn
      {:status, status}, {clauses, params} when not is_nil(status) ->
        {clauses ++ ["root_status = ?#{length(params) + 1}"], params ++ [encode_atom(status)]}

      {:trigger_type, trigger}, {clauses, params} when not is_nil(trigger) ->
        {clauses ++ ["trigger_type = ?#{length(params) + 1}"], params ++ [encode_atom(trigger)]}

      {:target_asset, target}, {clauses, params} when is_binary(target) and target != "" ->
        {clauses ++
           [
             "instr(char(10) || target_refs_text || char(10), char(10) || ?#{length(params) + 1} || char(10)) > 0"
           ], params ++ [target]}

      {:search, search}, {clauses, params} when is_binary(search) and search != "" ->
        placeholder = "?#{length(params) + 1}"

        {clauses ++
           [
             "(LOWER(group_id) LIKE #{placeholder} OR LOWER(target_refs_text) LIKE #{placeholder} OR LOWER(trigger_type) LIKE #{placeholder})"
           ], params ++ ["%#{String.downcase(search)}%"]}

      {:window, :has_window}, {clauses, params} ->
        {clauses ++ ["has_window = 1"], params}

      {:window, :no_window}, {clauses, params} ->
        {clauses ++ ["has_window = 0"], params}

      {:only_failed, true}, {clauses, params} ->
        {clauses ++ ["failed = 1"], params}

      {:only_running, true}, {clauses, params} ->
        {clauses ++ ["running = 1"], params}

      {:only_incomplete, true}, {clauses, params} ->
        {clauses ++ ["running = 1"], params}

      _other, acc ->
        acc
    end)
    |> case do
      {[], params} -> {"", params}
      {clauses, params} -> {"WHERE " <> Enum.join(clauses, " AND "), params}
    end
  end

  defp execution_group_summary_sort_sql(:failed_first), do: "failed DESC, activity_seq DESC"
  defp execution_group_summary_sort_sql(:running_first), do: "running DESC, activity_seq DESC"

  defp execution_group_summary_sort_sql(:status_priority),
    do: "failed DESC, running DESC, activity_seq DESC"

  defp execution_group_summary_sort_sql(_sort), do: "activity_seq DESC"

  defp execution_group_filter_sql(filters) do
    filters
    |> Enum.reduce({[], []}, fn
      {:status, status}, {clauses, params} when not is_nil(status) ->
        {clauses ++ ["root_status = ?#{length(params) + 1}"], params ++ [Atom.to_string(status)]}

      {:trigger_type, trigger}, {clauses, params} when not is_nil(trigger) ->
        {clauses ++ ["root_trigger_type = ?#{length(params) + 1}"],
         params ++ [Atom.to_string(trigger)]}

      {:target_asset, target}, {clauses, params} when is_binary(target) and target != "" ->
        {clauses ++
           [
             "instr(char(10) || root_targets || char(10), char(10) || ?#{length(params) + 1} || char(10)) > 0"
           ], params ++ [target]}

      {:search, search}, {clauses, params} when is_binary(search) and search != "" ->
        placeholder = "?#{length(params) + 1}"

        {clauses ++
           [
             "(group_id LIKE #{placeholder} OR root_targets LIKE #{placeholder} OR root_trigger_type LIKE #{placeholder})"
           ], params ++ ["%#{search}%"]}

      {:window, :has_window}, {clauses, params} ->
        {clauses ++ ["has_window = 1"], params}

      {:window, :no_window}, {clauses, params} ->
        {clauses ++ ["has_window = 0"], params}

      {:only_failed, true}, {clauses, params} ->
        {clauses ++ ["failed = 1"], params}

      {:only_running, true}, {clauses, params} ->
        {clauses ++ ["running = 1"], params}

      {:only_incomplete, true}, {clauses, params} ->
        {clauses ++ ["running = 1"], params}

      _other, acc ->
        acc
    end)
    |> case do
      {[], params} -> {"", params}
      {clauses, params} -> {"WHERE " <> Enum.join(clauses, " AND "), params}
    end
  end

  defp execution_group_sort_sql(:failed_first), do: "failed DESC, activity_seq DESC"
  defp execution_group_sort_sql(:running_first), do: "running DESC, activity_seq DESC"

  defp execution_group_sort_sql(:status_priority),
    do: "failed DESC, running DESC, activity_seq DESC"

  defp execution_group_sort_sql(_sort), do: "activity_seq DESC"

  defp run_events_query(run_id, opts) do
    after_sequence = Keyword.get(opts, :after_sequence)
    limit = Keyword.get(opts, :limit)
    order = event_order(opts)

    cond do
      not is_nil(after_sequence) and (not is_integer(after_sequence) or after_sequence < 0) ->
        {:error, :invalid_opts}

      not is_nil(limit) and (not is_integer(limit) or limit <= 0) ->
        {:error, :invalid_opts}

      is_nil(order) ->
        {:error, :invalid_opts}

      true ->
        clauses = ["run_id = ?1"]
        params = [run_id]
        {clauses, params} = maybe_after_sequence_clause(clauses, params, after_sequence)
        {limit_sql, params} = maybe_limit_clause(params, limit)

        {:ok,
         "SELECT global_sequence, event_blob FROM favn_run_events WHERE #{Enum.join(clauses, " AND ")} ORDER BY sequence #{order}#{limit_sql}",
         params}
    end
  end

  defp execution_group_events_query(group_id, opts) do
    after_global_sequence = Keyword.get(opts, :after_global_sequence)
    limit = Keyword.get(opts, :limit)
    order = event_order(opts)

    cond do
      not is_nil(after_global_sequence) and
          (not is_integer(after_global_sequence) or after_global_sequence < 0) ->
        {:error, :invalid_opts}

      not is_nil(limit) and (not is_integer(limit) or limit <= 0) ->
        {:error, :invalid_opts}

      is_nil(order) ->
        {:error, :invalid_opts}

      true ->
        clauses = ["r.root_execution_group_id = ?1"]
        params = [group_id]

        {clauses, params} =
          case after_global_sequence do
            sequence when is_integer(sequence) and sequence >= 0 ->
              {clauses ++ ["e.global_sequence > ?#{length(params) + 1}"], params ++ [sequence]}

            _other ->
              {clauses, params}
          end

        {limit_sql, params} = maybe_limit_clause(params, limit)

        {:ok,
         """
         SELECT e.global_sequence, e.event_blob
         FROM favn_run_events AS e
         INNER JOIN favn_runs AS r ON r.run_id = e.run_id
         WHERE #{Enum.join(clauses, " AND ")}
         ORDER BY e.global_sequence #{order}, e.run_id ASC, e.sequence ASC#{limit_sql}
         """, params}
    end
  end

  defp event_order(opts) do
    case Keyword.get(opts, :order, :asc) do
      :asc -> "ASC"
      :desc -> "DESC"
      _order -> nil
    end
  end

  defp maybe_after_sequence_clause(clauses, params, sequence)
       when is_integer(sequence) and sequence >= 0,
       do: {clauses ++ ["sequence > ?#{length(params) + 1}"], params ++ [sequence]}

  defp maybe_after_sequence_clause(clauses, params, _sequence), do: {clauses, params}

  defp maybe_limit_clause(params, limit) when is_integer(limit) and limit > 0,
    do: {" LIMIT ?#{length(params) + 1}", params ++ [limit]}

  defp maybe_limit_clause(params, _limit), do: {"", params}

  defp repair_missing_run_query_metadata(repo) do
    sql =
      run_snapshot_select() <>
        " WHERE r.root_execution_group_id IS NULL OR r.pipeline_submit_ref_text IS NULL"

    case SQL.query(repo, sql, []) do
      {:ok, %{rows: []}} ->
        :ok

      {:ok, %{rows: rows}} ->
        with {:ok, runs} <- decode_run_rows(rows) do
          Enum.reduce_while(runs, :ok, fn run, :ok ->
            case update_run_query_metadata(repo, run) do
              :ok -> {:cont, :ok}
              {:error, reason} -> {:halt, {:error, reason}}
            end
          end)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp update_run_query_metadata(repo, run) do
    metadata = RunQuery.metadata(run)

    sql =
      """
      UPDATE favn_runs
      SET root_execution_group_id = ?1,
          parent_run_id = ?2,
          root_run_id = ?3,
          submit_kind = ?4,
          trigger_type = ?5,
          asset_ref_text = ?6,
          target_refs_text = ?7,
          window_key = ?8,
          pipeline_submit_ref_text = ?9
      WHERE run_id = ?10
      """

    params = [
      metadata.root_execution_group_id,
      metadata.parent_run_id,
      metadata.root_run_id,
      metadata.submit_kind,
      metadata.trigger_type,
      metadata.asset_ref_text,
      metadata.target_refs_text,
      metadata.window_key,
      metadata.pipeline_submit_ref_text,
      run.id
    ]

    case SQL.query(repo, sql, params) do
      {:ok, _result} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp run_filter_sql(filters) do
    filters
    |> Enum.reduce({[], []}, fn
      {:status, status}, {clauses, params} ->
        {clauses ++ ["r.status = ?#{length(params) + 1}"], params ++ [Atom.to_string(status)]}

      {:manifest_version_id, manifest_version_id}, {clauses, params} ->
        {clauses ++ ["r.manifest_version_id = ?#{length(params) + 1}"],
         params ++ [manifest_version_id]}
    end)
    |> case do
      {[], params} -> {"", params}
      {clauses, params} -> {" WHERE " <> Enum.join(clauses, " AND "), params}
    end
  end

  defp target_run_filter_sql(:asset, target_ref_text, params) do
    placeholder = "?#{length(params) + 1}"

    {" AND (r.asset_ref_text = #{placeholder} OR r.target_refs_text = #{placeholder} OR r.target_refs_text LIKE #{placeholder} || char(10) || '%' OR r.target_refs_text LIKE '%' || char(10) || #{placeholder} OR r.target_refs_text LIKE '%' || char(10) || #{placeholder} || char(10) || '%')",
     params ++ [target_ref_text]}
  end

  defp target_run_filter_sql(:pipeline, target_ref_text, params) do
    placeholder = "?#{length(params) + 1}"

    {" AND r.pipeline_submit_ref_text != '' AND r.pipeline_submit_ref_text = #{placeholder}",
     params ++ [target_ref_text]}
  end

  defp run_snapshot_select do
    """
    SELECT r.run_blob, r.manifest_version_id, m.manifest_version_id, m.content_hash, m.schema_version, m.runner_contract_version, m.serialization_format, m.manifest_json, m.inserted_at
    FROM favn_runs AS r
    LEFT JOIN favn_manifest_versions AS m ON m.manifest_version_id = r.manifest_version_id
    """
  end

  defp decode_run_rows(rows) do
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

  defp encode_coverage_baseline(%CoverageBaseline{} = baseline) do
    case CoverageBaselineCodec.encode(baseline) do
      {:ok, payload} ->
        payload

      {:error, reason} ->
        raise ArgumentError, "invalid coverage baseline payload: #{inspect(reason)}"
    end
  end

  defp encode_backfill_window(%BackfillWindow{} = window) do
    case BackfillWindowCodec.encode(window) do
      {:ok, payload} ->
        payload

      {:error, reason} ->
        raise ArgumentError, "invalid backfill window payload: #{inspect(reason)}"
    end
  end

  defp encode_asset_window_state(%AssetWindowState{} = state) do
    case AssetWindowStateCodec.encode(state) do
      {:ok, payload} ->
        payload

      {:error, reason} ->
        raise ArgumentError, "invalid asset window state payload: #{inspect(reason)}"
    end
  end

  defp encode_asset_freshness_state(%AssetFreshnessState{} = state) do
    case AssetFreshnessStateCodec.encode(state) do
      {:ok, payload} ->
        payload

      {:error, reason} ->
        raise ArgumentError, "invalid asset freshness state payload: #{inspect(reason)}"
    end
  end

  defp encode_target_status(%TargetStatus{} = status) do
    case TargetStatusCodec.encode(status) do
      {:ok, payload} ->
        payload

      {:error, reason} ->
        raise ArgumentError, "invalid target status payload: #{inspect(reason)}"
    end
  end

  defp encode_backfill_progress(%BackfillProgress{} = progress) do
    case ProgressCodec.encode(progress) do
      {:ok, payload} ->
        payload

      {:error, reason} ->
        raise ArgumentError, "invalid backfill progress payload: #{inspect(reason)}"
    end
  end

  defp backfill_progress_params(%BackfillProgress{} = progress) do
    [
      progress.backfill_run_id,
      progress.total_count,
      progress.pending_count,
      progress.running_count,
      progress.ok_count,
      progress.partial_count,
      progress.error_count,
      progress.cancelled_count,
      progress.timed_out_count,
      encode_atom(progress.status),
      encode_backfill_progress(progress),
      encode_datetime(progress.updated_at)
    ]
  end

  defp encode_materialization_claim(%MaterializationClaim{} = claim) do
    case MaterializationClaimCodec.encode(claim) do
      {:ok, payload} ->
        payload

      {:error, reason} ->
        raise ArgumentError, "invalid materialization claim payload: #{inspect(reason)}"
    end
  end

  defp fetch_materialization_claim(repo, claim_key) do
    sql = "SELECT record_payload FROM favn_materialization_claims WHERE claim_key = ?1 LIMIT 1"

    case SQL.query(repo, sql, [claim_key]) do
      {:ok, %{rows: []}} -> {:ok, nil}
      {:ok, %{rows: [row]}} -> decode_materialization_claim_row(row)
      {:error, reason} -> {:error, reason}
    end
  end

  defp update_claimed_materialization_claim(repo, %MaterializationClaim{} = claim) do
    sql = """
    UPDATE favn_materialization_claims SET
      asset_ref_module = ?2,
      asset_ref_name = ?3,
      freshness_key = ?4,
      input_fingerprint = ?5,
      run_id = ?6,
      asset_step_id = ?7,
      node_key = ?8,
      runner_execution_id = ?9,
      manifest_version_id = ?10,
      manifest_content_hash = ?11,
      freshness_version = ?12,
      status = ?13,
      claimed_at = ?14,
      heartbeat_at = ?15,
      expires_at = ?16,
      finished_at = ?17,
      record_payload = ?18
    WHERE claim_key = ?1 AND status = 'claimed'
    """

    case SQL.query(repo, sql, materialization_claim_params(claim)) do
      {:ok, %{num_rows: 1}} -> :ok
      {:ok, %{num_rows: 0}} -> :conflict
      {:ok, _result} -> :conflict
      {:error, reason} -> {:error, reason}
    end
  end

  defp guarded_materialization_claim_transition(claim_key, opts, transition)
       when is_function(transition, 1) do
    with {:ok, repo} <- repo_name(opts) do
      repo.transact(fn ->
        with {:ok, %MaterializationClaim{status: :claimed} = claim} <-
               fetch_materialization_claim(repo, claim_key),
             updated = transition.(claim),
             :ok <- update_claimed_materialization_claim(repo, updated) do
          {:ok, {:ok, updated}}
        else
          {:ok, nil} -> repo.rollback(:not_found)
          {:ok, %MaterializationClaim{}} -> repo.rollback(:not_found)
          :conflict -> repo.rollback(:not_found)
          {:error, reason} -> repo.rollback(reason)
        end
      end)
      |> case do
        {:ok, result} -> result
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp insert_materialization_claim_if_absent(repo, %MaterializationClaim{} = claim) do
    sql = """
    INSERT OR IGNORE INTO favn_materialization_claims (
      claim_key, asset_ref_module, asset_ref_name, freshness_key, input_fingerprint,
      run_id, asset_step_id, node_key, runner_execution_id, manifest_version_id,
      manifest_content_hash, freshness_version, status, claimed_at, heartbeat_at,
      expires_at, finished_at, record_payload
    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)
    """

    case SQL.query(repo, sql, materialization_claim_params(claim)) do
      {:ok, %{num_rows: 1}} -> :ok
      {:ok, %{num_rows: 0}} -> :conflict
      {:ok, _result} -> :conflict
      {:error, reason} -> {:error, reason}
    end
  end

  defp reclaim_materialization_claim(repo, %MaterializationClaim{} = claim) do
    sql = """
    UPDATE favn_materialization_claims SET
      asset_ref_module = ?2,
      asset_ref_name = ?3,
      freshness_key = ?4,
      input_fingerprint = ?5,
      run_id = ?6,
      asset_step_id = ?7,
      node_key = ?8,
      runner_execution_id = ?9,
      manifest_version_id = ?10,
      manifest_content_hash = ?11,
      freshness_version = ?12,
      status = ?13,
      claimed_at = ?14,
      heartbeat_at = ?15,
      expires_at = ?16,
      finished_at = ?17,
      record_payload = ?18
    WHERE claim_key = ?1
      AND (
        status IN ('failed', 'cancelled', 'timed_out', 'expired')
        OR (status = 'claimed' AND expires_at <= ?19)
      )
    """

    params = materialization_claim_params(claim) ++ [encode_datetime(claim.claimed_at)]

    case SQL.query(repo, sql, params) do
      {:ok, %{num_rows: 1}} -> :ok
      {:ok, %{num_rows: 0}} -> :conflict
      {:ok, _result} -> :conflict
      {:error, reason} -> {:error, reason}
    end
  end

  defp materialization_claim_conflict_result(repo, %MaterializationClaim{} = claim) do
    with {:ok, existing} <- fetch_materialization_claim(repo, claim.claim_key) do
      {:ok, materialization_claim_acquire_decision(existing, claim.claimed_at)}
    else
      {:error, reason} -> repo.rollback(reason)
    end
  end

  defp execution_ownership_params(ownership, payload) do
    [
      ownership.ownership_id,
      ownership.run_id,
      ownership.asset_step_id,
      ownership.runner_execution_id,
      Atom.to_string(ownership.status),
      ownership.inserted_at,
      ownership.updated_at,
      payload
    ]
  end

  defp execution_ownership_active_statuses do
    Enum.map(FavnOrchestrator.RunExecutionOwnership.active_statuses(), &Atom.to_string/1)
  end

  defp materialization_claim_params(%MaterializationClaim{} = claim) do
    [
      claim.claim_key,
      encode_atom(claim.asset_ref_module),
      encode_atom(claim.asset_ref_name),
      claim.freshness_key,
      claim.input_fingerprint,
      claim.run_id,
      claim.asset_step_id,
      encode_node_key(claim.node_key),
      claim.runner_execution_id,
      claim.manifest_version_id,
      claim.manifest_content_hash,
      claim.freshness_version,
      encode_atom(claim.status),
      encode_datetime(claim.claimed_at),
      encode_datetime(claim.heartbeat_at),
      encode_datetime(claim.expires_at),
      encode_datetime(claim.finished_at),
      encode_materialization_claim(claim)
    ]
  end

  defp expire_materialization_claims_in_transaction(repo, %DateTime{} = now) do
    timestamp = DateTime.to_iso8601(now)

    repo.transact(fn ->
      sql =
        "SELECT record_payload FROM favn_materialization_claims WHERE status = ?1 AND expires_at <= ?2"

      case SQL.query(repo, sql, ["claimed", timestamp]) do
        {:ok, %{rows: rows}} ->
          rows
          |> Enum.reduce_while({:ok, 0}, fn row, {:ok, count} ->
            with {:ok, claim} <- decode_materialization_claim_row(row),
                 expired = %{claim | status: :expired, finished_at: now} do
              case update_claimed_materialization_claim(repo, expired) do
                :ok -> {:cont, {:ok, count + 1}}
                :conflict -> {:cont, {:ok, count}}
                {:error, reason} -> {:halt, {:error, reason}}
              end
            else
              {:error, reason} -> {:halt, {:error, reason}}
            end
          end)
          |> case do
            {:ok, count} -> {:ok, count}
            {:error, reason} -> repo.rollback(reason)
          end

        {:error, reason} ->
          repo.rollback(reason)
      end
    end)
    |> case do
      {:ok, count} -> {:ok, count}
      {:error, reason} -> {:error, reason}
    end
  end

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

    %{
      claim
      | status: status,
        error: field_value(failure, :error),
        finished_at: field_value(failure, :finished_at) || DateTime.utc_now(),
        metadata: field_value(failure, :metadata) || claim.metadata
    }
  end

  defp normalize_materialization_failure_status(status) when is_atom(status) do
    if status in MaterializationClaim.terminal_failure_statuses(), do: status, else: :failed
  end

  defp normalize_materialization_failure_status(status) when is_binary(status) do
    Enum.find(
      MaterializationClaim.terminal_failure_statuses(),
      :failed,
      &(Atom.to_string(&1) == status)
    )
  end

  defp encode_node_key(nil), do: nil
  defp encode_node_key(value) when is_binary(value), do: value
  defp encode_node_key(value), do: inspect(value)

  defp field_value(map, field), do: Map.get(map, field) || Map.get(map, Atom.to_string(field))

  defp encode_run_snapshot(run) do
    case RunSnapshotCodec.encode_run(run) do
      {:ok, payload} -> payload
      {:error, reason} -> raise ArgumentError, "invalid run snapshot payload: #{inspect(reason)}"
    end
  end

  defp encode_run_event(event) do
    case RunEventCodec.encode(event) do
      {:ok, payload} -> payload
      {:error, reason} -> raise ArgumentError, "invalid run event payload: #{inspect(reason)}"
    end
  end

  defp encode_log_entry(entry) do
    case LogEntryCodec.encode(entry) do
      {:ok, payload} -> payload
      {:error, reason} -> raise ArgumentError, "failed to encode log entry: #{inspect(reason)}"
    end
  end

  defp encode_scheduler_state(state) do
    case SchedulerStateCodec.encode_state(state) do
      {:ok, payload} ->
        payload

      {:error, reason} ->
        raise ArgumentError, "invalid scheduler state payload: #{inspect(reason)}"
    end
  end

  defp encode_optional_idempotency_response(nil, _operation), do: nil

  defp encode_optional_idempotency_response(value, operation) when is_binary(operation) do
    case IdempotencyResponseCodec.encode(operation, value) do
      {:ok, payload} ->
        payload

      {:error, reason} ->
        raise ArgumentError, "invalid idempotency response payload: #{inspect(reason)}"
    end
  end

  defp decode_optional_idempotency_response(nil), do: {:ok, nil}

  defp decode_optional_idempotency_response(payload) when is_binary(payload) do
    IdempotencyResponseCodec.decode(payload)
  end

  defp delete_expired_execution_leases(repo, %DateTime{} = now) do
    timestamp = DateTime.to_iso8601(now)

    repo.transact(fn ->
      case SQL.query(repo, "SELECT lease_id FROM favn_execution_leases WHERE expires_at <= ?1", [
             timestamp
           ]) do
        {:ok, %{rows: rows}} ->
          lease_ids = Enum.map(rows, fn [lease_id] -> lease_id end)

          with :ok <- delete_execution_lease_scopes(repo, lease_ids),
               :ok <- delete_execution_leases(repo, lease_ids) do
            {:ok, length(lease_ids)}
          else
            {:error, reason} -> repo.rollback(reason)
          end

        {:error, reason} ->
          repo.rollback(reason)
      end
    end)
    |> case do
      {:ok, count} -> {:ok, count}
      {:error, reason} -> {:error, reason}
    end
  end

  defp list_execution_leases_for_run(repo, run_id) do
    sql = "SELECT lease_payload FROM favn_execution_leases WHERE run_id = ?1"

    decode_rows(repo, sql, [run_id], &decode_execution_lease_row/1)
  end

  defp released_execution_scopes(leases) do
    leases
    |> Enum.flat_map(& &1.scopes)
    |> Enum.uniq_by(&ExecutionLeaseCodec.scope_identity/1)
  end

  defp ensure_execution_lease_capacity(repo, scopes, %DateTime{} = now) do
    timestamp = DateTime.to_iso8601(now)

    Enum.reduce_while(scopes, :ok, fn scope, :ok ->
      {scope_kind, scope_key} = ExecutionLeaseCodec.scope_identity(scope)

      sql = """
      SELECT COUNT(*)
      FROM favn_execution_lease_scopes AS s
      JOIN favn_execution_leases AS l ON l.lease_id = s.lease_id
      WHERE s.scope_kind = ?1 AND s.scope_key = ?2 AND l.expires_at > ?3
      """

      case SQL.query(repo, sql, [scope_kind, scope_key, timestamp]) do
        {:ok, %{rows: [[count]]}} when count < scope.limit -> {:cont, :ok}
        {:ok, %{rows: [[_count]]}} -> {:halt, {:error, {:execution_capacity_exceeded, scope}}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp insert_execution_lease(repo, lease, payload) do
    sql = """
    INSERT INTO favn_execution_leases
      (lease_id, run_id, asset_step_id, acquired_at, expires_at, lease_payload)
    VALUES (?1, ?2, ?3, ?4, ?5, ?6)
    """

    params = [
      lease.lease_id,
      lease.run_id,
      lease.asset_step_id,
      DateTime.to_iso8601(lease.acquired_at),
      DateTime.to_iso8601(lease.expires_at),
      payload
    ]

    case SQL.query(repo, sql, params) do
      {:ok, _result} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp insert_execution_lease_scopes(repo, lease) do
    Enum.reduce_while(lease.scopes, :ok, fn scope, :ok ->
      {scope_kind, scope_key} = ExecutionLeaseCodec.scope_identity(scope)

      sql = """
      INSERT INTO favn_execution_lease_scopes
        (lease_id, scope_kind, scope_key, scope_limit)
      VALUES (?1, ?2, ?3, ?4)
      """

      case SQL.query(repo, sql, [lease.lease_id, scope_kind, scope_key, scope.limit]) do
        {:ok, _result} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp fetch_execution_admission_waiter(repo, waiter_id) do
    sql =
      "SELECT waiter_payload FROM favn_execution_admission_waiters WHERE waiter_id = ?1 LIMIT 1"

    case SQL.query(repo, sql, [waiter_id]) do
      {:ok, %{rows: []}} -> {:ok, nil}
      {:ok, %{rows: [[payload]]}} -> ExecutionAdmissionWaiterCodec.decode(payload)
      {:error, reason} -> {:error, reason}
    end
  end

  defp next_execution_admission_waiter(waiter, nil), do: waiter

  defp next_execution_admission_waiter(waiter, existing) do
    %{waiter | inserted_at: existing.inserted_at, wake_generation: existing.wake_generation + 1}
  end

  defp upsert_execution_admission_waiter_row(repo, waiter, payload) do
    {scope_kind, scope_key} = ExecutionLeaseCodec.scope_identity(waiter.blocked_scope)

    sql = """
    INSERT INTO favn_execution_admission_waiters
      (waiter_id, run_id, asset_step_id, queue_reason, blocked_scope_kind, blocked_scope_key,
       inserted_at, updated_at, deadline_at, wake_generation, waiter_payload)
    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
    ON CONFLICT(waiter_id) DO UPDATE SET
      run_id = excluded.run_id,
      asset_step_id = excluded.asset_step_id,
      queue_reason = excluded.queue_reason,
      blocked_scope_kind = excluded.blocked_scope_kind,
      blocked_scope_key = excluded.blocked_scope_key,
      inserted_at = excluded.inserted_at,
      updated_at = excluded.updated_at,
      deadline_at = excluded.deadline_at,
      wake_generation = excluded.wake_generation,
      waiter_payload = excluded.waiter_payload
    """

    params = [
      waiter.waiter_id,
      waiter.run_id,
      waiter.asset_step_id,
      Atom.to_string(waiter.queue_reason),
      scope_kind,
      scope_key,
      DateTime.to_iso8601(waiter.inserted_at),
      DateTime.to_iso8601(waiter.updated_at),
      encode_datetime(waiter.deadline_at),
      waiter.wake_generation,
      payload
    ]

    query_ok(repo, sql, params)
  end

  defp delete_execution_lease_scopes(repo, lease_id) when is_binary(lease_id),
    do: delete_execution_lease_scopes(repo, [lease_id])

  defp delete_execution_lease_scopes(_repo, []), do: :ok

  defp delete_execution_lease_scopes(repo, lease_ids) when is_list(lease_ids) do
    placeholders = Enum.map_join(1..length(lease_ids), ",", &"?#{&1}")
    sql = "DELETE FROM favn_execution_lease_scopes WHERE lease_id IN (#{placeholders})"

    case SQL.query(repo, sql, lease_ids) do
      {:ok, _result} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp delete_execution_lease(repo, lease_id) do
    case SQL.query(repo, "DELETE FROM favn_execution_leases WHERE lease_id = ?1", [lease_id]) do
      {:ok, _result} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp delete_execution_leases(_repo, []), do: :ok

  defp delete_execution_leases(repo, lease_ids) do
    case delete_execution_leases_by_ids(repo, lease_ids) do
      {:ok, _count} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp delete_execution_leases_by_ids(_repo, []), do: {:ok, 0}

  defp delete_execution_leases_by_ids(repo, lease_ids) do
    placeholders = Enum.map_join(1..length(lease_ids), ",", &"?#{&1}")
    sql = "DELETE FROM favn_execution_leases WHERE lease_id IN (#{placeholders})"

    case SQL.query(repo, sql, lease_ids) do
      {:ok, result} -> {:ok, Map.get(result, :num_rows, 0)}
      {:error, reason} -> {:error, reason}
    end
  end

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
