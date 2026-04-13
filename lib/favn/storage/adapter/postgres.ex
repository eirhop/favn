defmodule Favn.Storage.Adapter.Postgres do
  @moduledoc """
  Durable PostgreSQL storage adapter backed by Ecto.

  ## Options

    * `:repo_mode` - `:managed` (default) or `:external`
    * `:repo_config` - repo config keyword list for managed mode
    * `:repo` - external repo module for external mode
    * `:migration_mode` - `:manual` (default) or `:auto` (managed mode only)

  The adapter persists structured run and node state in relational columns and
  JSONB payloads, with a versioned JSON snapshot for `%Favn.Run{}`
  reconstruction.
  """

  @behaviour Favn.Storage.Adapter

  alias Ecto.Adapters.SQL
  alias Favn.Run
  alias Favn.Scheduler.State, as: SchedulerState
  alias Favn.Storage.Postgres.Migrations
  alias Favn.Storage.Postgres.Repo
  alias Favn.Storage.Postgres.RunSerializer
  alias Favn.Storage.Postgres.Supervisor, as: PostgresSupervisor
  alias Favn.Storage.Postgres.TermJSON
  alias Favn.Window.Key

  @impl true
  def child_spec(opts) when is_list(opts) do
    case Keyword.get(opts, :repo_mode, :managed) do
      :managed ->
        with {:ok, repo_config} <- managed_repo_config(opts),
             :ok <- validate_migration_mode(opts) do
          child =
            Supervisor.child_spec(
              {PostgresSupervisor,
               [
                 repo_config: repo_config,
                 migration_mode: Keyword.get(opts, :migration_mode, :manual)
               ]},
              id: PostgresSupervisor,
              restart: :permanent,
              shutdown: 5_000,
              type: :supervisor
            )

          {:ok, child}
        end

      :external ->
        with {:ok, _repo} <- validate_external_repo(opts),
             :ok <- validate_external_migration_mode(opts) do
          :none
        end

      mode ->
        {:error, {:invalid_repo_mode, mode}}
    end
  end

  @impl true
  def scheduler_child_spec(_opts), do: :none

  @impl true
  def put_run(%Run{} = run, opts) do
    with {:ok, snapshot} <- safe_snapshot_from_run(run),
         {:ok, snapshot_hash} <- safe_snapshot_hash(snapshot),
         {:ok, repo} <- resolve_repo(opts),
         :ok <- ensure_schema_ready(repo),
         {:ok, write_seq} <- next_write_seq(repo) do
      persist_run_transaction(repo, run, snapshot, snapshot_hash, write_seq)
    end
  end

  defp safe_snapshot_from_run(%Run{} = run) do
    {:ok, RunSerializer.snapshot_from_run(run)}
  rescue
    error -> {:error, {:serialization_failed, error}}
  end

  defp safe_snapshot_hash(snapshot) do
    {:ok, snapshot_hash(snapshot)}
  rescue
    error -> {:error, {:snapshot_hash_failed, error}}
  end

  @impl true
  def get_run(run_id, opts) when is_binary(run_id) and is_list(opts) do
    with {:ok, repo} <- resolve_repo(opts),
         :ok <- ensure_schema_ready(repo) do
      sql = "SELECT snapshot_json FROM favn_runs WHERE id = $1 LIMIT 1"

      case SQL.query(repo, sql, [run_id]) do
        {:ok, %{rows: [[snapshot]]}} -> RunSerializer.run_from_snapshot(snapshot)
        {:ok, %{rows: []}} -> {:error, :not_found}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def list_runs(opts, adapter_opts) when is_list(opts) and is_list(adapter_opts) do
    with {:ok, repo} <- resolve_repo(adapter_opts),
         :ok <- ensure_schema_ready(repo) do
      status = Keyword.get(opts, :status)
      limit = Keyword.get(opts, :limit)
      {sql, params} = build_list_query(status, limit)

      case SQL.query(repo, sql, params) do
        {:ok, %{rows: rows}} -> decode_list_rows(rows)
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def get_scheduler_state(pipeline_module, schedule_id, opts)
      when is_atom(pipeline_module) and (is_atom(schedule_id) or is_nil(schedule_id)) and
             is_list(opts) do
    with {:ok, repo} <- resolve_repo(opts),
         :ok <- ensure_schema_ready(repo) do
      {sql, params} = scheduler_cursor_query(pipeline_module, schedule_id)

      case SQL.query(repo, sql, params) do
        {:ok, %{rows: []}} -> {:ok, nil}
        {:ok, %{rows: [row]}} -> {:ok, scheduler_state_from_row(pipeline_module, row)}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp scheduler_cursor_query(pipeline_module, nil) do
    # Compatibility fallback only. Runtime scheduler paths pass explicit
    # schedule_id values and should not rely on "latest row" selection.
    {
      """
      SELECT schedule_id, schedule_fingerprint, last_evaluated_at, last_due_at,
             last_submitted_due_at, in_flight_run_id, queued_due_at, updated_at
      FROM favn_scheduler_cursors
      WHERE pipeline_module = $1
      ORDER BY updated_at DESC
      LIMIT 1
      """,
      [Atom.to_string(pipeline_module)]
    }
  end

  defp scheduler_cursor_query(pipeline_module, schedule_id) when is_atom(schedule_id) do
    {
      """
      SELECT schedule_id, schedule_fingerprint, last_evaluated_at, last_due_at,
             last_submitted_due_at, in_flight_run_id, queued_due_at, updated_at
      FROM favn_scheduler_cursors
      WHERE pipeline_module = $1 AND schedule_id = $2
      LIMIT 1
      """,
      [Atom.to_string(pipeline_module), Atom.to_string(schedule_id)]
    }
  end

  @impl true
  def put_scheduler_state(%SchedulerState{} = state, opts) when is_list(opts) do
    with {:ok, repo} <- resolve_repo(opts),
         :ok <- ensure_schema_ready(repo) do
      # `version` is forward-looking groundwork for future compare-and-set
      # semantics. v0.5 increments version but does not yet enforce expected
      # version writes.
      sql = """
      INSERT INTO favn_scheduler_cursors (
        pipeline_module,
        schedule_id,
        schedule_fingerprint,
        last_evaluated_at,
        last_due_at,
        last_submitted_due_at,
        in_flight_run_id,
        queued_due_at,
        version,
        inserted_at,
        updated_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 1, $9, $9)
      ON CONFLICT(pipeline_module, schedule_id) DO UPDATE SET
        schedule_fingerprint = EXCLUDED.schedule_fingerprint,
        last_evaluated_at = EXCLUDED.last_evaluated_at,
        last_due_at = EXCLUDED.last_due_at,
        last_submitted_due_at = EXCLUDED.last_submitted_due_at,
        in_flight_run_id = EXCLUDED.in_flight_run_id,
        queued_due_at = EXCLUDED.queued_due_at,
        version = favn_scheduler_cursors.version + 1,
        updated_at = EXCLUDED.updated_at
      """

      now = DateTime.utc_now()

      params = [
        Atom.to_string(state.pipeline_module),
        encode_schedule_id(state.schedule_id),
        state.schedule_fingerprint,
        state.last_evaluated_at,
        state.last_due_at,
        state.last_submitted_due_at,
        state.in_flight_run_id,
        state.queued_due_at,
        now
      ]

      case SQL.query(repo, sql, params) do
        {:ok, _} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp decode_list_rows(rows) do
    rows
    |> Enum.reduce_while({:ok, []}, fn [snapshot], {:ok, acc} ->
      case RunSerializer.run_from_snapshot(snapshot) do
        {:ok, run} -> {:cont, {:ok, [run | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, runs} -> {:ok, Enum.reverse(runs)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp build_list_query(nil, nil) do
    {"SELECT snapshot_json FROM favn_runs ORDER BY write_seq DESC, id DESC", []}
  end

  defp build_list_query(status, nil) do
    {
      "SELECT snapshot_json FROM favn_runs WHERE status = $1 ORDER BY write_seq DESC, id DESC",
      [Atom.to_string(status)]
    }
  end

  defp build_list_query(nil, limit) do
    {
      "SELECT snapshot_json FROM favn_runs ORDER BY write_seq DESC, id DESC LIMIT $1",
      [limit]
    }
  end

  defp build_list_query(status, limit) do
    {
      "SELECT snapshot_json FROM favn_runs WHERE status = $1 ORDER BY write_seq DESC, id DESC LIMIT $2",
      [Atom.to_string(status), limit]
    }
  end

  defp persist_run_transaction(repo, %Run{} = run, snapshot, snapshot_hash, write_seq) do
    case repo.transact(fn -> do_persist_run(repo, run, snapshot, snapshot_hash, write_seq) end) do
      {:ok, :ok} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp do_persist_run(repo, %Run{} = run, snapshot, snapshot_hash, write_seq) do
    insert_sql = """
    INSERT INTO favn_runs (
      id,
      status,
      submit_kind,
      replay_mode,
      event_seq,
      write_seq,
      started_at,
      finished_at,
      max_concurrency,
      timeout_ms,
      rerun_of_run_id,
      parent_run_id,
      root_run_id,
      lineage_depth,
      target_refs_json,
      submit_ref_json,
      params_json,
      retry_policy_json,
      pipeline_json,
      pipeline_context_json,
      plan_json,
      backfill_json,
      operator_reason_json,
      error_json,
      terminal_reason_json,
      snapshot_version,
      snapshot_hash,
      snapshot_json,
      inserted_at,
      updated_at
    )
    VALUES (
      $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
      $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
      $21, $22, $23, $24, $25, $26, $27, $28, $29, $29
    )
    ON CONFLICT(id) DO UPDATE SET
      status = EXCLUDED.status,
      submit_kind = EXCLUDED.submit_kind,
      replay_mode = EXCLUDED.replay_mode,
      event_seq = EXCLUDED.event_seq,
      write_seq = EXCLUDED.write_seq,
      started_at = EXCLUDED.started_at,
      finished_at = EXCLUDED.finished_at,
      max_concurrency = EXCLUDED.max_concurrency,
      timeout_ms = EXCLUDED.timeout_ms,
      rerun_of_run_id = EXCLUDED.rerun_of_run_id,
      parent_run_id = EXCLUDED.parent_run_id,
      root_run_id = EXCLUDED.root_run_id,
      lineage_depth = EXCLUDED.lineage_depth,
      target_refs_json = EXCLUDED.target_refs_json,
      submit_ref_json = EXCLUDED.submit_ref_json,
      params_json = EXCLUDED.params_json,
      retry_policy_json = EXCLUDED.retry_policy_json,
      pipeline_json = EXCLUDED.pipeline_json,
      pipeline_context_json = EXCLUDED.pipeline_context_json,
      plan_json = EXCLUDED.plan_json,
      backfill_json = EXCLUDED.backfill_json,
      operator_reason_json = EXCLUDED.operator_reason_json,
      error_json = EXCLUDED.error_json,
      terminal_reason_json = EXCLUDED.terminal_reason_json,
      snapshot_version = EXCLUDED.snapshot_version,
      snapshot_hash = EXCLUDED.snapshot_hash,
      snapshot_json = EXCLUDED.snapshot_json,
      updated_at = EXCLUDED.updated_at
    WHERE favn_runs.event_seq < EXCLUDED.event_seq
    RETURNING write_seq
    """

    now = DateTime.utc_now()

    params = [
      run.id,
      Atom.to_string(run.status),
      Atom.to_string(run.submit_kind),
      Atom.to_string(run.replay_mode),
      run.event_seq,
      write_seq,
      run.started_at,
      run.finished_at,
      run.max_concurrency,
      run.timeout_ms,
      run.rerun_of_run_id,
      run.parent_run_id,
      run.root_run_id,
      run.lineage_depth,
      TermJSON.encode(run.target_refs),
      TermJSON.encode(run.submit_ref),
      TermJSON.encode(run.params),
      TermJSON.encode(run.retry_policy),
      TermJSON.encode(run.pipeline),
      TermJSON.encode(run.pipeline_context),
      TermJSON.encode(run.plan),
      TermJSON.encode(run.backfill),
      TermJSON.encode(run.operator_reason),
      TermJSON.encode(run.error),
      TermJSON.encode(run.terminal_reason),
      RunSerializer.snapshot_version(),
      snapshot_hash,
      snapshot,
      now
    ]

    repo
    |> SQL.query(insert_sql, params)
    |> handle_run_upsert_result(repo, run, write_seq, snapshot_hash)
  end

  defp handle_run_upsert_result(
         {:ok, %{num_rows: 1}},
         repo,
         %Run{} = run,
         write_seq,
         _snapshot_hash
       ) do
    case replace_run_nodes(repo, run, write_seq) do
      :ok -> :ok
      {:error, reason} -> repo.rollback(reason)
    end
  end

  defp handle_run_upsert_result(
         {:ok, %{num_rows: 0}},
         repo,
         %Run{} = run,
         _write_seq,
         snapshot_hash
       ) do
    case classify_run_conflict(repo, run.id, run.event_seq, snapshot_hash) do
      :idempotent -> :ok
      {:error, reason} -> repo.rollback(reason)
    end
  end

  defp handle_run_upsert_result({:error, reason}, repo, _run, _write_seq, _snapshot_hash) do
    repo.rollback(reason)
  end

  defp classify_run_conflict(repo, run_id, event_seq, snapshot_hash) do
    sql = "SELECT event_seq, snapshot_hash FROM favn_runs WHERE id = $1 LIMIT 1"

    case SQL.query(repo, sql, [run_id]) do
      {:ok, %{rows: [[^event_seq, ^snapshot_hash]]}} ->
        :idempotent

      {:ok, %{rows: [[^event_seq, _existing_hash]]}} ->
        {:error, :conflicting_snapshot}

      {:ok, %{rows: [[stored_seq, _existing_hash]]}} when stored_seq > event_seq ->
        {:error, :stale_write}

      {:ok, %{rows: [[_stored_seq, _existing_hash]]}} ->
        {:error, :stale_write}

      {:ok, %{rows: []}} ->
        {:error, :stale_write}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp replace_run_nodes(repo, %Run{} = run, write_seq) do
    delete_sql = "DELETE FROM favn_run_nodes WHERE run_id = $1"

    with {:ok, _} <- SQL.query(repo, delete_sql, [run.id]) do
      run
      |> node_rows()
      |> Enum.reduce_while(:ok, fn row, :ok ->
        case insert_run_node(repo, run.id, row, write_seq) do
          :ok -> {:cont, :ok}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)
    end
  end

  defp insert_run_node(repo, run_id, row, write_seq) do
    insert_sql = """
    INSERT INTO favn_run_nodes (
      run_id,
      ref_module,
      ref_name,
      window_key_text,
      window_key_json,
      stage,
      status,
      attempt_count,
      max_attempts,
      next_retry_at,
      started_at,
      finished_at,
      duration_ms,
      meta_json,
      error_json,
      attempts_json,
      inserted_at,
      updated_at
    ) VALUES (
      $1, $2, $3, $4, $5, $6, $7, $8, $9,
      $10, $11, $12, $13, $14, $15, $16, $17, $17
    )
    """

    now = DateTime.utc_now()

    params = [
      run_id,
      row.ref_module,
      row.ref_name,
      row.window_key_text,
      row.window_key_json,
      row.stage,
      row.status,
      row.attempt_count,
      row.max_attempts,
      row.next_retry_at,
      row.started_at,
      row.finished_at,
      row.duration_ms,
      row.meta_json,
      row.error_json,
      row.attempts_json,
      now
    ]

    with {:ok, _result} <- SQL.query(repo, insert_sql, params) do
      upsert_latest_success(repo, row, run_id, write_seq)
    end
  end

  defp upsert_latest_success(repo, row, run_id, write_seq) do
    cond do
      row.status != "ok" ->
        :ok

      not is_struct(row.finished_at, DateTime) ->
        :ok

      true ->
        sql = """
        INSERT INTO favn_asset_window_latest (
          ref_module,
          ref_name,
          window_key_text,
          window_key_json,
          last_run_id,
          last_finished_at,
          last_write_seq,
          updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT(ref_module, ref_name, window_key_text) DO UPDATE SET
          window_key_json = EXCLUDED.window_key_json,
          last_run_id = EXCLUDED.last_run_id,
          last_finished_at = EXCLUDED.last_finished_at,
          last_write_seq = EXCLUDED.last_write_seq,
          updated_at = EXCLUDED.updated_at
        WHERE favn_asset_window_latest.last_write_seq < EXCLUDED.last_write_seq
        """

        params = [
          row.ref_module,
          row.ref_name,
          row.window_key_text,
          row.window_key_json,
          run_id,
          row.finished_at,
          write_seq,
          DateTime.utc_now()
        ]

        case SQL.query(repo, sql, params) do
          {:ok, _} -> :ok
          {:error, reason} -> {:error, reason}
        end
    end
  end

  defp node_rows(%Run{} = run) do
    Enum.map(run.node_results, fn {{{ref_module, ref_name}, window_key}, result} ->
      %{
        ref_module: Atom.to_string(ref_module),
        ref_name: Atom.to_string(ref_name),
        window_key_text: encode_window_key(window_key),
        window_key_json: TermJSON.encode(window_key),
        stage: result.stage || 0,
        status: to_string(result.status),
        attempt_count: result.attempt_count || 0,
        max_attempts: result.max_attempts || 1,
        next_retry_at: result.next_retry_at,
        started_at: result.started_at,
        finished_at: result.finished_at,
        duration_ms: result.duration_ms,
        meta_json: TermJSON.encode(result.meta || %{}),
        error_json: TermJSON.encode(result.error),
        attempts_json: TermJSON.encode(result.attempts || [])
      }
    end)
  end

  defp encode_window_key(nil), do: "__nil__"
  defp encode_window_key(key) when is_map(key), do: Key.encode(key)

  defp scheduler_state_from_row(pipeline_module, [
         schedule_id,
         schedule_fingerprint,
         last_eval,
         last_due,
         last_sub,
         in_flight,
         queued,
         updated
       ]) do
    %SchedulerState{
      pipeline_module: pipeline_module,
      schedule_id: parse_schedule_id(schedule_id),
      schedule_fingerprint: schedule_fingerprint,
      last_evaluated_at: last_eval,
      last_due_at: last_due,
      last_submitted_due_at: last_sub,
      in_flight_run_id: in_flight,
      queued_due_at: queued,
      updated_at: updated
    }
  end

  defp parse_schedule_id(nil), do: nil
  # `__default__` keeps nullable schedule_id compatibility while preserving a
  # non-null composite storage key.
  defp parse_schedule_id("__default__"), do: nil

  defp parse_schedule_id(value) when is_binary(value) do
    String.to_existing_atom(value)
  rescue
    ArgumentError -> nil
  end

  # `nil` schedule_id maps to `__default__` for storage identity.
  defp encode_schedule_id(nil), do: "__default__"
  defp encode_schedule_id(value) when is_atom(value), do: Atom.to_string(value)

  defp next_write_seq(repo) do
    sql = "SELECT nextval('favn_run_write_seq')"

    case SQL.query(repo, sql, []) do
      {:ok, %{rows: [[value]]}} -> {:ok, value}
      {:error, reason} -> {:error, reason}
    end
  end

  defp snapshot_hash(snapshot) do
    # Snapshot hash is used for run write idempotency and conflict detection.
    # `write_seq` is ordering-only and intentionally not gap-free.
    # `snapshot_json` is a reconstruction/read-model cache, while relational
    # columns remain the primary durability/query contract.
    snapshot
    |> JSON.encode_to_iodata!()
    |> IO.iodata_to_binary()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp resolve_repo(opts) do
    case Keyword.get(opts, :repo_mode, :managed) do
      :managed -> {:ok, Repo}
      :external -> validate_external_repo(opts)
      mode -> {:error, {:invalid_repo_mode, mode}}
    end
  end

  defp managed_repo_config(opts) do
    case Keyword.get(opts, :repo_config) do
      config when is_list(config) and config != [] -> {:ok, config}
      nil -> {:error, :postgres_repo_config_required}
      _other -> {:error, :postgres_repo_config_invalid}
    end
  end

  defp validate_external_repo(opts) do
    case Keyword.get(opts, :repo) do
      repo when is_atom(repo) and not is_nil(repo) ->
        validate_external_repo_module(repo)

      nil ->
        {:error, :postgres_external_repo_required}

      _other ->
        {:error, :postgres_external_repo_invalid}
    end
  end

  defp validate_external_repo_module(repo) do
    cond do
      not Code.ensure_loaded?(repo) ->
        {:error, :postgres_external_repo_invalid}

      not function_exported?(repo, :__adapter__, 0) ->
        {:error, :postgres_external_repo_invalid}

      repo.__adapter__() != Ecto.Adapters.Postgres ->
        {:error, :postgres_external_repo_must_use_postgres}

      not (function_exported?(repo, :transact, 1) or function_exported?(repo, :transact, 2)) ->
        {:error, :postgres_external_repo_missing_transact}

      not function_exported?(repo, :rollback, 1) ->
        {:error, :postgres_external_repo_missing_rollback}

      true ->
        {:ok, repo}
    end
  rescue
    UndefinedFunctionError -> {:error, :postgres_external_repo_invalid}
  end

  defp validate_migration_mode(opts) do
    case Keyword.get(opts, :migration_mode, :manual) do
      mode when mode in [:manual, :auto] -> :ok
      mode -> {:error, {:invalid_migration_mode, mode}}
    end
  end

  defp validate_external_migration_mode(opts) do
    case Keyword.get(opts, :migration_mode, :manual) do
      :manual -> :ok
      :auto -> {:error, :postgres_external_repo_auto_migration_unsupported}
      mode -> {:error, {:invalid_migration_mode, mode}}
    end
  end

  defp ensure_schema_ready(repo) do
    key = {__MODULE__, repo, :schema_ready}

    case :persistent_term.get(key, :unknown) do
      true ->
        :ok

      :unknown ->
        if Migrations.schema_ready?(repo) do
          :persistent_term.put(key, true)
          :ok
        else
          {:error, :postgres_schema_not_ready}
        end

      _ ->
        {:error, :postgres_schema_not_ready}
    end
  end
end
