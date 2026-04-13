defmodule Favn.Storage.Adapter.SQLite do
  @moduledoc """
  Durable SQLite storage adapter backed by Ecto.

  ## Options

    * `:database` - SQLite database path (required)
    * `:pool_size` - Ecto pool size (default: `1`)
    * `:busy_timeout` - sqlite busy timeout in milliseconds (default: `5_000`)

  The adapter stores full `%Favn.Run{}` snapshots as Erlang binaries while also
  indexing common run list fields (`id`, `status`, timestamps) for deterministic
  querying.
  """

  @behaviour Favn.Storage.Adapter

  alias Ecto.Adapters.SQL
  alias Favn.Run
  alias Favn.Scheduler.State, as: SchedulerState
  alias Favn.Storage.SQLite.Repo
  alias Favn.Storage.SQLite.Supervisor, as: SQLiteSupervisor
  alias Favn.Window.Key

  @impl true
  def child_spec(opts) when is_list(opts) do
    with {:ok, repo_config} <- repo_config(opts) do
      child =
        Supervisor.child_spec(
          {SQLiteSupervisor, repo_config},
          id: SQLiteSupervisor,
          restart: :permanent,
          shutdown: 5_000,
          type: :supervisor
        )

      {:ok, child}
    end
  end

  @impl true
  def scheduler_child_spec(_opts), do: :none

  @impl true
  def put_run(%Run{} = run, opts) do
    with {:ok, _repo_config} <- repo_config(opts),
         :ok <- ensure_repo_started() do
      now_us = DateTime.utc_now() |> DateTime.to_unix(:microsecond)

      sql = """
      INSERT INTO runs (
        id,
        status,
        started_at,
        finished_at,
        inserted_at_us,
        updated_at_us,
        updated_seq,
        run_blob
      )
      VALUES (?1, ?2, ?3, ?4, ?5, ?5, ?6, ?7)
      ON CONFLICT(id) DO UPDATE SET
        status = excluded.status,
        started_at = excluded.started_at,
        finished_at = excluded.finished_at,
        updated_at_us = excluded.updated_at_us,
        updated_seq = excluded.updated_seq,
        run_blob = excluded.run_blob
      """

      updated_seq_sql = """
      INSERT INTO favn_counters (name, value)
      VALUES (?1, 1)
      ON CONFLICT(name) DO UPDATE SET value = value + 1
      RETURNING value
      """

      delete_node_results_sql = "DELETE FROM run_node_results WHERE run_id = ?1"

      insert_node_result_sql = """
      INSERT INTO run_node_results (
        run_id,
        ref_module,
        ref_name,
        window_key,
        status,
        started_at,
        finished_at,
        attempt_count,
        max_attempts,
        result_blob
      ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
      """

      upsert_latest_window_sql = """
      INSERT INTO window_latest_results (
        ref_module,
        ref_name,
        window_key,
        status,
        last_run_id,
        finished_at,
        updated_at_us
      ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
      ON CONFLICT(ref_module, ref_name, window_key) DO UPDATE SET
        status = excluded.status,
        last_run_id = excluded.last_run_id,
        finished_at = excluded.finished_at,
        updated_at_us = excluded.updated_at_us
      """

      case Repo.transact(
             fn ->
               with {:ok, %{rows: [[updated_seq]]}} <-
                      SQL.query(Repo, updated_seq_sql, ["run_write_order"]),
                    params <- [
                      run.id,
                      Atom.to_string(run.status),
                      datetime_to_iso(run.started_at),
                      datetime_to_iso(run.finished_at),
                      now_us,
                      updated_seq,
                      :erlang.term_to_binary(run)
                    ],
                    {:ok, _} <- SQL.query(Repo, sql, params),
                    {:ok, _} <- SQL.query(Repo, delete_node_results_sql, [run.id]),
                    :ok <-
                      persist_node_results(
                        run,
                        updated_seq,
                        insert_node_result_sql,
                        upsert_latest_window_sql
                      ) do
                 {:ok, :ok}
               else
                 {:error, reason} -> Repo.rollback(reason)
               end
             end,
             mode: :immediate
           ) do
        {:ok, :ok} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def get_run(run_id, opts) when is_binary(run_id) and is_list(opts) do
    with {:ok, _repo_config} <- repo_config(opts),
         :ok <- ensure_repo_started() do
      sql = "SELECT run_blob FROM runs WHERE id = ?1 LIMIT 1"

      case SQL.query(Repo, sql, [run_id]) do
        {:ok, %{rows: [[blob]]}} -> deserialize_run(blob)
        {:ok, %{rows: []}} -> {:error, :not_found}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def list_runs(opts, adapter_opts) when is_list(opts) and is_list(adapter_opts) do
    with {:ok, _repo_config} <- repo_config(adapter_opts),
         :ok <- ensure_repo_started() do
      status = Keyword.get(opts, :status)
      limit = Keyword.get(opts, :limit)

      {sql, params} = build_list_query(status, limit)

      case SQL.query(Repo, sql, params) do
        {:ok, %{rows: rows}} ->
          rows
          |> Enum.reduce_while({:ok, []}, fn [blob], {:ok, acc} ->
            case deserialize_run(blob) do
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

  defp build_list_query(nil, nil) do
    {
      "SELECT run_blob FROM runs ORDER BY updated_seq DESC, updated_at_us DESC, id DESC",
      []
    }
  end

  defp build_list_query(status, nil) do
    {
      "SELECT run_blob FROM runs WHERE status = ?1 ORDER BY updated_seq DESC, updated_at_us DESC, id DESC",
      [Atom.to_string(status)]
    }
  end

  defp build_list_query(nil, limit) do
    {
      "SELECT run_blob FROM runs ORDER BY updated_seq DESC, updated_at_us DESC, id DESC LIMIT ?1",
      [limit]
    }
  end

  defp build_list_query(status, limit) do
    {
      "SELECT run_blob FROM runs WHERE status = ?1 ORDER BY updated_seq DESC, updated_at_us DESC, id DESC LIMIT ?2",
      [Atom.to_string(status), limit]
    }
  end

  defp ensure_repo_started do
    if Process.whereis(Repo) == nil do
      {:error, :sqlite_repo_not_started}
    else
      :ok
    end
  end

  defp repo_config(opts) do
    database = Keyword.get(opts, :database)

    cond do
      is_nil(database) ->
        {:error, :sqlite_database_required}

      not is_binary(database) ->
        {:error, :sqlite_database_invalid}

      true ->
        {:ok,
         [
           database: database,
           pool_size: Keyword.get(opts, :pool_size, 1),
           busy_timeout: Keyword.get(opts, :busy_timeout, 5_000)
         ]}
    end
  end

  defp datetime_to_iso(nil), do: nil

  defp datetime_to_iso(%DateTime{} = dt), do: DateTime.to_iso8601(dt)

  defp deserialize_run(blob) when is_binary(blob) do
    {:ok, :erlang.binary_to_term(blob, [:safe])}
  rescue
    error -> {:error, {:invalid_run_blob, error}}
  end

  defp persist_node_results(%Run{} = run, updated_at_us, insert_sql, upsert_latest_sql) do
    run
    |> node_result_rows()
    |> Enum.reduce_while(:ok, fn row, :ok ->
      with {:ok, _} <- SQL.query(Repo, insert_sql, row.insert_params),
           {:ok, _} <- SQL.query(Repo, upsert_latest_sql, row.latest_params.(updated_at_us)) do
        {:cont, :ok}
      else
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp node_result_rows(%Run{} = run) do
    Enum.map(run.node_results || %{}, fn
      {{{ref_module, ref_name}, window_key} = node_key, result} ->
        encoded_window_key = encode_window_key(window_key)
        status = result.status |> to_string()

        %{
          insert_params: [
            run.id,
            Atom.to_string(ref_module),
            Atom.to_string(ref_name),
            encoded_window_key,
            status,
            datetime_to_iso(result.started_at),
            datetime_to_iso(result.finished_at),
            result.attempt_count || 0,
            result.max_attempts || 1,
            :erlang.term_to_binary(%{node_key: node_key, result: result})
          ],
          latest_params: fn updated_at_us ->
            [
              Atom.to_string(ref_module),
              Atom.to_string(ref_name),
              encoded_window_key,
              status,
              run.id,
              datetime_to_iso(result.finished_at),
              updated_at_us
            ]
          end
        }
    end)
  end

  defp encode_window_key(nil), do: "__nil__"
  defp encode_window_key(key) when is_map(key), do: Key.encode(key)

  @impl true
  def get_scheduler_state(pipeline_module, schedule_id, opts)
      when is_atom(pipeline_module) and (is_atom(schedule_id) or is_nil(schedule_id)) and
             is_list(opts) do
    with {:ok, _repo_config} <- repo_config(opts),
         :ok <- ensure_repo_started() do
      {sql, params} = scheduler_state_query(pipeline_module, schedule_id)

      case SQL.query(Repo, sql, params) do
        {:ok, %{rows: []}} ->
          {:ok, nil}

        {:ok, %{rows: [row]}} ->
          {:ok, scheduler_state_from_row(pipeline_module, row)}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp scheduler_state_query(pipeline_module, nil) do
    {
      """
      SELECT schedule_id, schedule_fingerprint, last_evaluated_at, last_due_at, last_submitted_due_at, in_flight_run_id, queued_due_at, updated_at
      FROM scheduler_states
      WHERE pipeline_module = ?1
      ORDER BY updated_at DESC
      LIMIT 1
      """,
      [Atom.to_string(pipeline_module)]
    }
  end

  defp scheduler_state_query(pipeline_module, schedule_id) when is_atom(schedule_id) do
    {
      """
      SELECT schedule_id, schedule_fingerprint, last_evaluated_at, last_due_at, last_submitted_due_at, in_flight_run_id, queued_due_at, updated_at
      FROM scheduler_states
      WHERE pipeline_module = ?1 AND schedule_id = ?2
      LIMIT 1
      """,
      [Atom.to_string(pipeline_module), Atom.to_string(schedule_id)]
    }
  end

  @impl true
  def put_scheduler_state(%SchedulerState{} = state, opts) when is_list(opts) do
    with {:ok, _repo_config} <- repo_config(opts),
         :ok <- ensure_repo_started() do
      sql = """
      INSERT INTO scheduler_states (
        pipeline_module,
        schedule_id,
        schedule_fingerprint,
        last_evaluated_at,
        last_due_at,
        last_submitted_due_at,
        in_flight_run_id,
        queued_due_at,
        updated_at
      ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
      ON CONFLICT(pipeline_module) DO UPDATE SET
        schedule_id = excluded.schedule_id,
        schedule_fingerprint = excluded.schedule_fingerprint,
        last_evaluated_at = excluded.last_evaluated_at,
        last_due_at = excluded.last_due_at,
        last_submitted_due_at = excluded.last_submitted_due_at,
        in_flight_run_id = excluded.in_flight_run_id,
        queued_due_at = excluded.queued_due_at,
        updated_at = excluded.updated_at
      """

      params = [
        Atom.to_string(state.pipeline_module),
        if(is_atom(state.schedule_id), do: Atom.to_string(state.schedule_id), else: nil),
        state.schedule_fingerprint,
        datetime_to_iso(state.last_evaluated_at),
        datetime_to_iso(state.last_due_at),
        datetime_to_iso(state.last_submitted_due_at),
        state.in_flight_run_id,
        datetime_to_iso(state.queued_due_at),
        datetime_to_iso(DateTime.utc_now())
      ]

      case SQL.query(Repo, sql, params) do
        {:ok, _} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

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
      last_evaluated_at: iso_to_datetime(last_eval),
      last_due_at: iso_to_datetime(last_due),
      last_submitted_due_at: iso_to_datetime(last_sub),
      in_flight_run_id: in_flight,
      queued_due_at: iso_to_datetime(queued),
      updated_at: iso_to_datetime(updated)
    }
  end

  defp parse_schedule_id(nil), do: nil

  defp parse_schedule_id(value) when is_binary(value) do
    String.to_existing_atom(value)
  rescue
    ArgumentError -> nil
  end

  defp iso_to_datetime(nil), do: nil

  defp iso_to_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, %DateTime{} = dt, _offset} -> dt
      _ -> nil
    end
  end
end
