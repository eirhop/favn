defmodule FavnStoragePostgres.Adapter do
  @moduledoc """
  Postgres-backed storage adapter implementing `Favn.Storage.Adapter`.
  """

  @behaviour Favn.Storage.Adapter

  alias Ecto.Adapters.SQL
  alias Favn.Manifest.Version
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage.ManifestCodec
  alias FavnOrchestrator.Storage.RunEventCodec
  alias FavnOrchestrator.Storage.RunStateCodec
  alias FavnOrchestrator.Storage.SchedulerStateCodec
  alias FavnOrchestrator.Storage.WriteSemantics
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.Supervisor, as: PostgresSupervisor

  @active_manifest_key "active_manifest_version_id"
  @nil_schedule_id "__nil__"

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
  def get_run(run_id, opts) when is_binary(run_id) and is_list(opts) do
    with {:ok, repo} <- resolve_repo(opts) do
      sql = "SELECT run_blob FROM favn_runs WHERE run_id = $1 LIMIT 1"

      case SQL.query(repo, sql, [run_id]) do
        {:ok, %{rows: [[blob]]}} -> decode_run(blob)
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
          |> Enum.reduce_while({:ok, []}, fn [blob], {:ok, acc} ->
            case decode_run(blob) do
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
      sql =
        """
        INSERT INTO favn_run_events (run_id, sequence, occurred_at, event_blob)
        VALUES ($1, $2, $3, $4)
        """

      case SQL.query(repo, sql, [
             run_id,
             normalized.sequence,
             normalized.occurred_at,
             encode_term(normalized)
           ]) do
        {:ok, _} ->
          :ok

        {:error, %Postgrex.Error{postgres: %{code: :unique_violation}}} ->
          {:error, :conflicting_event_sequence}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @impl true
  def list_run_events(run_id, opts) when is_binary(run_id) and is_list(opts) do
    with {:ok, repo} <- resolve_repo(opts) do
      sql = "SELECT event_blob FROM favn_run_events WHERE run_id = $1 ORDER BY sequence ASC"

      case SQL.query(repo, sql, [run_id]) do
        {:ok, %{rows: rows}} ->
          rows
          |> Enum.reduce_while({:ok, []}, fn [blob], {:ok, acc} ->
            case decode_term(blob) do
              {:ok, event} when is_map(event) -> {:cont, {:ok, [event | acc]}}
              {:ok, other} -> {:halt, {:error, {:invalid_event_blob, other}}}
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
    with {:ok, repo} <- resolve_repo(opts),
         {:ok, normalized_key} <- SchedulerStateCodec.normalize_key(key),
         {:ok, normalized_state} <- SchedulerStateCodec.normalize_state(state),
         {:ok, existing_version} <- fetch_scheduler_version(repo, normalized_key),
         {:ok, write_version} <-
           resolve_scheduler_version(existing_version, normalized_state[:version]) do
      persisted = Map.put(normalized_state, :version, write_version)
      {pipeline_module, schedule_id} = normalized_key

      sql =
        """
        INSERT INTO favn_scheduler_cursors (pipeline_module, schedule_id, version, updated_at, state_blob)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT(pipeline_module, schedule_id) DO UPDATE SET
          version = EXCLUDED.version,
          updated_at = EXCLUDED.updated_at,
          state_blob = EXCLUDED.state_blob
        """

      params = [
        Atom.to_string(pipeline_module),
        encode_schedule_id(schedule_id),
        write_version,
        DateTime.utc_now(),
        encode_term(persisted)
      ]

      case SQL.query(repo, sql, params) do
        {:ok, _} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def get_scheduler_state(key, opts) when is_list(opts) do
    with {:ok, repo} <- resolve_repo(opts),
         {:ok, {pipeline_module, schedule_id}} <- SchedulerStateCodec.normalize_key(key) do
      sql =
        """
        SELECT state_blob
        FROM favn_scheduler_cursors
        WHERE pipeline_module = $1 AND schedule_id = $2
        LIMIT 1
        """

      params = [Atom.to_string(pipeline_module), encode_schedule_id(schedule_id)]

      case SQL.query(repo, sql, params) do
        {:ok, %{rows: []}} ->
          {:ok, nil}

        {:ok, %{rows: [[blob]]}} ->
          with {:ok, decoded} <- decode_term(blob),
               true <- is_map(decoded) do
            {:ok,
             struct(
               Favn.Scheduler.State,
               Map.merge(decoded, %{pipeline_module: pipeline_module, schedule_id: schedule_id})
             )}
          else
            false -> {:error, :invalid_scheduler_blob}
            {:error, reason} -> {:error, reason}
          end

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp persist_run(repo, run) do
    repo.transact(fn ->
      with {:ok, existing} <- fetch_run_head(repo, run.id),
           decision <- decide_run_write(existing, run),
           :ok <- persist_run_decision(repo, decision, run) do
        :ok
      else
        {:error, reason} ->
          repo.rollback(reason)
      end
    end)
    |> case do
      {:ok, :ok} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp persist_run_decision(_repo, :idempotent, _run), do: :ok
  defp persist_run_decision(_repo, {:error, reason}, _run), do: {:error, reason}

  defp persist_run_decision(repo, decision, run) when decision in [:insert, :replace] do
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
        encode_term(run)
      ]

      case SQL.query(repo, sql, params) do
        {:ok, _} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp decide_run_write(nil, run),
    do: WriteSemantics.decide(nil, nil, run.event_seq, run.snapshot_hash)

  defp decide_run_write({existing_event_seq, existing_hash}, run) do
    WriteSemantics.decide(existing_event_seq, existing_hash, run.event_seq, run.snapshot_hash)
  end

  defp next_updated_seq(repo) do
    sql = "SELECT nextval('favn_run_write_seq')"

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

  defp resolve_scheduler_version(nil, nil), do: {:ok, 1}
  defp resolve_scheduler_version(nil, 1), do: {:ok, 1}
  defp resolve_scheduler_version(nil, _incoming), do: {:error, :invalid_scheduler_version}
  defp resolve_scheduler_version(existing, nil) when is_integer(existing), do: {:ok, existing + 1}

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
        {"SELECT run_blob FROM favn_runs ORDER BY updated_seq DESC, run_id DESC", []}

      is_nil(status) ->
        {"SELECT run_blob FROM favn_runs ORDER BY updated_seq DESC, run_id DESC LIMIT $1",
         [limit]}

      is_nil(limit) ->
        {
          "SELECT run_blob FROM favn_runs WHERE status = $1 ORDER BY updated_seq DESC, run_id DESC",
          [Atom.to_string(status)]
        }

      true ->
        {
          "SELECT run_blob FROM favn_runs WHERE status = $1 ORDER BY updated_seq DESC, run_id DESC LIMIT $2",
          [Atom.to_string(status), limit]
        }
    end
  end

  defp decode_run(blob) do
    with {:ok, decoded} <- decode_term(blob),
         %RunState{} = run_state <- decoded,
         {:ok, normalized} <- RunStateCodec.normalize(run_state) do
      {:ok, normalized}
    else
      {:error, reason} -> {:error, reason}
      other -> {:error, {:invalid_run_blob, other}}
    end
  end

  defp encode_term(value), do: :erlang.term_to_binary(value)

  defp decode_term(blob) when is_binary(blob) do
    {:ok, :erlang.binary_to_term(blob, [:safe])}
  rescue
    error -> {:error, {:invalid_blob, error}}
  end

  defp resolve_repo(opts) do
    with {:ok, normalized} <- normalize_opts(opts) do
      case Keyword.fetch!(normalized, :repo_mode) do
        :managed ->
          {:ok, Repo}

        :external ->
          {:ok, Keyword.fetch!(normalized, :repo)}
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
