defmodule Favn.Storage.Adapter.SQLite do
  @moduledoc """
  Durable SQLite storage adapter backed by Ecto.

  ## Options

    * `:database` - SQLite database path (required)
    * `:pool_size` - Ecto pool size (default: `5`)
    * `:busy_timeout` - sqlite busy timeout in milliseconds (default: `5_000`)

  The adapter stores full `%Favn.Run{}` snapshots as Erlang binaries while also
  indexing common run list fields (`id`, `status`, timestamps) for deterministic
  querying.
  """

  @behaviour Favn.Storage.Adapter

  alias Favn.Run
  alias Favn.Storage.SQLite.Migrations
  alias Favn.Storage.SQLite.Repo

  @impl true
  def child_spec(opts) when is_list(opts) do
    with {:ok, repo_config} <- repo_config(opts) do
      child =
        Supervisor.child_spec(
          {Repo, repo_config},
          id: Repo,
          restart: :permanent,
          shutdown: 5_000,
          type: :worker
        )

      {:ok, child}
    end
  end

  @impl true
  def put_run(%Run{} = run, opts) do
    with {:ok, _repo_config} <- repo_config(opts),
         :ok <- ensure_migrated() do
      now = DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_iso8601()

      sql = """
      INSERT INTO runs (id, status, started_at, finished_at, inserted_at, updated_at, run_blob)
      VALUES (?1, ?2, ?3, ?4, ?5, ?5, ?6)
      ON CONFLICT(id) DO UPDATE SET
        status = excluded.status,
        started_at = excluded.started_at,
        finished_at = excluded.finished_at,
        updated_at = excluded.updated_at,
        run_blob = excluded.run_blob
      """

      params = [
        run.id,
        Atom.to_string(run.status),
        datetime_to_iso(run.started_at),
        datetime_to_iso(run.finished_at),
        now,
        :erlang.term_to_binary(run)
      ]

      case Ecto.Adapters.SQL.query(Repo, sql, params) do
        {:ok, _} -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def get_run(run_id, opts) when is_binary(run_id) and is_list(opts) do
    with {:ok, _repo_config} <- repo_config(opts),
         :ok <- ensure_migrated() do
      sql = "SELECT run_blob FROM runs WHERE id = ?1 LIMIT 1"

      case Ecto.Adapters.SQL.query(Repo, sql, [run_id]) do
        {:ok, %{rows: [[blob]]}} -> {:ok, :erlang.binary_to_term(blob)}
        {:ok, %{rows: []}} -> {:error, :not_found}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def list_runs(opts, adapter_opts) when is_list(opts) and is_list(adapter_opts) do
    with {:ok, _repo_config} <- repo_config(adapter_opts),
         :ok <- ensure_migrated() do
      status = Keyword.get(opts, :status)
      limit = Keyword.get(opts, :limit)

      {sql, params} = build_list_query(status, limit)

      case Ecto.Adapters.SQL.query(Repo, sql, params) do
        {:ok, %{rows: rows}} ->
          runs = Enum.map(rows, fn [blob] -> :erlang.binary_to_term(blob) end)
          {:ok, runs}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp build_list_query(nil, nil) do
    {
      "SELECT run_blob FROM runs ORDER BY updated_at DESC, inserted_at DESC, id DESC",
      []
    }
  end

  defp build_list_query(status, nil) do
    {
      "SELECT run_blob FROM runs WHERE status = ?1 ORDER BY updated_at DESC, inserted_at DESC, id DESC",
      [Atom.to_string(status)]
    }
  end

  defp build_list_query(nil, limit) do
    {
      "SELECT run_blob FROM runs ORDER BY updated_at DESC, inserted_at DESC, id DESC LIMIT ?1",
      [limit]
    }
  end

  defp build_list_query(status, limit) do
    {
      "SELECT run_blob FROM runs WHERE status = ?1 ORDER BY updated_at DESC, inserted_at DESC, id DESC LIMIT ?2",
      [Atom.to_string(status), limit]
    }
  end

  defp ensure_migrated do
    if Process.whereis(Repo) == nil do
      {:error, :sqlite_repo_not_started}
    else
      Migrations.migrate!(Repo)
      :ok
    end
  rescue
    error -> {:error, error}
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
           pool_size: Keyword.get(opts, :pool_size, 5),
           busy_timeout: Keyword.get(opts, :busy_timeout, 5_000)
         ]}
    end
  end

  defp datetime_to_iso(nil), do: nil

  defp datetime_to_iso(%DateTime{} = dt),
    do: DateTime.truncate(dt, :second) |> DateTime.to_iso8601()
end
