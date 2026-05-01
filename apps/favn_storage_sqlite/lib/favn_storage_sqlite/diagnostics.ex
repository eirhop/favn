defmodule FavnStorageSqlite.Diagnostics do
  @moduledoc """
  SQLite-owned diagnostics for database configuration and schema readiness.
  """

  alias FavnStorageSqlite.Migrations
  alias FavnStorageSqlite.Repo

  @type migration_mode :: :auto | :manual
  @type readiness_status :: :ready | :ready_after_migration | :schema_not_ready
  @type readiness_diagnostics :: %{
          status: readiness_status(),
          ready?: boolean(),
          migration_mode: migration_mode(),
          database: %{configured?: true, path: :redacted},
          schema: Migrations.schema_diagnostics()
        }

  @doc """
  Returns redacted SQLite readiness diagnostics for the configured database.

  The readiness surface validates the configured database path, opens a
  temporary bootstrap repo, and inspects the schema through
  `FavnStorageSqlite.Migrations.schema_diagnostics/1`. It never returns the
  configured database path.
  """
  @spec readiness(keyword()) :: {:ok, readiness_diagnostics()} | {:error, map()}
  def readiness(opts) when is_list(opts) do
    database = Keyword.get(opts, :database)

    with {:ok, migration_mode} <- fetch_migration_mode(opts),
         :ok <- validate_database_path(opts),
         {:ok, schema} <- inspect_schema(opts, database) do
      {:ok, readiness_diagnostics(schema, migration_mode)}
    else
      {:error, reason} -> {:error, redacted_error(reason, database)}
    end
  end

  @spec validate_database_path(keyword()) :: :ok | {:error, term()}
  def validate_database_path(opts) when is_list(opts) do
    with {:ok, database} <- fetch_database(opts),
         :ok <- validate_absolute_path(database, Keyword.get(opts, :require_absolute_path, false)),
         :ok <- validate_parent_directory(database),
         :ok <- validate_parent_writable(database),
         :ok <- validate_target(database) do
      validate_writable(database)
    end
  end

  @spec validate_database_path!(keyword()) :: :ok
  def validate_database_path!(opts) when is_list(opts) do
    case validate_database_path(opts) do
      :ok -> :ok
      {:error, reason} -> raise ArgumentError, "invalid sqlite database path: #{inspect(reason)}"
    end
  end

  defp fetch_migration_mode(opts) do
    case Keyword.get(opts, :migration_mode, :auto) do
      mode when mode in [:auto, :manual] -> {:ok, mode}
      mode -> {:error, {:invalid_migration_mode, mode}}
    end
  end

  defp inspect_schema(opts, database) do
    repo_name = unique_repo_name()
    repo_opts = repo_opts(opts, database)

    case Repo.start_link(Keyword.put(repo_opts, :name, repo_name)) do
      {:ok, pid} ->
        Process.unlink(pid)

        try do
          Migrations.schema_diagnostics(repo_name)
        after
          GenServer.stop(pid, :normal, 5_000)
        end

      {:error, reason} ->
        {:error, {:database_open_failed, reason}}
    end
  end

  defp repo_opts(opts, database) do
    [
      database: database,
      pool_size: 1,
      busy_timeout: Keyword.get(opts, :busy_timeout, 5_000)
    ]
  end

  defp unique_repo_name do
    Module.concat(__MODULE__, "ReadinessRepo#{System.unique_integer([:positive, :monotonic])}")
  end

  defp readiness_diagnostics(schema, migration_mode) do
    status = readiness_status(schema.status, migration_mode)

    %{
      status: status,
      ready?: status in [:ready, :ready_after_migration],
      migration_mode: migration_mode,
      database: %{configured?: true, path: :redacted},
      schema: schema
    }
  end

  defp readiness_status(:ready, _migration_mode), do: :ready

  defp readiness_status(status, :auto) when status in [:empty_database, :upgrade_required],
    do: :ready_after_migration

  defp readiness_status(_status, _migration_mode), do: :schema_not_ready

  defp redacted_error(:sqlite_database_required, _database) do
    %{status: :invalid_configuration, reason: :sqlite_database_required}
  end

  defp redacted_error({:invalid_migration_mode, _mode}, _database) do
    %{status: :invalid_configuration, reason: :invalid_migration_mode}
  end

  defp redacted_error({reason, _path}, _database)
       when reason in [:database_path_not_absolute, :database_parent_missing] do
    %{status: :invalid_database_path, reason: reason}
  end

  defp redacted_error({reason, _path, detail}, database)
       when reason in [
              :database_parent_missing,
              :database_parent_not_directory,
              :database_parent_not_writable,
              :database_target_not_regular_file,
              :database_target_stat_failed,
              :database_file_not_writable,
              :database_file_cannot_be_created
            ] do
    %{status: :invalid_database_path, reason: reason, detail: redact(detail, database)}
  end

  defp redacted_error({:database_open_failed, reason}, database) do
    %{status: :database_unavailable, reason: redact(reason, database)}
  end

  defp redacted_error(reason, database) do
    %{status: :schema_diagnostics_failed, reason: redact(reason, database)}
  end

  defp redact(value, database) when is_binary(value) and is_binary(database) do
    String.replace(value, database, "[REDACTED]")
  end

  defp redact(value, _database) when is_binary(value), do: value

  defp redact(value, database) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> Enum.map(&redact(&1, database))
    |> List.to_tuple()
  end

  defp redact(value, database) when is_list(value) do
    Enum.map(value, &redact(&1, database))
  end

  defp redact(%{__struct__: _} = value, database) do
    value
    |> Map.from_struct()
    |> Map.new(fn {key, map_value} -> {key, redact(map_value, database)} end)
  end

  defp redact(value, database) when is_map(value) do
    Map.new(value, fn {key, map_value} -> {key, redact(map_value, database)} end)
  end

  defp redact(value, _database), do: value

  defp fetch_database(opts) do
    case Keyword.get(opts, :database) do
      database when is_binary(database) and database != "" -> {:ok, database}
      _ -> {:error, :sqlite_database_required}
    end
  end

  defp validate_absolute_path(database, true) do
    if Path.type(database) == :absolute do
      :ok
    else
      {:error, {:database_path_not_absolute, database}}
    end
  end

  defp validate_absolute_path(_database, false), do: :ok

  defp validate_parent_directory(database) do
    parent = Path.dirname(database)

    case File.stat(parent) do
      {:ok, %{type: :directory}} -> :ok
      {:ok, stat} -> {:error, {:database_parent_not_directory, parent, stat.type}}
      {:error, reason} -> {:error, {:database_parent_missing, parent, reason}}
    end
  end

  defp validate_parent_writable(database) do
    parent = Path.dirname(database)
    marker = ".favn_sqlite_write_check_#{System.unique_integer([:positive, :monotonic])}"
    path = Path.join(parent, marker)

    case File.open(path, [:write, :exclusive]) do
      {:ok, io} ->
        File.close(io)
        File.rm(path)
        :ok

      {:error, reason} ->
        {:error, {:database_parent_not_writable, parent, reason}}
    end
  end

  defp validate_target(database) do
    case File.stat(database) do
      {:ok, %{type: :regular}} -> :ok
      {:ok, stat} -> {:error, {:database_target_not_regular_file, database, stat.type}}
      {:error, :enoent} -> :ok
      {:error, reason} -> {:error, {:database_target_stat_failed, database, reason}}
    end
  end

  defp validate_writable(database) do
    if File.exists?(database) do
      validate_existing_file_writable(database)
    else
      validate_new_file_writable(database)
    end
  end

  defp validate_existing_file_writable(database) do
    case File.open(database, [:read, :write]) do
      {:ok, io} ->
        File.close(io)
        :ok

      {:error, reason} ->
        {:error, {:database_file_not_writable, database, reason}}
    end
  end

  defp validate_new_file_writable(database) do
    case File.open(database, [:write, :exclusive]) do
      {:ok, io} ->
        File.close(io)
        File.rm(database)
        :ok

      {:error, reason} ->
        {:error, {:database_file_cannot_be_created, database, reason}}
    end
  end
end
