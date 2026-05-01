defmodule FavnStorageSqlite.Diagnostics do
  @moduledoc false

  @spec validate_database_path(keyword()) :: :ok | {:error, term()}
  def validate_database_path(opts) when is_list(opts) do
    with {:ok, database} <- fetch_database(opts),
         :ok <- validate_absolute_path(database, Keyword.get(opts, :require_absolute_path, false)),
         :ok <- validate_parent_directory(database),
         :ok <- validate_parent_writable(database),
         :ok <- validate_target(database),
         :ok <- validate_writable(database) do
      :ok
    end
  end

  @spec validate_database_path!(keyword()) :: :ok
  def validate_database_path!(opts) when is_list(opts) do
    case validate_database_path(opts) do
      :ok -> :ok
      {:error, reason} -> raise ArgumentError, "invalid sqlite database path: #{inspect(reason)}"
    end
  end

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
