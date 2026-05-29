defmodule FavnStorageSqlite.Maintenance do
  @moduledoc """
  SQLite-owned control-plane maintenance operations.

  This module reuses `FavnStorageSqlite.Diagnostics` and
  `FavnStorageSqlite.Migrations` for readiness, schema classification, and
  migration execution. Live backup creation uses SQLite `VACUUM INTO`; raw live
  database-file copying is intentionally not implemented.
  """

  alias Ecto.Adapters.SQL
  alias FavnStorageSqlite.Diagnostics
  alias FavnStorageSqlite.Migrations
  alias FavnStorageSqlite.Repo

  @type result :: {:ok, map()} | {:error, map()}

  @doc "Returns redacted SQLite maintenance status."
  @spec status(keyword()) :: result()
  def status(opts) when is_list(opts) do
    case Diagnostics.readiness(opts) do
      {:ok, diagnostics} -> {:ok, Map.put(diagnostics, :adapter, :sqlite)}
      {:error, reason} -> {:error, normalize_error(:status, reason)}
    end
  end

  @doc "Runs or dry-runs explicit SQLite schema migration."
  @spec migrate(keyword(), keyword()) :: result()
  def migrate(adapter_opts, command_opts) when is_list(adapter_opts) and is_list(command_opts) do
    started = System.monotonic_time()

    with {:ok, dry_run?} <- migration_mode(command_opts),
         :ok <- validate_source(adapter_opts),
         {:ok, result} <-
           with_migration_repo(adapter_opts, fn repo -> migrate_repo(repo, dry_run?) end) do
      {:ok, Map.merge(result, %{adapter: :sqlite, duration_ms: duration_ms(started)})}
    else
      {:error, reason} -> {:error, normalize_error(:migrate, reason)}
    end
  end

  @doc "Creates a SQLite-native live backup with `VACUUM INTO`."
  @spec backup(keyword(), keyword()) :: result()
  def backup(adapter_opts, command_opts) when is_list(adapter_opts) and is_list(command_opts) do
    started = System.monotonic_time()
    started_at = DateTime.utc_now()
    verify? = Keyword.get(command_opts, :verify?, true)

    with {:ok, destination} <- fetch_path(command_opts, :to, :backup_destination_required),
         {:ok, source} <- fetch_path(adapter_opts, :database, :sqlite_database_required),
         :ok <- validate_existing_source(source, adapter_opts),
         :ok <- validate_backup_destination(source, destination, adapter_opts, command_opts),
         {:ok, :ready} <- ensure_ready_source(adapter_opts),
         {:ok, _backup} <- with_repo(adapter_opts, &create_backup(&1, destination)),
         {:ok, byte_size} <- file_size(destination),
         {:ok, checksum} <- checksum(destination),
         {:ok, verification} <- maybe_verify(adapter_opts, destination, verify?) do
      finished_at = DateTime.utc_now()

      {:ok,
       %{
         adapter: :sqlite,
         destination_identity: identity(destination),
         byte_size: byte_size,
         checksum: checksum,
         started_at: started_at,
         finished_at: finished_at,
         duration_ms: duration_ms(started),
         checkpoint_policy: :passive_before_vacuum_into,
         verification: verification,
         warnings: []
       }}
    else
      {:error, reason} -> {:error, normalize_error(:backup, reason)}
    end
  end

  @doc "Verifies a SQLite control-plane backup file."
  @spec verify_backup(keyword(), keyword()) :: result()
  def verify_backup(adapter_opts, command_opts)
      when is_list(adapter_opts) and is_list(command_opts) do
    with {:ok, path} <- fetch_path(command_opts, :path, :backup_path_required),
         {:ok, source} <- fetch_path(adapter_opts, :database, :sqlite_database_required),
         :ok <- validate_backup_file(path, source),
         {:ok, byte_size} <- file_size(path),
         {:ok, checksum} <- checksum(path),
         {:ok, result} <- with_repo(Keyword.put(adapter_opts, :database, path), &verify_repo/1) do
      {:ok,
       Map.merge(result, %{
         adapter: :sqlite,
         backup_status: :valid,
         checksum: checksum,
         byte_size: byte_size,
         warnings: []
       })}
    else
      {:error, reason} -> {:error, normalize_error(:verify_backup, reason)}
    end
  end

  defp migrate_repo(repo, dry_run?) do
    with {:ok, previous} <- Migrations.schema_diagnostics(repo),
         :ok <- migration_allowed(previous.status) do
      cond do
        dry_run? and previous.status in [:empty_database, :upgrade_required] ->
          {:ok,
           %{
             action: :dry_run,
             dry_run?: true,
             previous_schema_status: previous.status,
             final_schema_status: previous.status,
             migrated_versions: previous.missing_versions,
             migrated_count: length(previous.missing_versions),
             warnings: []
           }}

        previous.status == :ready ->
          {:ok,
           %{
             action: :noop,
             dry_run?: dry_run?,
             previous_schema_status: :ready,
             final_schema_status: :ready,
             migrated_versions: [],
             migrated_count: 0,
             warnings: []
           }}

        true ->
          migrated_versions = previous.missing_versions
          :ok = Migrations.migrate!(repo)

          with {:ok, final} <- Migrations.schema_diagnostics(repo),
               :ok <- ensure_ready_schema(final) do
            {:ok,
             %{
               action: :migrated,
               dry_run?: false,
               previous_schema_status: previous.status,
               final_schema_status: final.status,
               migrated_versions: migrated_versions,
               migrated_count: length(migrated_versions),
               warnings: []
             }}
          end
      end
    end
  rescue
    error -> {:error, {:migration_failed, error}}
  catch
    kind, reason -> {:error, {:migration_failed, {kind, reason}}}
  end

  defp migration_allowed(status) when status in [:ready, :empty_database, :upgrade_required],
    do: :ok

  defp migration_allowed(status), do: {:error, {:migration_not_allowed, status}}

  defp migration_mode(command_opts) do
    apply? = Keyword.get(command_opts, :apply?, false)
    dry_run? = Keyword.get(command_opts, :dry_run?, not apply?)

    if apply? and dry_run? do
      {:error, :ambiguous_migration_options}
    else
      {:ok, dry_run?}
    end
  end

  defp ensure_ready_schema(%{status: :ready}), do: :ok

  defp ensure_ready_schema(%{status: status}),
    do: {:error, {:migration_failed_final_schema, status}}

  defp create_backup(repo, destination) do
    with {:ok, _} <- SQL.query(repo, "PRAGMA wal_checkpoint(PASSIVE)", []),
         {:ok, result} <- SQL.query(repo, "VACUUM INTO #{sqlite_string(destination)}", []) do
      {:ok, result}
    else
      {:error, reason} -> {:error, {:backup_sql_failed, reason}}
    end
  end

  defp verify_repo(repo) do
    with {:ok, :ok} <- quick_check(repo),
         {:ok, schema} <- Migrations.schema_diagnostics(repo),
         :ok <- verify_schema(schema) do
      {:ok,
       %{
         integrity_check_status: :ok,
         schema_status: schema.status,
         failure_category: nil
       }}
    end
  end

  defp quick_check(repo) do
    case SQL.query(repo, "PRAGMA quick_check", []) do
      {:ok, %{rows: [["ok"]]}} -> {:ok, :ok}
      {:ok, %{rows: rows}} -> {:error, {:integrity_check_failed, rows}}
      {:error, reason} -> {:error, {:integrity_check_unavailable, reason}}
    end
  end

  defp verify_schema(%{status: :ready}), do: :ok
  defp verify_schema(%{status: status}), do: {:error, {:backup_schema_not_ready, status}}

  defp ensure_ready_source(adapter_opts) do
    case Diagnostics.readiness(adapter_opts) do
      {:ok, %{schema: %{status: :ready}}} -> {:ok, :ready}
      {:ok, %{schema: %{status: status}}} -> {:error, {:source_schema_not_ready, status}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp maybe_verify(_adapter_opts, _destination, false), do: {:ok, nil}

  defp maybe_verify(adapter_opts, destination, true) do
    case verify_backup(adapter_opts, path: destination) do
      {:ok, verification} -> {:ok, verification}
      {:error, reason} -> {:error, {:backup_verification_failed, reason}}
    end
  end

  defp validate_source(adapter_opts), do: Diagnostics.validate_database_path(adapter_opts)

  defp validate_existing_source(source, adapter_opts) do
    with :ok <-
           validate_absolute_path(
             source,
             Keyword.get(adapter_opts, :require_absolute_path, false)
           ) do
      case File.stat(source) do
        {:ok, %{type: :regular}} -> :ok
        {:ok, stat} -> {:error, {:source_database_not_regular_file, stat.type}}
        {:error, :enoent} -> {:error, :source_database_missing}
        {:error, reason} -> {:error, {:source_database_stat_failed, reason}}
      end
    end
  end

  defp validate_backup_destination(source, destination, adapter_opts, command_opts) do
    with :ok <-
           validate_absolute_path(
             destination,
             Keyword.get(adapter_opts, :require_absolute_path, false)
           ),
         :ok <- validate_not_source(destination, source),
         :ok <- validate_destination_parent(destination),
         :ok <- validate_destination_parent_writable(destination),
         :ok <- reject_unsupported_backup_options(command_opts),
         :ok <- validate_destination_target(destination) do
      :ok
    end
  end

  defp reject_unsupported_backup_options(command_opts) do
    if Keyword.get(command_opts, :overwrite?, false) do
      {:error, :backup_overwrite_not_supported}
    else
      :ok
    end
  end

  defp validate_backup_file(path, source) do
    with :ok <- validate_not_source(path, source) do
      case File.stat(path) do
        {:ok, %{type: :regular}} -> :ok
        {:ok, stat} -> {:error, {:backup_not_regular_file, stat.type}}
        {:error, :enoent} -> {:error, :backup_missing}
        {:error, reason} -> {:error, {:backup_stat_failed, reason}}
      end
    end
  end

  defp validate_absolute_path(path, true) do
    if Path.type(path) == :absolute, do: :ok, else: {:error, :backup_path_not_absolute}
  end

  defp validate_absolute_path(_path, false), do: :ok

  defp validate_not_source(path, source) do
    if Path.expand(path) == Path.expand(source), do: {:error, :backup_same_as_source}, else: :ok
  end

  defp validate_destination_parent(destination) do
    parent = Path.dirname(destination)

    case File.stat(parent) do
      {:ok, %{type: :directory}} -> :ok
      {:ok, stat} -> {:error, {:backup_parent_not_directory, stat.type}}
      {:error, reason} -> {:error, {:backup_parent_missing, reason}}
    end
  end

  defp validate_destination_parent_writable(destination) do
    parent = Path.dirname(destination)
    marker = ".favn_sqlite_backup_write_check_#{System.unique_integer([:positive, :monotonic])}"
    path = Path.join(parent, marker)

    case File.open(path, [:write, :exclusive]) do
      {:ok, io} ->
        File.close(io)
        File.rm(path)
        :ok

      {:error, reason} ->
        {:error, {:backup_parent_not_writable, reason}}
    end
  end

  defp validate_destination_target(destination) do
    case File.stat(destination) do
      {:ok, %{type: :regular}} -> {:error, :backup_destination_exists}
      {:ok, stat} -> {:error, {:backup_destination_not_regular_file, stat.type}}
      {:error, :enoent} -> :ok
      {:error, reason} -> {:error, {:backup_destination_stat_failed, reason}}
    end
  end

  defp with_repo(opts, fun) when is_list(opts) and is_function(fun, 1) do
    database = Keyword.get(opts, :database)
    repo_name = unique_repo_name()

    repo_opts = [
      database: database,
      pool_size: 1,
      busy_timeout: Keyword.get(opts, :busy_timeout, 5_000),
      name: repo_name
    ]

    case Repo.start_link(repo_opts) do
      {:ok, pid} ->
        Process.unlink(pid)

        try do
          fun.(repo_name)
        after
          GenServer.stop(pid, :normal, 5_000)
        end

      {:error, reason} ->
        {:error, {:database_open_failed, reason}}
    end
  end

  defp with_migration_repo(opts, fun) when is_list(opts) and is_function(fun, 1) do
    if Process.whereis(Repo) do
      fun.(Repo)
    else
      repo_opts = [
        database: Keyword.get(opts, :database),
        pool_size: 1,
        busy_timeout: Keyword.get(opts, :busy_timeout, 5_000),
        name: Repo
      ]

      case Repo.start_link(repo_opts) do
        {:ok, pid} ->
          Process.unlink(pid)

          try do
            fun.(Repo)
          after
            GenServer.stop(pid, :normal, 5_000)
          end

        {:error, reason} ->
          {:error, {:database_open_failed, reason}}
      end
    end
  end

  defp unique_repo_name do
    Module.concat(__MODULE__, "MaintenanceRepo#{System.unique_integer([:positive, :monotonic])}")
  end

  defp fetch_path(opts, key, error) do
    case Keyword.get(opts, key) do
      path when is_binary(path) and path != "" -> {:ok, path}
      _ -> {:error, error}
    end
  end

  defp file_size(path) do
    case File.stat(path) do
      {:ok, %{type: :regular, size: size}} -> {:ok, size}
      {:ok, stat} -> {:error, {:backup_not_regular_file, stat.type}}
      {:error, reason} -> {:error, {:backup_stat_failed, reason}}
    end
  end

  defp checksum(path) do
    context = :crypto.hash_init(:sha256)

    checksum =
      path
      |> File.stream!([], 2048)
      |> Enum.reduce(context, &:crypto.hash_update(&2, &1))
      |> :crypto.hash_final()
      |> Base.encode16(case: :lower)

    {:ok, checksum}
  rescue
    error -> {:error, {:backup_checksum_failed, error}}
  end

  defp sqlite_string(value) do
    "'" <> String.replace(value, "'", "''") <> "'"
  end

  defp identity(path), do: %{path: :redacted, basename: Path.basename(path)}

  defp duration_ms(started) do
    System.convert_time_unit(System.monotonic_time() - started, :native, :millisecond)
  end

  defp normalize_error(operation, reason) do
    category = error_category(operation, reason)

    %{
      category: category,
      operation: operation,
      adapter: :sqlite,
      reason: safe_reason(reason),
      retryable?: retryable?(category),
      details: safe_details(reason)
    }
  end

  defp error_category(_operation, reason)
       when reason in [
              :sqlite_database_required,
              :backup_destination_required,
              :backup_path_required,
              :ambiguous_migration_options
            ],
       do: :invalid_configuration

  defp error_category(_operation, reason)
       when reason in [
              :backup_path_not_absolute,
              :backup_same_as_source,
              :backup_destination_exists,
              :backup_overwrite_not_supported
            ],
       do: :backup_invalid

  defp error_category(_operation, :backup_missing), do: :filesystem_error
  defp error_category(:backup, :source_database_missing), do: :database_unavailable
  defp error_category(:migrate, {:migration_not_allowed, _status}), do: :migration_not_allowed
  defp error_category(:migrate, {:migration_failed, _reason}), do: :migration_failed
  defp error_category(:migrate, {:migration_failed_final_schema, _status}), do: :migration_failed
  defp error_category(:backup, {:source_schema_not_ready, _status}), do: :schema_not_ready
  defp error_category(:backup, {:backup_verification_failed, _reason}), do: :verification_failed
  defp error_category(:backup, _reason), do: :backup_failed
  defp error_category(:verify_backup, {:backup_schema_not_ready, _status}), do: :backup_invalid
  defp error_category(:verify_backup, {:integrity_check_failed, _rows}), do: :verification_failed

  defp error_category(:verify_backup, {:integrity_check_unavailable, _reason}),
    do: :verification_failed

  defp error_category(:verify_backup, _reason), do: :backup_invalid

  defp error_category(_operation, %{status: :database_unavailable}), do: :database_unavailable
  defp error_category(_operation, %{status: :invalid_database_path}), do: :invalid_configuration
  defp error_category(_operation, %{status: :invalid_configuration}), do: :invalid_configuration
  defp error_category(_operation, {:database_open_failed, _reason}), do: :database_unavailable

  defp error_category(_operation, {reason, _detail})
       when reason in [
              :source_database_not_regular_file,
              :source_database_stat_failed,
              :backup_parent_not_directory,
              :backup_parent_missing,
              :backup_parent_not_writable,
              :backup_destination_not_regular_file,
              :backup_destination_stat_failed,
              :backup_not_regular_file,
              :backup_stat_failed,
              :backup_checksum_failed
            ],
       do: :filesystem_error

  defp error_category(_operation, _reason), do: :invalid_configuration

  defp safe_reason(%{reason: reason}) when is_atom(reason), do: reason
  defp safe_reason(reason) when is_atom(reason), do: reason
  defp safe_reason({reason, _detail}) when is_atom(reason), do: reason
  defp safe_reason({reason, _detail, _extra}) when is_atom(reason), do: reason
  defp safe_reason(_reason), do: :maintenance_failed

  defp safe_details({:migration_not_allowed, status}), do: %{schema_status: status}
  defp safe_details({:migration_failed_final_schema, status}), do: %{schema_status: status}
  defp safe_details({:source_schema_not_ready, status}), do: %{schema_status: status}
  defp safe_details({:backup_schema_not_ready, status}), do: %{schema_status: status}

  defp safe_details({:backup_verification_failed, %{category: category}}),
    do: %{category: category}

  defp safe_details(%{status: status}), do: %{status: status}
  defp safe_details(_reason), do: %{}

  defp retryable?(:database_unavailable), do: true
  defp retryable?(:filesystem_error), do: true
  defp retryable?(_category), do: false
end
