defmodule FavnStorageSqlite.BackupTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL
  alias FavnStorageSqlite.Maintenance
  alias FavnStorageSqlite.Migrations
  alias FavnStorageSqlite.Repo

  setup do
    maybe_stop_process(Repo)

    on_exit(fn -> maybe_stop_process(Repo) end)

    :ok
  end

  test "valid live database backup verifies and remains redacted" do
    source = ready_database!("backup-source.db")
    destination = temp_path("backup-destination.db")

    assert {:ok, backup} = Maintenance.backup([database: source], to: destination)

    assert backup.destination_identity.path == :redacted
    assert backup.destination_identity.basename == Path.basename(destination)
    assert backup.byte_size > 0
    assert is_binary(backup.checksum)
    assert backup.checkpoint_policy == :passive_before_vacuum_into
    assert backup.verification.backup_status == :valid
    assert backup.verification.schema_status == :ready
    refute inspect(backup) =~ source
    refute inspect(backup) =~ destination

    assert {:ok, verification} = Maintenance.verify_backup([database: source], path: destination)
    assert verification.backup_status == :valid
    assert verification.integrity_check_status == :ok
    assert verification.schema_status == :ready

    rm_sqlite_files(source)
    rm_sqlite_files(destination)
  end

  test "backup verification handles missing, wrong, and corrupt files" do
    source = ready_database!("verify-source.db")
    missing = temp_path("verify-missing.db")

    assert {:error, missing_error} = Maintenance.verify_backup([database: source], path: missing)
    assert missing_error.category == :filesystem_error
    assert missing_error.reason == :backup_missing
    refute inspect(missing_error) =~ missing

    wrong = temp_path("verify-wrong.db")
    start_repo!(wrong)
    assert {:ok, _} = SQL.query(Repo, "CREATE TABLE unrelated (id INTEGER PRIMARY KEY)", [])
    maybe_stop_process(Repo)

    assert {:error, wrong_error} = Maintenance.verify_backup([database: source], path: wrong)
    assert wrong_error.category == :backup_invalid
    assert wrong_error.details.schema_status == :schema_missing

    corrupt = temp_path("verify-corrupt.db")
    File.write!(corrupt, "not a sqlite database")

    assert {:error, corrupt_error} = Maintenance.verify_backup([database: source], path: corrupt)
    assert corrupt_error.category in [:backup_invalid, :verification_failed]
    refute inspect(corrupt_error) =~ corrupt

    Enum.each([source, wrong, corrupt], &rm_sqlite_files/1)
  end

  test "backup rejects unsafe destinations" do
    source = ready_database!("backup-unsafe-source.db")
    destination = temp_path("backup-existing.db")
    File.write!(destination, "existing")

    assert {:error, same_source_error} = Maintenance.backup([database: source], to: source)
    assert same_source_error.category == :backup_invalid
    assert same_source_error.reason == :backup_same_as_source

    assert {:error, exists_error} = Maintenance.backup([database: source], to: destination)
    assert exists_error.category == :backup_invalid
    assert exists_error.reason == :backup_destination_exists

    rm_sqlite_files(source)
    rm_sqlite_files(destination)
  end

  test "WAL-backed uncheckpointed writes are backed up with VACUUM INTO" do
    source = ready_database!("backup-wal-source.db")
    destination = temp_path("backup-wal-destination.db")

    assert {:ok, _} = SQL.query(Repo, "PRAGMA journal_mode=WAL", [])

    assert {:ok, _} =
             SQL.query(
               Repo,
               "INSERT INTO favn_runtime_settings (key, value_text, updated_at) VALUES ('wal_key', 'wal_value', ?1)",
               [DateTime.utc_now()]
             )

    assert File.exists?(source <> "-wal")

    assert {:ok, backup} = Maintenance.backup([database: source], to: destination)
    assert backup.verification.backup_status == :valid

    maybe_stop_process(Repo)
    start_repo!(destination)

    assert {:ok, %{rows: [["wal_value"]]}} =
             SQL.query(
               Repo,
               "SELECT value_text FROM favn_runtime_settings WHERE key = 'wal_key'",
               []
             )

    rm_sqlite_files(source)
    rm_sqlite_files(destination)
  end

  defp ready_database!(name) do
    db_path = temp_path(name)
    start_repo!(db_path)
    :ok = Migrations.migrate!(Repo)
    db_path
  end

  defp start_repo!(db_path) do
    maybe_stop_process(Repo)
    {:ok, pid} = Repo.start_link(database: db_path, pool_size: 1, busy_timeout: 5_000)
    Process.unlink(pid)
    pid
  end

  defp temp_path(name) do
    Path.join(
      System.tmp_dir!(),
      "favn_sqlite_backup_#{System.unique_integer([:positive, :monotonic])}_#{name}"
    )
  end

  defp rm_sqlite_files(path) do
    maybe_stop_process(Repo)
    File.rm(path)
    File.rm(path <> "-wal")
    File.rm(path <> "-shm")
  end

  defp maybe_stop_process(name) do
    case Process.whereis(name) do
      nil -> :ok
      pid -> maybe_stop_pid(pid)
    end
  end

  defp maybe_stop_pid(pid) when is_pid(pid) do
    if Process.alive?(pid) do
      Process.unlink(pid)
      GenServer.stop(pid, :normal, 5_000)
    end
  end
end
