defmodule FavnStorageSqlite.MaintenanceTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL
  alias FavnStorageSqlite.Maintenance
  alias FavnStorageSqlite.Migrations
  alias FavnStorageSqlite.Migrations.CreateFoundation
  alias FavnStorageSqlite.Repo
  alias FavnStorageSqlite.Supervisor, as: SQLiteSupervisor
  alias Mix.Tasks.Favn.Sqlite.Maintenance, as: MaintenanceTask

  setup do
    maybe_stop_process(Repo)

    previous_shell = Mix.shell()
    previous_adapter = Application.get_env(:favn_orchestrator, :storage_adapter)
    previous_opts = Application.get_env(:favn_orchestrator, :storage_adapter_opts)
    previous_dynamic = Application.get_env(:favn_orchestrator, :runtime_config_dynamic_env?)

    on_exit(fn ->
      Mix.shell(previous_shell)
      maybe_stop_process(Repo)
      restore_env(:storage_adapter, previous_adapter)
      restore_env(:storage_adapter_opts, previous_opts)
      restore_env(:runtime_config_dynamic_env?, previous_dynamic)
    end)

    :ok
  end

  test "status reuses diagnostics for an empty manual database" do
    db_path = temp_path("status-empty.db")

    assert {:ok, status} = Maintenance.status(database: db_path, migration_mode: :manual)

    assert status.adapter == :sqlite
    assert status.ready? == false
    assert status.status == :schema_not_ready
    assert status.schema.status == :empty_database
    refute inspect(status) =~ db_path

    rm_sqlite_files(db_path)
  end

  test "empty database migration dry-run reports planned versions without mutating" do
    db_path = temp_path("migrate-empty-dry-run.db")

    assert {:ok, result} =
             Maintenance.migrate([database: db_path, migration_mode: :manual], dry_run?: true)

    assert result.action == :dry_run
    assert result.previous_schema_status == :empty_database
    assert result.final_schema_status == :empty_database
    assert result.migrated_count > 0

    start_repo!(db_path)
    assert {:ok, diagnostics} = Migrations.schema_diagnostics(Repo)
    assert diagnostics.status == :empty_database

    rm_sqlite_files(db_path)
  end

  test "empty database migration applies all migrations" do
    db_path = temp_path("migrate-empty-apply.db")

    assert {:ok, result} =
             Maintenance.migrate([database: db_path, migration_mode: :manual], apply?: true)

    assert result.action == :migrated
    assert result.previous_schema_status == :empty_database
    assert result.final_schema_status == :ready
    assert result.migrated_count > 0

    start_repo!(db_path)
    assert Migrations.schema_ready?(Repo)

    rm_sqlite_files(db_path)
  end

  test "upgrade-required database migration applies missing migrations" do
    db_path = temp_path("migrate-upgrade.db")
    start_repo!(db_path)
    Ecto.Migrator.run(Repo, [{20_260_415_000_000, CreateFoundation}], :up, all: true)
    maybe_stop_process(Repo)

    assert {:ok, result} = Maintenance.migrate([database: db_path], apply?: true)

    assert result.action == :migrated
    assert result.previous_schema_status == :upgrade_required
    assert result.final_schema_status == :ready
    assert result.migrated_count > 0

    rm_sqlite_files(db_path)
  end

  test "ready database migration is a no-op success" do
    db_path = ready_database!("migrate-ready.db")

    assert {:ok, result} = Maintenance.migrate([database: db_path], apply?: true)

    assert result.action == :noop
    assert result.previous_schema_status == :ready
    assert result.final_schema_status == :ready
    assert result.migrated_count == 0

    rm_sqlite_files(db_path)
  end

  test "migration rejects ambiguous direct API options" do
    db_path = temp_path("migrate-ambiguous.db")

    assert {:error, error} =
             Maintenance.migrate([database: db_path], apply?: true, dry_run?: true)

    assert error.category == :invalid_configuration
    assert error.reason == :ambiguous_migration_options
    rm_sqlite_files(db_path)
  end

  test "maintenance task migrates when normal manual startup would reject" do
    db_path = temp_path("task-migrate-manual-upgrade.db")
    start_repo!(db_path)
    Ecto.Migrator.run(Repo, [{20_260_415_000_000, CreateFoundation}], :up, all: true)
    maybe_stop_process(Repo)

    Process.flag(:trap_exit, true)

    assert {:error, {%RuntimeError{message: message}, _stack}} =
             SQLiteSupervisor.start_link(database: db_path, migration_mode: :manual, pool_size: 1)

    assert message =~ "schema is not ready"
    assert message =~ "upgrade_required"

    Mix.shell(Mix.Shell.Process)
    Mix.Task.reenable("favn.sqlite.maintenance")
    Application.put_env(:favn_orchestrator, :runtime_config_dynamic_env?, true)
    Application.put_env(:favn_orchestrator, :storage_adapter, Favn.Storage.Adapter.SQLite)

    Application.put_env(:favn_orchestrator, :storage_adapter_opts,
      database: db_path,
      migration_mode: :manual,
      pool_size: 1
    )

    MaintenanceTask.run(["migrate", "--apply"])

    assert_receive {:mix_shell, :info, ["operation=migrate"]}
    assert_receive {:mix_shell, :info, ["final_schema_status=ready"]}

    start_repo!(db_path)
    assert Migrations.schema_ready?(Repo)

    rm_sqlite_files(db_path)
  end

  test "migration rejects newer, non-Favn, inconsistent, and invalid databases" do
    newer = ready_database!("migrate-newer.db")
    assert {:ok, _} = SQL.query(Repo, "INSERT INTO schema_migrations (version) VALUES (999)", [])
    maybe_stop_process(Repo)

    assert {:error, newer_error} = Maintenance.migrate([database: newer], apply?: true)
    assert newer_error.category == :migration_not_allowed
    assert newer_error.details.schema_status == :schema_newer_than_release

    non_favn = temp_path("migrate-non-favn.db")
    start_repo!(non_favn)
    assert {:ok, _} = SQL.query(Repo, "CREATE TABLE unrelated (id INTEGER PRIMARY KEY)", [])
    maybe_stop_process(Repo)

    assert {:error, non_favn_error} = Maintenance.migrate([database: non_favn], apply?: true)
    assert non_favn_error.category == :migration_not_allowed
    assert non_favn_error.details.schema_status == :schema_missing

    inconsistent = ready_database!("migrate-inconsistent.db")
    assert {:ok, _} = SQL.query(Repo, "DROP TABLE favn_runs", [])
    maybe_stop_process(Repo)

    assert {:error, inconsistent_error} =
             Maintenance.migrate([database: inconsistent], apply?: true)

    assert inconsistent_error.category == :migration_not_allowed
    assert inconsistent_error.details.schema_status == :schema_inconsistent

    invalid = temp_path("missing-parent/migrate-invalid.db")
    assert {:error, invalid_error} = Maintenance.migrate([database: invalid], apply?: true)
    assert invalid_error.category == :invalid_configuration
    refute inspect(invalid_error) =~ invalid

    Enum.each([newer, non_favn, inconsistent], &rm_sqlite_files/1)
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
      "favn_sqlite_maintenance_#{System.unique_integer([:positive, :monotonic])}_#{name}"
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

  defp restore_env(key, nil), do: Application.delete_env(:favn_orchestrator, key)
  defp restore_env(key, value), do: Application.put_env(:favn_orchestrator, key, value)
end
