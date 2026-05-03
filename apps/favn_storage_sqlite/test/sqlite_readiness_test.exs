defmodule FavnStorageSqlite.ReadinessTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL
  alias Favn.Storage.Adapter.SQLite, as: Adapter
  alias FavnStorageSqlite.Diagnostics
  alias FavnStorageSqlite.Migrations
  alias FavnStorageSqlite.Migrations.CreateFoundation
  alias FavnStorageSqlite.Repo
  alias FavnStorageSqlite.Supervisor, as: SQLiteSupervisor

  setup do
    maybe_stop_process(Repo)

    on_exit(fn ->
      maybe_stop_process(Repo)
    end)

    :ok
  end

  test "database path diagnostics require configured path" do
    assert {:error, :sqlite_database_required} = Diagnostics.validate_database_path(database: "")
    assert {:error, :sqlite_database_required} = Diagnostics.validate_database_path([])
  end

  test "adapter readiness delegates to redacted sqlite diagnostics" do
    db_path = temp_path("adapter-readiness-ready.db")
    repo_pid = start_repo!(db_path)
    :ok = Migrations.migrate!(Repo)
    maybe_stop_pid(repo_pid)

    assert {:ok, diagnostics} = Adapter.readiness(database: db_path, migration_mode: :manual)

    assert diagnostics.status == :ready
    assert diagnostics.ready?
    assert diagnostics.database == %{configured?: true, path: :redacted}
    assert diagnostics.schema.status == :ready
    refute inspect(diagnostics) =~ db_path

    File.rm(db_path)
  end

  test "readiness reports invalid paths without leaking configured path" do
    path = temp_path("missing-secret-parent/db.sqlite")

    assert {:error, error} = Adapter.readiness(database: path, migration_mode: :manual)

    assert error.status == :invalid_database_path
    assert error.reason == :database_parent_missing
    refute inspect(error) =~ path
    refute inspect(error) =~ "missing-secret-parent"
  end

  test "readiness reports an empty manual database as schema not ready" do
    db_path = temp_path("readiness-empty-manual.db")

    assert {:ok, diagnostics} = Diagnostics.readiness(database: db_path, migration_mode: :manual)

    assert diagnostics.status == :schema_not_ready
    refute diagnostics.ready?
    assert diagnostics.migration_mode == :manual
    assert diagnostics.database == %{configured?: true, path: :redacted}
    assert diagnostics.schema.status == :empty_database
    refute inspect(diagnostics) =~ db_path

    File.rm(db_path)
  end

  test "readiness reports ready schema" do
    db_path = temp_path("readiness-ready.db")
    repo_pid = start_repo!(db_path)
    :ok = Migrations.migrate!(Repo)
    maybe_stop_pid(repo_pid)

    assert {:ok, diagnostics} = Diagnostics.readiness(database: db_path, migration_mode: :manual)

    assert diagnostics.status == :ready
    assert diagnostics.ready?
    assert diagnostics.schema.status == :ready
    assert diagnostics.schema.missing_tables == []
    assert diagnostics.schema.missing_versions == []
    refute inspect(diagnostics) =~ db_path

    File.rm(db_path)
  end

  test "readiness reports newer schema as not ready" do
    db_path = temp_path("readiness-newer.db")
    repo_pid = start_repo!(db_path)
    :ok = Migrations.migrate!(Repo)

    assert {:ok, _} =
             SQL.query(
               Repo,
               "INSERT INTO schema_migrations (version) VALUES (99999999999999)",
               []
             )

    maybe_stop_pid(repo_pid)

    assert {:ok, diagnostics} = Diagnostics.readiness(database: db_path, migration_mode: :auto)

    assert diagnostics.status == :schema_not_ready
    refute diagnostics.ready?
    assert diagnostics.schema.status == :schema_newer_than_release
    assert diagnostics.schema.future_versions == ["99999999999999"]
    refute inspect(diagnostics) =~ db_path

    File.rm(db_path)
  end

  test "readiness reports inconsistent schema as not ready" do
    db_path = temp_path("readiness-inconsistent.db")
    repo_pid = start_repo!(db_path)
    :ok = Migrations.migrate!(Repo)
    assert {:ok, _} = SQL.query(Repo, "DROP TABLE favn_runs", [])
    maybe_stop_pid(repo_pid)

    assert {:ok, diagnostics} = Diagnostics.readiness(database: db_path, migration_mode: :auto)

    assert diagnostics.status == :schema_not_ready
    refute diagnostics.ready?
    assert diagnostics.schema.status == :schema_inconsistent
    assert "favn_runs" in diagnostics.schema.missing_tables
    refute inspect(diagnostics) =~ db_path

    File.rm(db_path)
  end

  test "readiness redacts diagnostics failures" do
    path = temp_path("invalid-mode-secret.db")

    assert {:error, error} = Adapter.readiness(database: path, migration_mode: {:bad, path})

    assert error == %{status: :invalid_configuration, reason: :invalid_migration_mode}
    refute inspect(error) =~ path
  end

  test "database path diagnostics can require absolute path" do
    assert {:error, {:database_path_not_absolute, "relative.db"}} =
             Diagnostics.validate_database_path(
               database: "relative.db",
               require_absolute_path: true
             )
  end

  test "database path diagnostics require existing parent directory" do
    path = temp_path("missing-parent/db.sqlite")

    assert {:error, {:database_parent_missing, _parent, :enoent}} =
             Diagnostics.validate_database_path(database: path)
  end

  test "adapter child_spec reports path diagnostics as recoverable errors" do
    path = temp_path("missing-parent/child-spec.sqlite")

    assert {:error, {:database_parent_missing, _parent, :enoent}} =
             Adapter.child_spec(database: path)

    assert {:error, {:database_path_not_absolute, "relative.db"}} =
             Adapter.child_spec(database: "relative.db", require_absolute_path: true)
  end

  test "adapter validates migration mode" do
    path = temp_path("invalid-mode.db")

    assert {:error, {:invalid_migration_mode, :bad}} =
             Adapter.child_spec(database: path, migration_mode: :bad)

    assert {:error, {:invalid_migration_mode, :bad}} =
             Adapter.get_run("run", database: path, migration_mode: :bad)
  end

  test "database path diagnostics reject directory targets" do
    path = temp_dir("directory-target")

    assert {:error, {:database_target_not_regular_file, ^path, :directory}} =
             Diagnostics.validate_database_path(database: path)

    File.rm_rf!(path)
  end

  test "schema diagnostics classify an empty database" do
    db_path = temp_path("empty.db")
    start_repo!(db_path)

    assert {:ok, diagnostics} = Migrations.schema_diagnostics(Repo)
    assert diagnostics.status == :empty_database
    assert diagnostics.applied_versions == []
    refute Migrations.schema_ready?(Repo)

    File.rm(db_path)
  end

  test "schema diagnostics classify ready current schema" do
    db_path = temp_path("ready.db")
    start_repo!(db_path)
    :ok = Migrations.migrate!(Repo)

    assert {:ok, diagnostics} = Migrations.schema_diagnostics(Repo)
    assert diagnostics.status == :ready
    assert diagnostics.missing_tables == []
    assert diagnostics.missing_versions == []
    assert Migrations.schema_ready?(Repo)

    File.rm(db_path)
  end

  test "schema diagnostics classify schema missing" do
    db_path = temp_path("schema-missing.db")
    start_repo!(db_path)
    assert {:ok, _} = SQL.query(Repo, "CREATE TABLE unrelated (id INTEGER PRIMARY KEY)", [])

    assert {:ok, diagnostics} = Migrations.schema_diagnostics(Repo)
    assert diagnostics.status == :schema_missing
    assert diagnostics.applied_versions == []
    refute Migrations.schema_ready?(Repo)

    File.rm(db_path)
  end

  test "schema diagnostics classify upgrade required" do
    db_path = temp_path("upgrade-required.db")
    start_repo!(db_path)

    Ecto.Migrator.run(Repo, [{20_260_415_000_000, CreateFoundation}], :up, all: true)

    assert {:ok, diagnostics} = Migrations.schema_diagnostics(Repo)
    assert diagnostics.status == :upgrade_required

    assert diagnostics.missing_versions == [
             "20260428100000",
             "20260502100000",
             "20260503100000"
           ]

    refute Migrations.schema_ready?(Repo)

    File.rm(db_path)
  end

  test "schema diagnostics classify newer schema" do
    db_path = temp_path("newer.db")
    start_repo!(db_path)
    :ok = Migrations.migrate!(Repo)

    assert {:ok, _} =
             SQL.query(
               Repo,
               "INSERT INTO schema_migrations (version) VALUES (99999999999999)",
               []
             )

    assert {:ok, diagnostics} = Migrations.schema_diagnostics(Repo)
    assert diagnostics.status == :schema_newer_than_release
    assert diagnostics.future_versions == ["99999999999999"]
    refute Migrations.schema_ready?(Repo)

    File.rm(db_path)
  end

  test "schema diagnostics classify inconsistent schema" do
    db_path = temp_path("inconsistent.db")
    start_repo!(db_path)
    :ok = Migrations.migrate!(Repo)
    assert {:ok, _} = SQL.query(Repo, "DROP TABLE favn_runs", [])

    assert {:ok, diagnostics} = Migrations.schema_diagnostics(Repo)
    assert diagnostics.status == :schema_inconsistent
    assert "favn_runs" in diagnostics.missing_tables
    refute Migrations.schema_ready?(Repo)

    File.rm(db_path)
  end

  test "manual startup can initialize an empty database when enabled" do
    db_path = temp_path("manual-init-empty.db")

    {:ok, pid} =
      SQLiteSupervisor.start_link(
        database: db_path,
        migration_mode: :manual,
        initialize_empty?: true,
        pool_size: 1
      )

    assert Migrations.schema_ready?(Repo)
    maybe_stop_pid(pid)
    File.rm(db_path)
  end

  test "manual startup rejects an upgrade without auto migrating" do
    db_path = temp_path("manual-upgrade-required.db")
    repo_pid = start_repo!(db_path)
    Ecto.Migrator.run(Repo, [{20_260_415_000_000, CreateFoundation}], :up, all: true)
    maybe_stop_pid(repo_pid)

    Process.flag(:trap_exit, true)

    assert {:error, {%RuntimeError{message: message}, _stack}} =
             SQLiteSupervisor.start_link(database: db_path, migration_mode: :manual, pool_size: 1)

    assert message =~ "favn sqlite schema is not ready"
    assert message =~ "upgrade_required"

    start_repo!(db_path)
    assert {:ok, diagnostics} = Migrations.schema_diagnostics(Repo)
    assert diagnostics.status == :upgrade_required

    File.rm(db_path)
  end

  test "auto startup rejects a schema newer than the release" do
    db_path = temp_path("auto-newer.db")
    repo_pid = start_repo!(db_path)
    :ok = Migrations.migrate!(Repo)

    assert {:ok, _} =
             SQL.query(
               Repo,
               "INSERT INTO schema_migrations (version) VALUES (99999999999999)",
               []
             )

    maybe_stop_pid(repo_pid)

    Process.flag(:trap_exit, true)

    assert {:error, {%RuntimeError{message: message}, _stack}} =
             SQLiteSupervisor.start_link(database: db_path, migration_mode: :auto, pool_size: 1)

    assert message =~ "schema is not ready before migrations"
    assert message =~ "schema_newer_than_release"

    File.rm(db_path)
  end

  test "auto startup rejects an inconsistent schema after migrations" do
    db_path = temp_path("auto-inconsistent.db")
    repo_pid = start_repo!(db_path)
    :ok = Migrations.migrate!(Repo)
    assert {:ok, _} = SQL.query(Repo, "DROP TABLE favn_runs", [])
    maybe_stop_pid(repo_pid)

    Process.flag(:trap_exit, true)

    assert {:error, {%RuntimeError{message: message}, _stack}} =
             SQLiteSupervisor.start_link(database: db_path, migration_mode: :auto, pool_size: 1)

    assert message =~ "schema is not ready before migrations"
    assert message =~ "schema_inconsistent"

    File.rm(db_path)
  end

  test "auto startup rejects a non-favn schema before migrating" do
    db_path = temp_path("auto-schema-missing.db")
    repo_pid = start_repo!(db_path)
    assert {:ok, _} = SQL.query(Repo, "CREATE TABLE unrelated (id INTEGER PRIMARY KEY)", [])
    maybe_stop_pid(repo_pid)

    Process.flag(:trap_exit, true)

    assert {:error, {%RuntimeError{message: message}, _stack}} =
             SQLiteSupervisor.start_link(database: db_path, migration_mode: :auto, pool_size: 1)

    assert message =~ "schema is not ready before migrations"
    assert message =~ "schema_missing"

    start_repo!(db_path)
    assert {:ok, diagnostics} = Migrations.schema_diagnostics(Repo)
    assert diagnostics.status == :schema_missing

    File.rm(db_path)
  end

  defp start_repo!(db_path) do
    maybe_stop_process(Repo)
    {:ok, pid} = Repo.start_link(database: db_path, pool_size: 1, busy_timeout: 5_000)
    Process.unlink(pid)
    on_exit(fn -> maybe_stop_pid(pid) end)
    pid
  end

  defp temp_path(name) do
    Path.join(
      System.tmp_dir!(),
      "favn_sqlite_readiness_#{System.unique_integer([:positive, :monotonic])}_#{name}"
    )
  end

  defp temp_dir(name) do
    path = temp_path(name)
    File.mkdir_p!(path)
    path
  end

  defp maybe_stop_process(name) do
    case Process.whereis(name) do
      nil -> :ok
      pid -> maybe_stop_pid(pid)
    end
  end

  defp maybe_stop_pid(nil), do: :ok

  defp maybe_stop_pid(pid) when is_pid(pid) do
    if Process.alive?(pid) do
      Process.unlink(pid)
      Supervisor.stop(pid, :normal, 5_000)
    else
      :ok
    end
  end
end
