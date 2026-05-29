defmodule Mix.Tasks.Favn.Sqlite.MaintenanceTest do
  use ExUnit.Case, async: false

  alias Mix.Tasks.Favn.Sqlite.Maintenance, as: Task

  defmodule ProbeAdapter do
    @behaviour Favn.Storage.MaintenanceAdapter

    @impl true
    def maintenance_status(opts) do
      send(Keyword.fetch!(opts, :test_pid), {:task_status_called, opts})

      {:ok,
       %{
         adapter: :probe,
         status: :ready,
         ready?: true,
         migration_mode: :manual,
         database: %{path: :redacted},
         schema: %{status: :ready, missing_versions: [], future_versions: [], missing_tables: []}
       }}
    end

    @impl true
    def migrate_storage(opts, command_opts) do
      send(Keyword.fetch!(opts, :test_pid), {:task_migrate_called, command_opts})

      {:ok,
       %{
         adapter: :probe,
         action: :dry_run,
         dry_run?: true,
         previous_schema_status: :upgrade_required,
         final_schema_status: :upgrade_required,
         migrated_versions: ["1"],
         migrated_count: 1
       }}
    end

    @impl true
    def backup_storage(opts, command_opts) do
      send(Keyword.fetch!(opts, :test_pid), {:task_backup_called, command_opts})

      {:ok,
       %{
         adapter: :probe,
         destination_identity: %{path: :redacted, basename: "backup.db"},
         byte_size: 5,
         checksum: "abc",
         checkpoint_policy: :passive_before_vacuum_into,
         verification: %{backup_status: :valid, schema_status: :ready}
       }}
    end

    @impl true
    def verify_storage_backup(opts, command_opts) do
      send(Keyword.fetch!(opts, :test_pid), {:task_verify_called, command_opts})

      {:ok,
       %{
         adapter: :probe,
         backup_status: :valid,
         integrity_check_status: :ok,
         schema_status: :ready,
         byte_size: 5,
         checksum: "abc"
       }}
    end
  end

  setup do
    previous_shell = Mix.shell()
    Mix.shell(Mix.Shell.Process)
    Mix.Task.reenable("favn.sqlite.maintenance")

    previous_adapter = Application.get_env(:favn_orchestrator, :storage_adapter)
    previous_opts = Application.get_env(:favn_orchestrator, :storage_adapter_opts)
    previous_dynamic = Application.get_env(:favn_orchestrator, :runtime_config_dynamic_env?)

    Application.put_env(:favn_orchestrator, :runtime_config_dynamic_env?, true)
    Application.put_env(:favn_orchestrator, :storage_adapter, ProbeAdapter)
    Application.put_env(:favn_orchestrator, :storage_adapter_opts, test_pid: self())

    on_exit(fn ->
      Mix.shell(previous_shell)
      restore_env(:storage_adapter, previous_adapter)
      restore_env(:storage_adapter_opts, previous_opts)
      restore_env(:runtime_config_dynamic_env?, previous_dynamic)
    end)

    :ok
  end

  test "status task routes only through the operator facade to configured adapter" do
    Task.run(["status"])

    assert_receive {:task_status_called, opts}
    assert opts[:test_pid] == self()
    assert_receive {:mix_shell, :info, ["operation=status"]}
    assert_receive {:mix_shell, :info, ["adapter=probe"]}
  end

  test "migrate and backup parse command options" do
    Task.run(["migrate", "--dry-run"])
    assert_receive {:task_migrate_called, opts}
    assert opts[:dry_run?] == true
    assert opts[:apply?] == false

    Mix.Task.reenable("favn.sqlite.maintenance")
    Task.run(["backup", "--to", "/secret/backup.db", "--no-verify"])
    assert_receive {:task_backup_called, opts}
    assert opts[:to] == "/secret/backup.db"
    assert opts[:verify?] == false
  end

  test "backup rejects unsupported overwrite flag" do
    assert_raise Mix.Error, ~r/invalid sqlite maintenance command/, fn ->
      Task.run(["backup", "--to", "/secret/backup.db", "--overwrite"])
    end
  end

  test "verify-backup parses path and output remains redacted" do
    secret = "/tmp/secret/favn-control-plane.db"

    Task.run(["verify-backup", "--path", secret])

    assert_receive {:task_verify_called, opts}
    assert opts[:path] == secret

    refute_receive {:mix_shell, :info, [^secret]}
  end

  test "task source stays thin and does not reference SQLite internals" do
    source =
      __DIR__
      |> Path.join("../../../lib/mix/tasks/favn.sqlite.maintenance.ex")
      |> Path.expand()
      |> File.read!()

    assert source =~ "FavnOrchestrator.Operator.Maintenance"
    assert source =~ "FavnOrchestrator.Operator.MaintenanceBootstrap"
    refute source =~ "app.start"
    refute source =~ "FavnStorageSqlite.Repo"
    refute source =~ "FavnStorageSqlite.Migrations"
    refute source =~ "FavnStorageSqlite.Supervisor"
    refute source =~ "Ecto"
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn_orchestrator, key)
  defp restore_env(key, value), do: Application.put_env(:favn_orchestrator, key, value)
end
