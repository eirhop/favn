defmodule FavnOrchestrator.StorageMaintenanceTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias FavnOrchestrator.Operator.Maintenance
  alias FavnOrchestrator.Operator.Maintenance.BackupResult
  alias FavnOrchestrator.Operator.Maintenance.MaintenanceError
  alias FavnOrchestrator.Operator.Maintenance.StatusResult
  alias FavnOrchestrator.Operator.Maintenance.VerificationResult

  defmodule ProbeAdapter do
    @behaviour Favn.Storage.MaintenanceAdapter

    @impl true
    def maintenance_status(opts) do
      send(Keyword.fetch!(opts, :test_pid), {:maintenance_status, opts})

      {:ok,
       %{
         adapter: :probe,
         status: :ready,
         ready?: true,
         migration_mode: :manual,
         database: %{path: :redacted},
         schema: %{
           status: :ready,
           missing_versions: [],
           future_versions: [],
           missing_tables: [],
           hidden_repo: SecretRepo
         }
       }}
    end

    @impl true
    def migrate_storage(opts, command_opts) do
      send(Keyword.fetch!(opts, :test_pid), {:migrate_storage, opts, command_opts})

      {:ok,
       %{
         adapter: :probe,
         action: :dry_run,
         dry_run?: true,
         previous_schema_status: :upgrade_required,
         final_schema_status: :upgrade_required,
         migrated_versions: ["1"],
         duration_ms: 1
       }}
    end

    @impl true
    def backup_storage(opts, command_opts) do
      send(Keyword.fetch!(opts, :test_pid), {:backup_storage, opts, command_opts})
      result = Keyword.fetch!(opts, :backup_result)
      result
    end

    @impl true
    def verify_storage_backup(opts, command_opts) do
      send(Keyword.fetch!(opts, :test_pid), {:verify_storage_backup, opts, command_opts})

      {:ok,
       %{
         adapter: :probe,
         backup_status: :valid,
         integrity_check_status: :ok,
         schema_status: :ready,
         checksum: "abc",
         byte_size: 3
       }}
    end
  end

  defmodule UnsupportedAdapter do
    def child_spec(_opts), do: :none
  end

  setup do
    previous_adapter = Application.get_env(:favn_orchestrator, :storage_adapter)
    previous_opts = Application.get_env(:favn_orchestrator, :storage_adapter_opts)
    previous_dynamic = Application.get_env(:favn_orchestrator, :runtime_config_dynamic_env?)

    Application.put_env(:favn_orchestrator, :runtime_config_dynamic_env?, true)

    on_exit(fn ->
      restore_env(:storage_adapter, previous_adapter)
      restore_env(:storage_adapter_opts, previous_opts)
      restore_env(:runtime_config_dynamic_env?, previous_dynamic)
    end)

    :ok
  end

  test "operator status routes through configured maintenance adapter opts and returns DTO" do
    Application.put_env(:favn_orchestrator, :storage_adapter, ProbeAdapter)
    Application.put_env(:favn_orchestrator, :storage_adapter_opts, test_pid: self())

    assert {:ok, %StatusResult{} = result} = Maintenance.status([])

    assert_receive {:maintenance_status, opts}
    assert opts[:test_pid] == self()
    assert result.ready?
    assert result.schema_status == :ready
    refute Map.has_key?(result, :hidden_repo)
    refute inspect(result) =~ "SecretRepo"
  end

  test "unsupported adapters return stable errors without module internals" do
    Application.put_env(:favn_orchestrator, :storage_adapter, UnsupportedAdapter)
    Application.put_env(:favn_orchestrator, :storage_adapter_opts, [])

    assert {:error, %MaintenanceError{} = error} = Maintenance.status([])

    assert error.category == :unsupported_adapter
    assert error.adapter == :custom
    refute inspect(error) =~ "UnsupportedAdapter"
  end

  test "backup and verify DTOs normalize bounded adapter fields" do
    Application.put_env(:favn_orchestrator, :storage_adapter, ProbeAdapter)

    Application.put_env(:favn_orchestrator, :storage_adapter_opts,
      test_pid: self(),
      backup_result:
        {:ok,
         %{
           adapter: :probe,
           destination_identity: %{path: :redacted, basename: "backup.db"},
           byte_size: 10,
           checksum: "abc",
           checkpoint_policy: :passive_before_vacuum_into,
           verification: %{backup_status: :valid, schema_status: :ready}
         }}
    )

    assert {:ok, %BackupResult{} = backup} = Maintenance.backup(to: "/secret/backup.db")
    assert backup.destination_identity.path == :redacted
    assert backup.verification.backup_status == :valid

    assert {:ok, %VerificationResult{} = verification} =
             Maintenance.verify_backup(path: "/secret/backup.db")

    assert verification.schema_status == :ready
  end

  test "failure logs and telemetry redact untrusted metadata" do
    parent = self()
    ref = make_ref()
    secret = "/tmp/favn-secret-control-plane.db"

    :telemetry.attach(
      {__MODULE__, ref},
      [:favn, :orchestrator, :storage_maintenance],
      fn event, measurements, metadata, _config ->
        send(parent, {:telemetry, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach({__MODULE__, ref}) end)

    Application.put_env(:favn_orchestrator, :storage_adapter, ProbeAdapter)

    Application.put_env(:favn_orchestrator, :storage_adapter_opts,
      test_pid: self(),
      backup_result: {:error, %{category: :backup_failed, reason: "database=#{secret}"}}
    )

    log = capture_log(fn -> assert {:error, _error} = Maintenance.backup(to: secret) end)

    assert_receive {:telemetry, [:favn, :orchestrator, :storage_maintenance], %{duration_ms: _},
                    metadata}

    assert metadata.operation == :backup
    assert metadata.status == :error
    assert metadata.failure_category == :backup_failed
    refute inspect(metadata) =~ secret
    refute log =~ secret
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn_orchestrator, key)
  defp restore_env(key, value), do: Application.put_env(:favn_orchestrator, key, value)
end
