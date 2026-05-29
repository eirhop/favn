defmodule FavnOrchestrator.Operator.Maintenance do
  @moduledoc """
  Public same-BEAM operator contract for control-plane maintenance.

  Callers receive explicit DTO structs and stable maintenance errors. They do not
  receive Ecto repos, schemas, migrations, supervisors, or adapter-private state.
  """

  alias FavnOrchestrator.OperationalEvents
  alias FavnOrchestrator.Operator.Maintenance.BackupResult
  alias FavnOrchestrator.Operator.Maintenance.MaintenanceError
  alias FavnOrchestrator.Operator.Maintenance.MigrationResult
  alias FavnOrchestrator.Operator.Maintenance.StatusResult
  alias FavnOrchestrator.Operator.Maintenance.VerificationResult
  alias FavnOrchestrator.Storage.Maintenance, as: StorageMaintenance

  require Logger

  @type result(result) :: {:ok, result} | {:error, MaintenanceError.t()}

  @doc "Returns control-plane storage maintenance status."
  @spec status(keyword()) :: result(StatusResult.t())
  def status(opts \\ []) when is_list(opts) do
    run(:status, opts, &StorageMaintenance.status/1, &StatusResult.new/1)
  end

  @doc "Runs or dry-runs explicit control-plane storage migration."
  @spec migrate(keyword()) :: result(MigrationResult.t())
  def migrate(opts \\ []) when is_list(opts) do
    run(:migrate, opts, &StorageMaintenance.migrate/1, &MigrationResult.new/1)
  end

  @doc "Creates a control-plane storage backup."
  @spec backup(keyword()) :: result(BackupResult.t())
  def backup(opts \\ []) when is_list(opts) do
    run(:backup, opts, &StorageMaintenance.backup/1, &BackupResult.new/1)
  end

  @doc "Verifies a control-plane storage backup."
  @spec verify_backup(keyword()) :: result(VerificationResult.t())
  def verify_backup(opts \\ []) when is_list(opts) do
    run(:verify_backup, opts, &StorageMaintenance.verify_backup/1, &VerificationResult.new/1)
  end

  defp run(operation, opts, storage_fun, dto_fun) do
    started = System.monotonic_time()

    result =
      case storage_fun.(opts) do
        {:ok, attrs} -> {:ok, dto_fun.(attrs)}
        {:error, %MaintenanceError{} = error} -> {:error, error}
        {:error, reason} -> {:error, MaintenanceError.normalize(operation, reason, [])}
      end

    emit(operation, started, result)
    result
  end

  defp emit(operation, started, result) do
    duration_ms =
      System.convert_time_unit(System.monotonic_time() - started, :native, :millisecond)

    metadata = event_metadata(operation, result, duration_ms)
    level = if match?({:ok, _result}, result), do: :info, else: :warning

    OperationalEvents.emit(
      :storage_maintenance,
      %{duration_ms: duration_ms},
      metadata,
      level: level
    )
  end

  defp event_metadata(operation, {:ok, result}, duration_ms) do
    %{
      operation: operation,
      status: :ok,
      adapter: Map.get(result, :adapter),
      duration_ms: duration_ms,
      schema_status: Map.get(result, :schema_status) || Map.get(result, :final_schema_status),
      migration_mode: Map.get(result, :migration_mode)
    }
  end

  defp event_metadata(operation, {:error, %MaintenanceError{} = error}, duration_ms) do
    %{
      operation: operation,
      status: :error,
      adapter: error.adapter,
      duration_ms: duration_ms,
      failure_category: error.category,
      reason: error.reason
    }
  end
end
