defmodule Favn.Storage.MaintenanceAdapter do
  @moduledoc """
  Focused storage-adapter behaviour for operator maintenance commands.

  This contract is intentionally separate from `Favn.Storage.Adapter` so control-
  plane persistence callbacks do not grow SQLite-specific maintenance concerns.
  Implementations must return adapter-owned maps or stable error terms without
  exposing repos, schemas, migration modules, or adapter-private processes.
  """

  @type adapter_opts :: keyword()
  @type command_opts :: keyword()
  @type result :: {:ok, map()} | {:error, term()}

  @callback maintenance_status(adapter_opts()) :: result()
  @callback migrate_storage(adapter_opts(), command_opts()) :: result()
  @callback backup_storage(adapter_opts(), command_opts()) :: result()
  @callback verify_storage_backup(adapter_opts(), command_opts()) :: result()
end
