defmodule FavnOrchestrator.Storage.Maintenance do
  @moduledoc """
  Storage-owned dispatch for control-plane maintenance operations.

  The dispatcher calls only the configured storage adapter and the focused
  `Favn.Storage.MaintenanceAdapter` callbacks. Adapter-private repos, schemas,
  migrations, and supervisors remain behind the adapter boundary.
  """

  alias Favn.Storage.MaintenanceAdapter
  alias FavnOrchestrator.Operator.Maintenance.MaintenanceError
  alias FavnOrchestrator.RuntimeConfig

  @type operation :: :status | :migrate | :backup | :verify_backup

  @doc "Returns adapter-owned maintenance status data."
  @spec status(keyword()) :: {:ok, map()} | {:error, MaintenanceError.t()}
  def status(opts \\ []) when is_list(opts), do: dispatch(:status, opts)

  @doc "Runs or plans explicit storage migration through the configured adapter."
  @spec migrate(keyword()) :: {:ok, map()} | {:error, MaintenanceError.t()}
  def migrate(opts \\ []) when is_list(opts), do: dispatch(:migrate, opts)

  @doc "Creates a control-plane backup through the configured adapter."
  @spec backup(keyword()) :: {:ok, map()} | {:error, MaintenanceError.t()}
  def backup(opts \\ []) when is_list(opts), do: dispatch(:backup, opts)

  @doc "Verifies a control-plane backup through the configured adapter."
  @spec verify_backup(keyword()) :: {:ok, map()} | {:error, MaintenanceError.t()}
  def verify_backup(opts \\ []) when is_list(opts), do: dispatch(:verify_backup, opts)

  defp dispatch(operation, opts) do
    runtime_config = RuntimeConfig.current()
    adapter = runtime_config.storage_adapter
    adapter_label = adapter_label(adapter)

    with :ok <- ensure_maintenance_adapter(adapter, operation, adapter_label) do
      call_adapter(operation, adapter, runtime_config.storage_adapter_opts, opts)
      |> normalize_response(operation, adapter_label)
    end
  rescue
    error ->
      {:error, MaintenanceError.new(operation, :invalid_configuration, error, adapter: :unknown)}
  catch
    kind, reason ->
      {:error,
       MaintenanceError.new(operation, :invalid_configuration, {kind, reason}, adapter: :unknown)}
  end

  defp ensure_maintenance_adapter(adapter, operation, adapter_label) do
    with {:module, ^adapter} <- Code.ensure_loaded(adapter),
         true <- implements_maintenance?(adapter) do
      :ok
    else
      _ ->
        {:error,
         MaintenanceError.new(operation, :unsupported_adapter, :maintenance_not_supported,
           adapter: adapter_label
         )}
    end
  end

  defp implements_maintenance?(adapter) do
    callbacks = MaintenanceAdapter.behaviour_info(:callbacks)
    Enum.all?(callbacks, fn {name, arity} -> function_exported?(adapter, name, arity) end)
  end

  defp call_adapter(:status, adapter, adapter_opts, _opts),
    do: adapter.maintenance_status(adapter_opts)

  defp call_adapter(:migrate, adapter, adapter_opts, opts),
    do: adapter.migrate_storage(adapter_opts, opts)

  defp call_adapter(:backup, adapter, adapter_opts, opts),
    do: adapter.backup_storage(adapter_opts, opts)

  defp call_adapter(:verify_backup, adapter, adapter_opts, opts),
    do: adapter.verify_storage_backup(adapter_opts, opts)

  defp normalize_response({:ok, result}, _operation, adapter_label) when is_map(result),
    do: {:ok, Map.put_new(result, :adapter, adapter_label)}

  defp normalize_response({:error, reason}, operation, adapter_label),
    do: {:error, MaintenanceError.normalize(operation, reason, adapter: adapter_label)}

  defp normalize_response(other, operation, adapter_label) do
    {:error,
     MaintenanceError.new(operation, :invalid_configuration, {:invalid_adapter_response, other},
       adapter: adapter_label
     )}
  end

  defp adapter_label(Favn.Storage.Adapter.SQLite), do: :sqlite
  defp adapter_label(FavnOrchestrator.Storage.Adapter.Memory), do: :memory
  defp adapter_label(Favn.Storage.Adapter.Postgres), do: :postgres
  defp adapter_label(_adapter), do: :custom
end
