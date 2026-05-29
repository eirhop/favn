defmodule FavnOrchestrator.Operator.MaintenanceBootstrap do
  @moduledoc """
  Minimal local operator bootstrap for storage maintenance commands.

  This intentionally does not start `FavnOrchestrator.Application`. It applies
  production storage environment when present, builds the runtime config from app
  env, and starts only dependency applications required by the configured storage
  maintenance adapter.
  """

  alias FavnOrchestrator.ProductionRuntimeConfig
  alias FavnOrchestrator.RuntimeConfig

  @doc "Bootstraps storage maintenance without starting the orchestrator tree."
  @spec bootstrap(map()) :: :ok | {:error, term()}
  def bootstrap(env \\ System.get_env()) when is_map(env) do
    with :ok <- ProductionRuntimeConfig.apply_storage_from_env_if_configured(env),
         %RuntimeConfig{} = runtime_config <- RuntimeConfig.from_app_env(),
         :ok <- start_adapter_dependencies(runtime_config.storage_adapter) do
      :ok
    end
  end

  defp start_adapter_dependencies(adapter) do
    case static_adapter_label(adapter) do
      :sqlite -> ensure_started(:favn_storage_sqlite)
      _other -> :ok
    end
  end

  defp ensure_started(app) do
    case Application.ensure_all_started(app) do
      {:ok, _apps} -> :ok
      {:error, reason} -> {:error, {:maintenance_dependency_start_failed, app, reason}}
    end
  end

  defp static_adapter_label(Favn.Storage.Adapter.SQLite), do: :sqlite
  defp static_adapter_label(_adapter), do: :custom
end
