defmodule Favn.TestSetup do
  @moduledoc false

  @type state :: %{
          previous_asset_modules: list(module()) | nil,
          previous_pipeline_modules: list(module()) | nil,
          previous_storage_adapter: module() | nil,
          previous_storage_adapter_opts: keyword() | nil,
          previous_orchestrator_storage_adapter: module() | nil,
          previous_orchestrator_storage_adapter_opts: keyword() | nil,
          previous_scheduler_opts: keyword() | nil,
          previous_connection_modules: list(module()) | nil,
          previous_connections: keyword() | map() | nil
        }

  @spec capture_state() :: state()
  def capture_state do
    %{
      previous_asset_modules: Application.get_env(:favn, :asset_modules),
      previous_pipeline_modules: Application.get_env(:favn, :pipeline_modules),
      previous_storage_adapter: Application.get_env(:favn, :storage_adapter),
      previous_storage_adapter_opts: Application.get_env(:favn, :storage_adapter_opts),
      previous_orchestrator_storage_adapter:
        Application.get_env(:favn_orchestrator, :storage_adapter),
      previous_orchestrator_storage_adapter_opts:
        Application.get_env(:favn_orchestrator, :storage_adapter_opts),
      previous_scheduler_opts: Application.get_env(:favn, :scheduler),
      previous_connection_modules: Application.get_env(:favn, :connection_modules),
      previous_connections: Application.get_env(:favn, :connections)
    }
  end

  @spec configure_storage_adapter(module(), keyword()) :: :ok
  def configure_storage_adapter(store, store_opts \\ []) do
    Application.put_env(:favn, :storage_adapter, store)
    Application.put_env(:favn, :storage_adapter_opts, store_opts)
    Application.put_env(:favn_orchestrator, :storage_adapter, store)
    Application.put_env(:favn_orchestrator, :storage_adapter_opts, store_opts)
  end

  @spec restore_state(state(), keyword()) :: :ok
  def restore_state(state, opts \\ []) do
    restore_env(:asset_modules, state.previous_asset_modules)

    if Keyword.get(opts, :clear_storage_adapter_env?, false) do
      Application.delete_env(:favn, :storage_adapter)
      Application.delete_env(:favn, :storage_adapter_opts)
      Application.delete_env(:favn_orchestrator, :storage_adapter)
      Application.delete_env(:favn_orchestrator, :storage_adapter_opts)
    else
      restore_env(:storage_adapter, state.previous_storage_adapter)
      restore_env(:storage_adapter_opts, state.previous_storage_adapter_opts)

      restore_env(
        :favn_orchestrator,
        :storage_adapter,
        state.previous_orchestrator_storage_adapter
      )

      restore_env(
        :favn_orchestrator,
        :storage_adapter_opts,
        state.previous_orchestrator_storage_adapter_opts
      )
    end

    restore_env(:pipeline_modules, state.previous_pipeline_modules)
    restore_env(:scheduler, state.previous_scheduler_opts)
    restore_env(:connection_modules, state.previous_connection_modules)
    restore_env(:connections, state.previous_connections)

    :ok
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn, key)
  defp restore_env(key, value), do: Application.put_env(:favn, key, value)

  defp restore_env(app, key, nil), do: Application.delete_env(app, key)
  defp restore_env(app, key, value), do: Application.put_env(app, key, value)
end
