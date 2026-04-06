defmodule Favn.TestSetup do
  @moduledoc false

  @type state :: %{
          previous_modules: list(module()) | nil,
          previous_catalog: {:ok, Favn.Assets.Registry.catalog()} | {:error, term()},
          previous_storage_adapter: module() | nil,
          previous_storage_adapter_opts: keyword() | nil
        }

  @spec capture_state() :: state()
  def capture_state do
    previous_modules = Application.get_env(:favn, :asset_modules)

    %{
      previous_modules: previous_modules,
      previous_catalog: Favn.Assets.Registry.build_catalog(previous_modules || []),
      previous_storage_adapter: Application.get_env(:favn, :storage_adapter),
      previous_storage_adapter_opts: Application.get_env(:favn, :storage_adapter_opts)
    }
  end

  @spec setup_asset_modules([module()], keyword()) :: :ok
  def setup_asset_modules(modules, opts \\ []) do
    Application.put_env(:favn, :asset_modules, modules)
    :ok = Favn.Assets.Registry.reload()

    if Keyword.get(opts, :reload_graph?, false) do
      :ok = Favn.Assets.GraphIndex.reload()
    end

    :ok
  end

  @spec configure_storage_adapter(module(), keyword()) :: :ok
  def configure_storage_adapter(store, store_opts \\ []) do
    Application.put_env(:favn, :storage_adapter, store)
    Application.put_env(:favn, :storage_adapter_opts, store_opts)
  end

  @spec clear_memory_storage_adapter() :: :ok
  def clear_memory_storage_adapter do
    table = Favn.Storage.Adapter.Memory.Table

    if :ets.whereis(table) != :undefined do
      :ets.delete_all_objects(table)
    end

    :ok
  end

  @spec restore_state(state(), keyword()) :: :ok
  def restore_state(state, opts \\ []) do
    restore_asset_modules(state.previous_modules)

    if Keyword.get(opts, :clear_storage_adapter_env?, false) do
      Application.delete_env(:favn, :storage_adapter)
      Application.delete_env(:favn, :storage_adapter_opts)
    else
      restore_env(:storage_adapter, state.previous_storage_adapter)
      restore_env(:storage_adapter_opts, state.previous_storage_adapter_opts)
    end

    restore_registry(state.previous_catalog, opts)
  end

  defp restore_asset_modules(nil), do: Application.delete_env(:favn, :asset_modules)
  defp restore_asset_modules(modules), do: Application.put_env(:favn, :asset_modules, modules)

  defp restore_registry({:ok, _catalog}, opts) do
    :ok = Favn.Assets.Registry.reload()

    if Keyword.get(opts, :reload_graph?, false) do
      :ok = Favn.Assets.GraphIndex.reload()
    end

    :ok
  end

  defp restore_registry({:error, _reason}, _opts), do: :ok

  defp restore_env(key, nil), do: Application.delete_env(:favn, key)
  defp restore_env(key, value), do: Application.put_env(:favn, key, value)
end
