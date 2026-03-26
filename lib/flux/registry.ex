defmodule Flux.Registry do
  @moduledoc """
  Global asset discovery and lookup for configured Flux asset modules.

  The registry keeps global discovery explicit by reading the configured
  `:asset_modules` scope for the `:flux` application rather than scanning all
  loaded modules in the VM.

  Registry data is loaded during application startup and cached in
  `:persistent_term` for frequent read access. The registry is treated as
  immutable for the lifetime of the booted application and is rebuilt only
  when the compiled asset module configuration changes.
  """

  alias Flux.Asset
  alias Flux.Ref

  @catalog_key {__MODULE__, :catalog}

  @typedoc """
  Registry loading errors.
  """
  @type error ::
          {:invalid_asset_module, module()}
          | {:duplicate_asset, Flux.asset_ref()}

  @doc """
  List all globally configured assets.
  """
  @spec list_assets() :: {:ok, [Asset.t()]} | {:error, error()}
  def list_assets do
    with {:ok, catalog} <- cached_catalog() do
      {:ok, catalog.assets}
    end
  end

  @doc """
  Fetch a globally configured asset by canonical reference.
  """
  @spec get_asset(Ref.t()) :: {:ok, Asset.t()} | {:error, error() | :asset_not_found}
  def get_asset({module, name} = ref) when is_atom(module) and is_atom(name) do
    with {:ok, catalog} <- cached_catalog() do
      case Map.fetch(catalog.assets_by_ref, ref) do
        {:ok, %Asset{} = asset} -> {:ok, asset}
        :error -> {:error, :asset_not_found}
      end
    end
  end

  @doc """
  Build and cache the registry catalog from configured asset modules.
  """
  @spec load() :: :ok | {:error, error()}
  def load do
    with {:ok, catalog} <- build_catalog() do
      :persistent_term.put(@catalog_key, catalog)
      :ok
    end
  end

  @doc false
  @spec reload() :: :ok | {:error, error()}
  def reload, do: load()

  @doc """
  Return the configured asset modules for global discovery.
  """
  @spec configured_modules() :: [module()]
  def configured_modules do
    :flux
    |> Application.get_env(:asset_modules, [])
    |> Enum.uniq()
  end

  @doc """
  Build a canonical asset catalog from configured asset modules.
  """
  @spec build_catalog() ::
          {:ok, %{assets: [Asset.t()], assets_by_ref: %{Ref.t() => Asset.t()}}}
          | {:error, error()}
  def build_catalog do
    configured_modules()
    |> build_catalog()
  end

  @doc false
  @spec build_catalog([module()]) ::
          {:ok, %{assets: [Asset.t()], assets_by_ref: %{Ref.t() => Asset.t()}}}
          | {:error, error()}
  def build_catalog(modules) when is_list(modules) do
    modules
    |> Enum.reduce_while({:ok, %{assets: [], assets_by_ref: %{}}}, fn module, {:ok, catalog} ->
      if Flux.asset_module?(module) do
        case merge_assets(catalog, module.__flux_assets__()) do
          {:ok, updated_catalog} -> {:cont, {:ok, updated_catalog}}
          {:error, _reason} = error -> {:halt, error}
        end
      else
        {:halt, {:error, {:invalid_asset_module, module}}}
      end
    end)
    |> case do
      {:ok, catalog} -> {:ok, %{catalog | assets: Enum.reverse(catalog.assets)}}
      {:error, _reason} = error -> error
    end
  end

  defp cached_catalog do
    case :persistent_term.get(@catalog_key, :undefined) do
      :undefined -> load_and_fetch_catalog()
      catalog -> {:ok, catalog}
    end
  end

  defp load_and_fetch_catalog do
    with :ok <- load() do
      {:ok, :persistent_term.get(@catalog_key)}
    end
  end

  defp merge_assets(catalog, assets) do
    assets
    |> Enum.reduce_while({:ok, catalog}, fn %Asset{} = asset, {:ok, acc} ->
      if Map.has_key?(acc.assets_by_ref, asset.ref) do
        {:halt, {:error, {:duplicate_asset, asset.ref}}}
      else
        {:cont,
         {:ok,
          %{
            assets: [asset | acc.assets],
            assets_by_ref: Map.put(acc.assets_by_ref, asset.ref, asset)
          }}}
      end
    end)
  end
end
