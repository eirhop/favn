defmodule FluxTest do
  use ExUnit.Case

  doctest Flux

  require Logger

  defmodule SampleAssets do
    use Flux.Assets

    @doc "Extract raw orders"
    @asset true
    def extract_orders, do: [%{id: 1}]

    @doc "Normalize extracted orders"
    @asset depends_on: [:extract_orders], tags: [:sales]
    def normalize_orders(orders), do: orders
  end

  defmodule CrossModuleAssets do
    use Flux.Assets

    alias FluxTest.SampleAssets

    @doc "Publish normalized orders"
    @asset depends_on: [{SampleAssets, :normalize_orders}], tags: [:reporting]
    def publish_orders(orders), do: orders
  end

  defmodule SpoofedAssets do
    def __flux_assets__, do: :oops
  end

  setup do
    previous_modules = Application.get_env(:flux, :asset_modules)
    previous_catalog = Flux.Registry.build_catalog(previous_modules || [])

    on_exit(fn ->
      if is_nil(previous_modules) do
        Application.delete_env(:flux, :asset_modules)
      else
        Application.put_env(:flux, :asset_modules, previous_modules)
      end

      restore_registry(previous_catalog)
    end)

    :ok
  end

  test "lists globally configured assets through the registry-backed facade" do
    Application.put_env(:flux, :asset_modules, [SampleAssets, CrossModuleAssets])
    assert :ok = Flux.Registry.reload()

    assert {:ok, assets} = Flux.list_assets()

    assert Enum.map(assets, & &1.ref) == [
             {SampleAssets, :extract_orders},
             {SampleAssets, :normalize_orders},
             {CrossModuleAssets, :publish_orders}
           ]
  end

  test "lists assets for a module through the public facade" do
    assert {:ok, assets} = Flux.list_assets(SampleAssets)

    Logger.debug("facade asset list: #{inspect(assets, pretty: true)}")

    assert Enum.map(assets, & &1.name) == [:extract_orders, :normalize_orders]
  end

  test "fetches an asset through the public facade" do
    Application.put_env(:flux, :asset_modules, [SampleAssets, CrossModuleAssets])
    assert :ok = Flux.Registry.reload()

    assert {:ok, asset} = Flux.get_asset({SampleAssets, :normalize_orders})
    assert {:ok, cross_module_asset} = Flux.get_asset({CrossModuleAssets, :publish_orders})

    Logger.debug("facade asset lookup: #{inspect(asset, pretty: true)}")

    assert asset.depends_on == [{SampleAssets, :extract_orders}]
    assert cross_module_asset.depends_on == [{SampleAssets, :normalize_orders}]

    assert {:error, :asset_not_found} = Flux.get_asset({SampleAssets, :missing})
    assert {:error, :not_asset_module} = Flux.get_asset({Enum, :map})
  end

  test "reports whether a module exposes Flux asset metadata" do
    assert Flux.asset_module?(SampleAssets)
    refute Flux.asset_module?(Enum)
    refute Flux.asset_module?(SpoofedAssets)
  end

  test "rejects modules that only spoof __flux_assets__/0" do
    assert {:error, :not_asset_module} = Flux.list_assets(SpoofedAssets)
    Application.put_env(:flux, :asset_modules, [SpoofedAssets])

    assert {:error, {:invalid_asset_module, SpoofedAssets}} = Flux.Registry.reload()
    assert {:error, :not_asset_module} = Flux.get_asset({SpoofedAssets, :normalize_orders})
  end

  test "reports invalid globally configured modules" do
    Application.put_env(:flux, :asset_modules, [SampleAssets, Enum])

    assert {:error, {:invalid_asset_module, Enum}} = Flux.Registry.reload()
  end

  test "reads from the startup-loaded cache until the registry is reloaded" do
    Application.put_env(:flux, :asset_modules, [SampleAssets])
    assert :ok = Flux.Registry.reload()
    assert {:ok, assets} = Flux.list_assets()

    assert Enum.map(assets, & &1.ref) == [
             {SampleAssets, :extract_orders},
             {SampleAssets, :normalize_orders}
           ]

    Application.put_env(:flux, :asset_modules, [SampleAssets, CrossModuleAssets])

    assert {:ok, cached_assets} = Flux.list_assets()

    assert Enum.map(cached_assets, & &1.ref) == [
             {SampleAssets, :extract_orders},
             {SampleAssets, :normalize_orders}
           ]

    assert :ok = Flux.Registry.reload()
    assert {:ok, reloaded_assets} = Flux.list_assets()

    assert Enum.map(reloaded_assets, & &1.ref) == [
             {SampleAssets, :extract_orders},
             {SampleAssets, :normalize_orders},
             {CrossModuleAssets, :publish_orders}
           ]
  end

  defp restore_registry({:ok, _catalog}), do: :ok = Flux.Registry.reload()
  defp restore_registry({:error, _reason}), do: :ok
end
