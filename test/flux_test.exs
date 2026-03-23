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

  defmodule SpoofedAssets do
    def __flux_assets__, do: :oops
  end

  test "lists assets for a module through the public facade" do
    assert {:ok, assets} = Flux.list_assets(SampleAssets)

    Logger.debug("facade asset list: #{inspect(assets, pretty: true)}")

    assert Enum.map(assets, & &1.name) == [:extract_orders, :normalize_orders]
  end

  test "fetches an asset through the public facade" do
    assert {:ok, asset} = Flux.get_asset({SampleAssets, :normalize_orders})

    Logger.debug("facade asset lookup: #{inspect(asset, pretty: true)}")

    assert asset.depends_on == [{SampleAssets, :extract_orders}]

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
    assert {:error, :not_asset_module} = Flux.get_asset({SpoofedAssets, :normalize_orders})
  end
end
