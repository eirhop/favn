defmodule FavnReferenceWorkload.ManifestGenerationTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest

  test "configured reference workload compiles into one canonical manifest" do
    assert {:ok, %Manifest{} = manifest} = Favn.generate_manifest()

    assert length(manifest.assets) == 15
    assert length(manifest.pipelines) == 1

    assert Enum.any?(manifest.assets, fn asset ->
             asset.ref == {FavnReferenceWorkload.Warehouse.Ops.ReferenceWorkloadComplete, :asset}
           end)

    assert Enum.any?(manifest.assets, fn asset ->
             asset.ref == {FavnReferenceWorkload.Warehouse.Gold.ExecutiveOverview, :asset}
           end)

    orders_asset =
      Enum.find(manifest.assets, fn asset ->
        asset.ref == {FavnReferenceWorkload.Warehouse.Raw.Orders, :asset}
      end)

    assert orders_asset.type == :elixir

    assert {FavnReferenceWorkload.Warehouse.Raw.Customers, :asset} in orders_asset.depends_on

    assert {FavnReferenceWorkload.Warehouse.Sources.ChannelCatalog, :asset} in orders_asset.depends_on

    order_facts_asset =
      Enum.find(manifest.assets, fn asset ->
        asset.ref == {FavnReferenceWorkload.Warehouse.Stg.OrderFacts, :asset}
      end)

    assert {FavnReferenceWorkload.Warehouse.Raw.Orders, :asset} in order_facts_asset.depends_on

    assert {FavnReferenceWorkload.Warehouse.Raw.OrderItems, :asset} in order_facts_asset.depends_on

    assert {FavnReferenceWorkload.Warehouse.Raw.Products, :asset} in order_facts_asset.depends_on
    assert {FavnReferenceWorkload.Warehouse.Raw.Payments, :asset} in order_facts_asset.depends_on
    assert {FavnReferenceWorkload.Warehouse.Stg.Customers, :asset} in order_facts_asset.depends_on

    [pipeline] = manifest.pipelines
    assert pipeline.name == :reference_workload_daily
    assert pipeline.deps == :all
  end
end
