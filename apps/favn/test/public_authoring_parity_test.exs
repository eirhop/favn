defmodule Favn.PublicAuthoringParityTest do
  use ExUnit.Case, async: true

  alias Favn.Test.Fixtures.Assets.Basic.AdditionalAssets
  alias Favn.Test.Fixtures.Assets.Basic.CrossModuleAssets
  alias Favn.Test.Fixtures.Assets.Basic.SampleAssets
  alias Favn.Test.Fixtures.Assets.Basic.SpoofedAssets
  alias FavnTestSupport.Fixtures

  defmodule RelationOrders do
    use Favn.Namespace, relation: [connection: :warehouse, catalog: "raw", schema: "commerce"]
    use Favn.Asset

    @relation [name: "orders"]
    def asset(_ctx), do: :ok
  end

  defmodule RelationCustomers do
    use Favn.Namespace, relation: [connection: :warehouse, catalog: "raw", schema: "commerce"]
    use Favn.Asset

    @relation [name: "customers"]
    def asset(_ctx), do: :ok
  end

  defmodule RelationCustomer360 do
    use Favn.Namespace, relation: [connection: :warehouse, catalog: "gold", schema: "commerce"]
    use Favn.SQLAsset

    @materialized :view
    query do
      ~SQL"""
      select o.id, c.id as customer_id
      from raw.commerce.orders o
      join raw.commerce.customers c on c.id = o.customer_id
      """
    end
  end

  setup_all do
    Fixtures.compile_fixture!(:basic_assets)
    :ok
  end

  setup do
    previous_asset_modules = Application.get_env(:favn, :asset_modules)

    on_exit(fn ->
      if is_nil(previous_asset_modules) do
        Application.delete_env(:favn, :asset_modules)
      else
        Application.put_env(:favn, :asset_modules, previous_asset_modules)
      end
    end)

    :ok
  end

  test "list_assets/0 returns configured assets sorted by canonical ref" do
    Application.put_env(:favn, :asset_modules, [SampleAssets, CrossModuleAssets, AdditionalAssets])

    assert {:ok, assets} = Favn.list_assets()

    assert Enum.map(assets, & &1.ref) == [
             {AdditionalAssets, :archive_orders},
             {CrossModuleAssets, :publish_orders},
             {SampleAssets, :extract_orders},
             {SampleAssets, :normalize_orders}
           ]
  end

  test "list_assets/1 keeps deterministic results across overlapping modules" do
    assert {:ok, assets} =
             Favn.list_assets([SampleAssets, CrossModuleAssets, AdditionalAssets, SampleAssets])

    assert Enum.map(assets, & &1.ref) == [
             {AdditionalAssets, :archive_orders},
             {CrossModuleAssets, :publish_orders},
             {SampleAssets, :extract_orders},
             {SampleAssets, :normalize_orders}
           ]
  end

  test "get_asset/1 resolves canonical refs across modules" do
    assert {:ok, asset} = Favn.get_asset({SampleAssets, :normalize_orders})
    assert asset.depends_on == [{SampleAssets, :extract_orders}]

    assert {:ok, cross_module_asset} = Favn.get_asset({CrossModuleAssets, :publish_orders})
    assert cross_module_asset.depends_on == [{SampleAssets, :normalize_orders}]
  end

  test "configured single-module lookups keep inferred relation dependencies" do
    Application.put_env(:favn, :asset_modules, [
      RelationOrders,
      RelationCustomers,
      RelationCustomer360
    ])

    assert {:ok, [asset]} = Favn.list_assets(RelationCustomer360)
    assert asset.depends_on == [{RelationCustomers, :asset}, {RelationOrders, :asset}]

    assert {:ok, fetched} = Favn.get_asset(RelationCustomer360)
    assert fetched.depends_on == [{RelationCustomers, :asset}, {RelationOrders, :asset}]
  end

  test "single-module lookup falls back when omitted and surfaces configured catalog failures when included" do
    module_name =
      Module.concat(__MODULE__, "SingleAssetFallback#{System.unique_integer([:positive])}")

    Code.compile_string(
      """
      defmodule #{inspect(module_name)} do
        use Favn.Asset

        @doc "Single asset fallback"
        def asset(_ctx), do: :ok
      end
      """,
      "test/public_authoring_parity_test.exs"
    )

    Application.put_env(:favn, :asset_modules, [SampleAssets])

    assert {:ok, asset} = Favn.get_asset(module_name)
    assert asset.ref == {module_name, :asset}

    Application.put_env(:favn, :asset_modules, [RelationOrders, SpoofedAssets])

    assert {:error, {SpoofedAssets, {:invalid_asset_module, SpoofedAssets}}} =
             Favn.get_asset(RelationOrders)
  end

  test "asset_module?/1 and get_asset/1 reject spoofed and invalid modules" do
    assert Favn.asset_module?(SampleAssets)
    refute Favn.asset_module?(Enum)
    refute Favn.asset_module?(SpoofedAssets)

    assert {:error, :not_asset_module} = Favn.get_asset({Enum, :map})
    assert {:error, :not_asset_module} = Favn.get_asset({SpoofedAssets, :asset})
    assert {:error, :asset_not_found} = Favn.get_asset({SampleAssets, :missing})
  end

  test "list_assets/0 reports invalid configured modules with module context" do
    Application.put_env(:favn, :asset_modules, [SampleAssets, SpoofedAssets])

    assert {:error, {SpoofedAssets, {:invalid_asset_module, SpoofedAssets}}} = Favn.list_assets()
  end

  test "single-asset module fetch keeps doc and canonical ref" do
    module_name = Module.concat(__MODULE__, "SingleAsset#{System.unique_integer([:positive])}")

    Code.compile_string(
      """
      defmodule #{inspect(module_name)} do
        use Favn.Asset

        @doc "Single asset facade"
        def asset(_ctx), do: :ok
      end
      """,
      "test/public_authoring_parity_test.exs"
    )

    assert {:ok, asset} = Favn.get_asset(module_name)
    assert asset.ref == {module_name, :asset}
    assert asset.doc == "Single asset facade"
  end
end
