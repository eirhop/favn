defmodule FavnAuthoringTest do
  use ExUnit.Case, async: false

  defmodule ConfiguredAsset do
    use Favn.Asset

    def asset(_ctx), do: :ok
  end

  defmodule DirectAsset do
    use Favn.Asset

    def asset(_ctx), do: :ok
  end

  defmodule DiscoveredSourceOrders do
    use Favn.Namespace,
      relation: [connection: :warehouse, catalog: "raw", schema: "commerce"]

    use Favn.Asset

    relation(name: "orders")

    def asset(_ctx), do: :ok
  end

  defmodule DiscoveredSourceCustomers do
    use Favn.Namespace,
      relation: [connection: :warehouse, catalog: "raw", schema: "commerce"]

    use Favn.Asset

    relation(name: "customers")

    def asset(_ctx), do: :ok
  end

  defmodule DiscoveredCustomer360 do
    use Favn.Namespace,
      relation: [connection: :warehouse, catalog: "gold", schema: "commerce"]

    use Favn.SQLAsset

    materialized(:view)

    query do
      ~SQL"select o.id, c.id as customer_id from raw.commerce.orders o
            join raw.commerce.customers c on c.id = o.customer_id"
    end
  end

  setup do
    previous_assets = Application.get_env(:favn, :asset_modules)
    previous_discovery = Application.get_env(:favn, :discovery)

    on_exit(fn ->
      restore_env(:asset_modules, previous_assets)
      restore_env(:discovery, previous_discovery)
    end)

    :ok
  end

  test "list_assets/0 uses configured asset modules" do
    Application.put_env(:favn, :asset_modules, [ConfiguredAsset])

    assert {:ok, [%Favn.Asset{ref: {ConfiguredAsset, :asset}}]} = FavnAuthoring.list_assets()
  end

  test "list_assets/1 compiles an omitted module directly" do
    Application.put_env(:favn, :asset_modules, [ConfiguredAsset])

    assert {:ok, [%Favn.Asset{ref: {DirectAsset, :asset}}]} =
             FavnAuthoring.list_assets(DirectAsset)
  end

  test "list_assets/1 uses discovered catalog dependency inference" do
    app =
      load_test_app!([
        DiscoveredSourceOrders,
        DiscoveredSourceCustomers,
        DiscoveredCustomer360
      ])

    Application.put_env(:favn, :discovery, apps: [app], assets: :all)
    Application.delete_env(:favn, :asset_modules)

    assert {
             :ok,
             [
               %Favn.Asset{
                 ref: {DiscoveredCustomer360, :asset},
                 depends_on: depends_on
               }
             ]
           } = FavnAuthoring.list_assets(DiscoveredCustomer360)

    assert Enum.sort(depends_on) ==
             Enum.sort([{DiscoveredSourceOrders, :asset}, {DiscoveredSourceCustomers, :asset}])
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn, key)
  defp restore_env(key, value), do: Application.put_env(:favn, key, value)

  defp load_test_app!(modules) do
    app = String.to_atom("favn_discovery_list_test_#{System.unique_integer([:positive])}")

    :ok =
      :application.load(
        {:application, app,
         [
           description: ~c"FavnAuthoring list assets test app",
           vsn: ~c"1",
           modules: modules,
           registered: [],
           applications: [:kernel, :stdlib]
         ]}
      )

    on_exit(fn ->
      :application.unload(app)
    end)

    app
  end
end
