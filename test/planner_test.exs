defmodule Flux.PlannerTest do
  use ExUnit.Case

  defmodule BronzeAssets do
    use Flux.Assets

    @asset true
    def raw_orders, do: :ok

    @asset true
    def raw_customers, do: :ok
  end

  defmodule SilverAssets do
    use Flux.Assets

    alias Flux.PlannerTest.BronzeAssets

    @asset depends_on: [{BronzeAssets, :raw_orders}]
    def nightly_orders(_orders), do: :ok

    @asset depends_on: [{BronzeAssets, :raw_customers}]
    def monthly_customers(_customers), do: :ok
  end

  defmodule GoldAssets do
    use Flux.Assets

    alias Flux.PlannerTest.SilverAssets

    @asset depends_on: [{SilverAssets, :nightly_orders}, {SilverAssets, :monthly_customers}]
    def gold_sales(_orders, _customers), do: :ok

    @asset depends_on: [{SilverAssets, :nightly_orders}]
    def gold_finance(_orders), do: :ok
  end

  setup do
    previous_modules = Application.get_env(:flux, :asset_modules)
    previous_catalog = Flux.Registry.build_catalog(previous_modules || [])

    Application.put_env(:flux, :asset_modules, [BronzeAssets, SilverAssets, GoldAssets])
    assert :ok = Flux.Registry.reload()
    assert :ok = Flux.GraphIndex.reload()

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

  test "builds multi-target plans with shared dependency dedup and stage grouping" do
    assert {:ok, plan} =
             Flux.plan_run([
               {GoldAssets, :gold_sales},
               {GoldAssets, :gold_finance}
             ])

    assert Map.keys(plan.nodes) |> MapSet.new() ==
             MapSet.new([
               {BronzeAssets, :raw_customers},
               {BronzeAssets, :raw_orders},
               {SilverAssets, :monthly_customers},
               {SilverAssets, :nightly_orders},
               {GoldAssets, :gold_finance},
               {GoldAssets, :gold_sales}
             ])

    assert plan.stages == [
             [{BronzeAssets, :raw_customers}, {BronzeAssets, :raw_orders}],
             [{SilverAssets, :monthly_customers}, {SilverAssets, :nightly_orders}],
             [{GoldAssets, :gold_finance}, {GoldAssets, :gold_sales}]
           ]

    assert plan.nodes[{SilverAssets, :nightly_orders}].downstream == [
             {GoldAssets, :gold_finance},
             {GoldAssets, :gold_sales}
           ]
  end

  test "supports dependencies: :none for target-only planning" do
    assert {:ok, plan} = Flux.plan_run({GoldAssets, :gold_sales}, dependencies: :none)

    assert plan.topo_order == [{GoldAssets, :gold_sales}]
    assert plan.stages == [[{GoldAssets, :gold_sales}]]
    assert plan.nodes[{GoldAssets, :gold_sales}].upstream == []
  end

  test "returns errors for invalid planner input" do
    assert {:error, :empty_targets} = Flux.plan_run([])

    assert {:error, {:invalid_dependencies_mode, :invalid}} =
             Flux.plan_run({GoldAssets, :gold_sales}, dependencies: :invalid)

    assert {:error, {:asset_not_found, {GoldAssets, :missing}}} =
             Flux.plan_run({GoldAssets, :missing})
  end

  defp restore_registry({:ok, _catalog}) do
    :ok = Flux.Registry.reload()
    :ok = Flux.GraphIndex.reload()
  end

  defp restore_registry({:error, _reason}), do: :ok
end
