defmodule Favn.Assets.PlannerTest do
  use ExUnit.Case

  alias Favn.Test.Fixtures.Assets.Graph.BronzeAssets
  alias Favn.Test.Fixtures.Assets.Graph.GoldAssets
  alias Favn.Test.Fixtures.Assets.Graph.SilverAssets

  setup do
    state = Favn.TestSetup.capture_state()

    :ok =
      Favn.TestSetup.setup_asset_modules([BronzeAssets, SilverAssets, GoldAssets],
        reload_graph?: true
      )

    on_exit(fn ->
      Favn.TestSetup.restore_state(state, reload_graph?: true)
    end)

    :ok
  end

  test "builds multi-target plans with shared dependency dedup and stage grouping" do
    assert {:ok, plan} =
             Favn.plan_asset_run([
               {GoldAssets, :gold_sales},
               {GoldAssets, :gold_finance}
             ])

    assert Map.keys(plan.nodes) |> MapSet.new() ==
             MapSet.new([
               {{BronzeAssets, :raw_customers}, nil},
               {{BronzeAssets, :raw_orders}, nil},
               {{SilverAssets, :monthly_customers}, nil},
               {{SilverAssets, :nightly_orders}, nil},
               {{GoldAssets, :gold_finance}, nil},
               {{GoldAssets, :gold_sales}, nil}
             ])

    assert plan.stages == [
             [{BronzeAssets, :raw_customers}, {BronzeAssets, :raw_orders}],
             [{SilverAssets, :monthly_customers}, {SilverAssets, :nightly_orders}],
             [{GoldAssets, :gold_finance}, {GoldAssets, :gold_sales}]
           ]

    assert plan.target_node_keys == [
             {{GoldAssets, :gold_finance}, nil},
             {{GoldAssets, :gold_sales}, nil}
           ]

    assert plan.nodes[{{SilverAssets, :nightly_orders}, nil}].downstream == [
             {{GoldAssets, :gold_finance}, nil},
             {{GoldAssets, :gold_sales}, nil}
           ]

    assert plan.nodes[{{SilverAssets, :nightly_orders}, nil}].node_key ==
             {{SilverAssets, :nightly_orders}, nil}
  end

  test "supports dependencies: :none for target-only planning" do
    assert {:ok, plan} = Favn.plan_asset_run({GoldAssets, :gold_sales}, dependencies: :none)

    assert plan.topo_order == [{GoldAssets, :gold_sales}]
    assert plan.stages == [[{GoldAssets, :gold_sales}]]
    assert plan.nodes[{{GoldAssets, :gold_sales}, nil}].upstream == []
  end

  test "returns errors for invalid planner input" do
    assert {:error, :empty_targets} = Favn.plan_asset_run([])

    assert {:error, {:invalid_dependencies_mode, :invalid}} =
             Favn.plan_asset_run({GoldAssets, :gold_sales}, dependencies: :invalid)

    assert {:error, :asset_not_found} = Favn.plan_asset_run({GoldAssets, :missing})
  end

  test "normalizes duplicate targets into deterministic sorted order" do
    assert {:ok, plan} =
             Favn.plan_asset_run([
               {GoldAssets, :gold_sales},
               {GoldAssets, :gold_finance},
               {GoldAssets, :gold_sales}
             ])

    assert plan.target_refs == [
             {GoldAssets, :gold_finance},
             {GoldAssets, :gold_sales}
           ]
  end
end
