defmodule Favn.Assets.PlannerTest do
  use ExUnit.Case

  alias Favn.Test.Fixtures.Assets.Graph.BronzeAssets
  alias Favn.Test.Fixtures.Assets.Graph.GoldAssets
  alias Favn.Test.Fixtures.Assets.Graph.SilverAssets

  defmodule WindowPlannerFixtures do
    defmodule Bronze do
      use Favn.Assets

      @asset true
      @window Favn.Window.hourly(lookback: 1)
      def hourly_orders(_ctx), do: :ok
    end

    defmodule Gold do
      use Favn.Assets

      @asset true
      @window Favn.Window.daily()
      @depends {Bronze, :hourly_orders}
      def daily_orders(_ctx), do: :ok
    end
  end

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

    assert plan.node_stages == [
             [{{BronzeAssets, :raw_customers}, nil}, {{BronzeAssets, :raw_orders}, nil}],
             [{{SilverAssets, :monthly_customers}, nil}, {{SilverAssets, :nightly_orders}, nil}],
             [{{GoldAssets, :gold_finance}, nil}, {{GoldAssets, :gold_sales}, nil}]
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
    assert plan.node_stages == [[{{GoldAssets, :gold_sales}, nil}]]
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

  test "expands windowed node keys using anchor window and lookback" do
    state = Favn.TestSetup.capture_state()

    :ok =
      Favn.TestSetup.setup_asset_modules(
        [WindowPlannerFixtures.Bronze, WindowPlannerFixtures.Gold],
        reload_graph?: true
      )

    on_exit(fn ->
      Favn.TestSetup.restore_state(state, reload_graph?: true)
    end)

    anchor =
      Favn.Window.anchor(
        :day,
        DateTime.from_naive!(~N[2025-01-10 00:00:00], "Etc/UTC"),
        DateTime.from_naive!(~N[2025-01-11 00:00:00], "Etc/UTC")
      )

    assert {:ok, plan} =
             Favn.plan_asset_run({WindowPlannerFixtures.Gold, :daily_orders},
               anchor_window: anchor
             )

    assert length(plan.target_node_keys) == 1
    [target_key] = plan.target_node_keys

    assert match?(
             {{WindowPlannerFixtures.Gold, :daily_orders}, %{kind: :day, timezone: "Etc/UTC"}},
             target_key
           )

    assert map_size(plan.nodes) == 26
    assert length(plan.nodes[target_key].upstream) == 24
  end
end
