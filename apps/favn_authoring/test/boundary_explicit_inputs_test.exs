defmodule FavnAuthoring.BoundaryExplicitInputsTest do
  use ExUnit.Case, async: true

  alias Favn.Assets.Planner

  defmodule RawAsset do
    use Favn.Asset

    def asset(_ctx), do: :ok
  end

  defmodule GoldAsset do
    use Favn.Asset

    @depends RawAsset
    def asset(_ctx), do: :ok
  end

  test "planner builds from explicit authoring asset modules input" do
    assert {:ok, plan} =
             Planner.plan({GoldAsset, :asset},
               asset_modules: [RawAsset, GoldAsset],
               dependencies: :all
             )

    assert plan.target_refs == [{GoldAsset, :asset}]
    assert plan.topo_order == [{RawAsset, :asset}, {GoldAsset, :asset}]
  end
end
