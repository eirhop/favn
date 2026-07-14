defmodule FavnOrchestrator.Operator.Catalogue.TargetsTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest.Asset
  alias Favn.Manifest.Pipeline
  alias FavnOrchestrator.Operator.Catalogue.Targets

  test "projects stable asset target ids and normalizes nested manifest values" do
    asset = %Asset{
      ref: {MyApp.Assets.Orders, :asset},
      module: MyApp.Assets.Orders,
      name: :asset,
      runtime_config: %{{:provider, :key} => :value}
    }

    target = Targets.asset(asset)

    assert target.target_id == "asset:Elixir.MyApp.Assets.Orders:asset"
    assert target.asset_ref == "Elixir.MyApp.Assets.Orders:asset"
    assert target.runtime_config == %{"{:provider, :key}" => "value"}
  end

  test "malformed persisted window data is returned safely instead of raising" do
    pipeline = %Pipeline{
      module: MyApp.Pipelines.Orders,
      name: :orders,
      selectors: [],
      window: %{unexpected: :value}
    }

    assert Targets.pipeline(pipeline).window == %{"unexpected" => "value"}
  end
end
