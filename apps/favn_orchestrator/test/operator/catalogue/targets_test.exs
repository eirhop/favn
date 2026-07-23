defmodule FavnOrchestrator.Operator.Catalogue.TargetsTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest.Asset
  alias Favn.Manifest.Pipeline
  alias Favn.RuntimeConfig.Ref, as: RuntimeConfigRef
  alias Favn.SQL.PartitionSpec
  alias FavnOrchestrator.Operator.Catalogue.Targets

  test "projects stable asset target ids and normalizes nested manifest values" do
    asset = %Asset{
      ref: {MyApp.Assets.Orders, :asset},
      module: MyApp.Assets.Orders,
      name: :asset,
      partition_spec: PartitionSpec.normalize!([:tenant_id, {:month, :occurred_at}]),
      runtime_config: %{{:provider, :key} => :value}
    }

    target = Targets.asset(asset)

    assert target.target_id == "asset:Elixir.MyApp.Assets.Orders:asset"
    assert target.asset_ref == "Elixir.MyApp.Assets.Orders:asset"

    assert target.partition_spec == %{
             "keys" => [
               %{"bucket_count" => nil, "column" => "tenant_id", "transform" => "identity"},
               %{"bucket_count" => nil, "column" => "occurred_at", "transform" => "month"}
             ]
           }

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

  test "descriptor persistence preserves boolean types recursively" do
    asset = %Asset{
      ref: {MyApp.Assets.Orders, :asset},
      module: MyApp.Assets.Orders,
      name: :asset,
      runtime_config: %{
        warehouse: %{
          password: RuntimeConfigRef.secret_env!("WAREHOUSE_PASSWORD", required?: false)
        }
      }
    }

    descriptor =
      Map.merge(Targets.asset(asset), %{
        can_run_without_window?: false,
        can_backfill?: true,
        window: %{allow_full_load: false, required: true},
        metadata: %{literal: "true"}
      })

    serialized = Targets.serialize_descriptor(descriptor)
    restored = Targets.restore_descriptor(serialized)

    assert serialized["can_run_without_window?"] == false
    assert serialized["can_backfill?"] == true
    assert serialized["window"] == %{"allow_full_load" => false, "required" => true}
    assert serialized["runtime_config"]["warehouse"]["password"]["secret"] == true
    assert restored.can_run_without_window? == false
    assert restored.can_backfill? == true
    assert restored.window == %{"allow_full_load" => false, "required" => true}
    assert restored.metadata == %{"literal" => "true"}
  end
end
