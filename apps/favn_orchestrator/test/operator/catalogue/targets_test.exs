defmodule FavnOrchestrator.Operator.Catalogue.TargetsTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest.Asset
  alias Favn.Manifest.Pipeline
  alias Favn.Window.Policy
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

  test "descriptor serialization preserves booleans at every nesting level" do
    descriptor = %{
      can_run_without_window?: false,
      can_backfill?: true,
      window: %{allow_full_load: false, nested: [%{enabled: true}]},
      label: "false"
    }

    serialized = Targets.serialize_descriptor(descriptor)
    restored = Targets.restore_descriptor(serialized)

    assert serialized == %{
             "can_run_without_window?" => false,
             "can_backfill?" => true,
             "window" => %{
               "allow_full_load" => false,
               "nested" => [%{"enabled" => true}]
             },
             "label" => "false"
           }

    assert restored.can_run_without_window? == false
    assert restored.can_backfill? == true
    assert restored.window["allow_full_load"] == false
    assert restored.window["nested"] == [%{"enabled" => true}]
    assert restored.label == "false"
  end

  test "required-window pipeline capability booleans survive persistence" do
    pipeline = %Pipeline{
      module: MyApp.Pipelines.Orders,
      name: :orders,
      selectors: [],
      window: Policy.new!(:monthly)
    }

    restored =
      pipeline
      |> Targets.pipeline()
      |> Targets.serialize_descriptor()
      |> Targets.restore_descriptor()

    assert restored.can_run_without_window? == false
    assert restored.can_backfill? == true
    assert restored.window["allow_full_load"] == false
  end

  test "descriptor restoration repairs only known legacy boolean fields" do
    restored =
      Targets.restore_descriptor(%{
        "can_run_without_window?" => "false",
        "can_backfill?" => "true",
        "window" => %{
          "allow_full_load" => "false",
          "required" => "true",
          "label" => "true"
        },
        "runtime_config" => %{
          "required" => "false",
          "refs" => [
            %{
              "provider" => "env",
              "key" => "TOKEN",
              "secret" => "true",
              "required" => "false"
            }
          ]
        },
        "label" => "false",
        "metadata" => %{"enabled" => "true"}
      })

    assert restored.can_run_without_window? == false
    assert restored.can_backfill? == true

    assert restored.window == %{
             "allow_full_load" => false,
             "required" => true,
             "label" => "true"
           }

    assert restored.runtime_config == %{
             "required" => "false",
             "refs" => [
               %{
                 "provider" => "env",
                 "key" => "TOKEN",
                 "secret" => true,
                 "required" => false
               }
             ]
           }

    assert restored.label == "false"
    assert restored.metadata == %{"enabled" => "true"}
  end
end
