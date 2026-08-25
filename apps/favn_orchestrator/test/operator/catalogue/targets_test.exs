defmodule FavnOrchestrator.Operator.Catalogue.TargetsTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest.Asset
  alias Favn.Manifest.Pipeline
  alias Favn.RuntimeConfig.Ref, as: RuntimeConfigRef
  alias Favn.SQL.PartitionSpec
  alias Favn.Window.Policy
  alias FavnOrchestrator.Operator.Catalogue.Targets

  doctest Targets

  test "names a single-asset module after the module, not after its :asset ref" do
    asset = %Asset{
      ref: {MyApp.Lifecycle.RetryProbe, :asset},
      module: MyApp.Lifecycle.RetryProbe,
      name: :asset
    }

    target = Targets.asset(asset)

    # `use Favn.Asset` always produces the ref name `:asset`, so without this
    # every plain Elixir asset in a project reads "asset" in the catalogue.
    assert target.name == "retry_probe"
    assert target.module_path == ["lifecycle"]
  end

  test "a module with no namespace below its root still resolves a name" do
    asset = %Asset{ref: {Orders, :asset}, module: Orders, name: :asset}

    target = Targets.asset(asset)

    assert target.name == "orders"
    assert target.module_path == []
  end

  test "an Erlang-style module atom resolves without raising" do
    asset = %Asset{ref: {:crm_probe, :asset}, module: :crm_probe, name: :asset}

    target = Targets.asset(asset)

    assert target.name == "crm_probe"
    assert target.module_path == []
  end

  test "prefers an owned relation's name over the module's" do
    asset = %Asset{
      ref: {MyApp.Warehouse.Source.Crm.Events.Activity, :asset},
      module: MyApp.Warehouse.Source.Crm.Events.Activity,
      name: :asset,
      relation: %Favn.RelationRef{
        connection: :warehouse,
        catalog: "source",
        schema: "crm",
        name: "activity"
      }
    }

    target = Targets.asset(asset)

    assert target.name == "activity"

    assert target.relation == %{
             connection: "warehouse",
             catalog: "source",
             schema: "crm",
             name: "activity"
           }
  end

  test "a multi-asset module keeps its ref name and its leaf in the module path" do
    asset = %Asset{
      ref: {MyApp.Landing.Crm.Daily, :activities},
      module: MyApp.Landing.Crm.Daily,
      name: :activities
    }

    target = Targets.asset(asset)

    assert target.name == "activities"
    assert target.module_path == ["landing", "crm", "daily"]
  end

  test "the derived name and module path survive descriptor persistence" do
    asset = %Asset{
      ref: {MyApp.Lifecycle.ElasticScaleProbe.Fast, :asset},
      module: MyApp.Lifecycle.ElasticScaleProbe.Fast,
      name: :asset
    }

    restored =
      asset |> Targets.asset() |> Targets.serialize_descriptor() |> Targets.restore_descriptor()

    assert restored.name == "fast"
    assert restored.module_path == ["lifecycle", "elastic_scale_probe"]
  end

  test "a descriptor persisted before these fields existed still resolves both" do
    # The name and the module path are derived on read, not stored, so every
    # deployment already active in PostgreSQL gets them without republishing.
    # Persisting them instead would have left those workspaces reading "asset".
    restored =
      Targets.restore_descriptor(%{
        "target_id" => "at_1",
        "label" => "{MyApp.Lifecycle.RetryProbe, :asset}",
        "asset_ref" => "Elixir.MyApp.Lifecycle.RetryProbe:asset",
        "relation" => nil
      })

    assert restored.name == "retry_probe"
    assert restored.module_path == ["lifecycle"]
  end

  test "a restored descriptor prefers its relation's name over the module's" do
    # JSONB decodes the relation with string keys, which is the only shape the
    # read path ever sees.
    restored =
      Targets.restore_descriptor(%{
        "target_id" => "at_2",
        "label" => "{MyApp.Warehouse.Source.Crm.Events.Activity, :asset}",
        "asset_ref" => "Elixir.MyApp.Warehouse.Source.Crm.Events.Activity:asset",
        "relation" => %{
          "connection" => "warehouse",
          "catalog" => "source",
          "schema" => "crm",
          "name" => "activity"
        }
      })

    assert restored.name == "activity"
    assert restored.module_path == ["warehouse", "source", "crm", "events"]
  end

  test "a pipeline descriptor gains no asset naming keys" do
    restored =
      Targets.restore_descriptor(%{"target_id" => "pt_1", "label" => "MyApp.Pipelines.Daily"})

    refute Map.has_key?(restored, :name)
    refute Map.has_key?(restored, :module_path)
  end

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

  test "pipeline catalogue exposes the authored combine default" do
    pipeline = %Pipeline{
      module: MyApp.Pipelines.Orders,
      name: :orders,
      selectors: [],
      window: Policy.new!(:daily, combine_windows: true)
    }

    assert Targets.pipeline(pipeline).window.combine_windows
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
