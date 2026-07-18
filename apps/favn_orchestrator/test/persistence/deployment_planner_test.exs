defmodule FavnOrchestrator.Persistence.DeploymentPlannerTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.Version
  alias FavnOrchestrator.Persistence.DeploymentPlanner
  alias FavnOrchestrator.Persistence.TargetIdentity

  test "builds a deterministic exact catalog with hidden transitive dependencies" do
    source = asset({MyApp.Source, :source}, [])
    private_stage = asset({MyApp.PrivateStage, :stage}, [source.ref])
    customer_output = asset({MyApp.CustomerOutput, :output}, [private_stage.ref])
    assets = [source, private_stage, customer_output]
    {:ok, graph} = Graph.build(assets)

    manifest = %Manifest{
      assets: assets,
      pipelines: [
        %Pipeline{
          module: MyApp.CustomerPipeline,
          name: :daily,
          selectors: [{:asset, customer_output.ref}]
        }
      ],
      graph: graph
    }

    {:ok, version} = Version.new(manifest, manifest_version_id: "mv-deployment-planner")

    selection = %DeploymentPlanner{
      common_assets: [source.ref],
      common_pipelines: [],
      workspace_assets: [],
      workspace_pipelines: [{MyApp.CustomerPipeline, :daily}]
    }

    assert {:ok, targets} = DeploymentPlanner.plan(version, selection)

    assert targets == Enum.sort_by(targets, &{&1.target_kind, &1.target_id})

    by_id = Map.new(targets, &{&1.target_id, &1})

    assert %{selection_source: :common, customer_visible: true} =
             by_id[TargetIdentity.for_asset(source.ref)]

    assert %{selection_source: :dependency, customer_visible: false} =
             by_id[TargetIdentity.for_asset(private_stage.ref)]

    assert %{selection_source: :dependency, customer_visible: false} =
             by_id[TargetIdentity.for_asset(customer_output.ref)]

    assert %{selection_source: :explicit, customer_visible: true} =
             by_id[TargetIdentity.for_pipeline({MyApp.CustomerPipeline, :daily})]

    assert Enum.all?(targets, &is_binary(&1.descriptor["target_id"]))
    assert Enum.all?(targets, &is_binary(&1.descriptor["label"]))
  end

  test "rejects ambiguous common and workspace-specific selections" do
    ref = {MyApp.Source, :source}
    asset = asset(ref, [])
    {:ok, graph} = Graph.build([asset])
    {:ok, version} = Version.new(%Manifest{assets: [asset], graph: graph})

    selection = %DeploymentPlanner{
      common_assets: [ref],
      common_pipelines: [],
      workspace_assets: [ref],
      workspace_pipelines: []
    }

    assert {:error, {:ambiguous_target_selection, :asset, [^ref]}} =
             DeploymentPlanner.plan(version, selection)
  end

  defp asset(ref, dependencies) do
    %Asset{
      ref: ref,
      module: elem(ref, 0),
      name: elem(ref, 1),
      type: :source,
      depends_on: dependencies
    }
  end
end
