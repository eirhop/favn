defmodule FavnOrchestrator.RunServer.Execution.CompactExecutionIndexTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest.Asset
  alias Favn.Manifest.Index
  alias Favn.Plan
  alias Favn.RelationRef
  alias Favn.TargetIdentity
  alias FavnOrchestrator.RunServer.Execution
  alias FavnOrchestrator.RunState

  test "retains pinned upstream generation assets without adding executable nodes" do
    upstream = persisted_asset(MyApp.Upstream)
    target = persisted_asset(MyApp.Target)
    observer = %Asset{ref: {MyApp.Observer, :asset}, module: MyApp.Observer, name: :asset}

    target_node = %{
      ref: target.ref,
      input_generations: [
        %{
          target_id: upstream.target_descriptor.target_id,
          target_generation_id: "generation-upstream"
        }
      ]
    }

    run =
      RunState.new(
        id: "run-compact-index",
        manifest_version_id: "manifest-compact-index",
        manifest_content_hash: String.duplicate("b", 64),
        required_runner_release_id: FavnTestSupport.runner_release_id(),
        asset_ref: target.ref,
        plan: %Plan{
          nodes: %{{target.ref, nil} => target_node},
          topo_order: [target.ref],
          target_refs: [target.ref],
          target_node_keys: [{target.ref, nil}]
        }
      )

    manifest_index = %Index{
      assets_by_ref: Map.new([upstream, target, observer], &{&1.ref, &1})
    }

    compact = Execution.compact_execution_index(run, manifest_index)

    assert Map.keys(compact.assets_by_ref) |> MapSet.new() ==
             MapSet.new([upstream.ref, target.ref])

    assert Map.keys(run.plan.nodes) == [{target.ref, nil}]
  end

  test "retains a pinned non-persisted upstream asset by its semantic target identity" do
    upstream = %Asset{
      ref: {MyApp.Landing, :snapshot},
      module: MyApp.Landing,
      name: :snapshot,
      type: :elixir,
      semantic_generation_id: "ag_landing_snapshot"
    }

    target = persisted_asset(MyApp.Target)

    target_node = %{
      ref: target.ref,
      input_generations: [
        %{
          target_id: TargetIdentity.for_asset(upstream.ref),
          target_generation_id: nil,
          evidence_generation_id: upstream.semantic_generation_id,
          physical_relation: nil
        }
      ]
    }

    run =
      RunState.new(
        id: "run-compact-semantic-index",
        manifest_version_id: "manifest-compact-semantic-index",
        manifest_content_hash: String.duplicate("b", 64),
        required_runner_release_id: FavnTestSupport.runner_release_id(),
        asset_ref: target.ref,
        plan: %Plan{
          nodes: %{{target.ref, nil} => target_node},
          topo_order: [target.ref],
          target_refs: [target.ref],
          target_node_keys: [{target.ref, nil}]
        }
      )

    manifest_index = %Index{assets_by_ref: Map.new([upstream, target], &{&1.ref, &1})}
    compact = Execution.compact_execution_index(run, manifest_index)

    assert Map.keys(compact.assets_by_ref) |> MapSet.new() ==
             MapSet.new([upstream.ref, target.ref])
  end

  defp persisted_asset(module) do
    FavnTestSupport.with_target_descriptor(%Asset{
      ref: {module, :asset},
      module: module,
      name: :asset,
      type: :sql,
      relation: RelationRef.new!(connection: :warehouse, schema: "main", name: "target"),
      materialization: :table,
      execution_package_hash: String.duplicate("a", 64)
    })
  end
end
