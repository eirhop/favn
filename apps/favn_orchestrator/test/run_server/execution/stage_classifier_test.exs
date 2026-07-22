defmodule FavnOrchestrator.RunServer.Execution.StageClassifierTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest.Version
  alias Favn.Plan
  alias FavnOrchestrator.RefreshPolicy
  alias FavnOrchestrator.RunServer.Execution.StageClassifier
  alias FavnOrchestrator.RunState

  test "classification yields after a bounded node batch" do
    node_keys = Enum.map(1..10, &{{__MODULE__, String.to_atom("asset_#{&1}")}, nil})

    nodes =
      Map.new(node_keys, fn {ref, nil} = node_key ->
        {node_key,
         %{
           ref: ref,
           node_key: node_key,
           window: nil,
           upstream: [],
           downstream: [],
           stage: 0,
           action: :run
         }}
      end)

    plan = %Plan{
      target_refs: Enum.map(node_keys, &elem(&1, 0)),
      target_node_keys: node_keys,
      nodes: nodes,
      topo_order: Enum.map(node_keys, &elem(&1, 0)),
      stages: [Enum.map(node_keys, &elem(&1, 0))],
      node_stages: [node_keys]
    }

    run =
      RunState.new(
        id: "bounded-classification",
        manifest_version_id: "mv_stage_classifier",
        manifest_content_hash: "hash",
        required_runner_release_id: FavnTestSupport.runner_release_id(),
        plan: plan,
        asset_ref: hd(plan.target_refs)
      )

    policy = RefreshPolicy.from_value!(:force)

    context = %{
      assets_by_ref: %{},
      refresh_policy: policy,
      forced_node_keys: RefreshPolicy.expand_force_set(policy, plan),
      prior_states: %{},
      current_states: %{},
      completed_node_keys: MapSet.new(),
      refreshed_node_keys: MapSet.new(),
      upstream_statuses: %{},
      now: DateTime.utc_now()
    }

    version = %Version{manifest_version_id: "mv_stage_classifier", content_hash: "hash"}

    assert {:ok, ^run, runnable, decisions, ^context, nil, remaining} =
             StageClassifier.classify(run, version, 0, node_keys, context, nil)

    assert length(runnable) == 4
    assert length(remaining) == 6
    assert map_size(decisions) == 4
    assert Enum.all?(decisions, fn {_node_key, decision} -> decision.reason == :forced end)
  end
end
