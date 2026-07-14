defmodule FavnOrchestrator.RunReadModel.StepProjectionTest do
  use ExUnit.Case, async: true

  alias Favn.Plan
  alias FavnOrchestrator.RunReadModel.StepProjection
  alias FavnOrchestrator.RunState

  test "applies a string-keyed pipeline default pool without rescanning the plan" do
    ref = {MyApp.Assets.Gold, :asset}
    node_key = {ref, nil}

    run = %RunState{
      id: "pool-projection",
      submit_kind: :pipeline,
      status: :running,
      target_refs: [ref],
      metadata: %{"pipeline_execution_policy" => %{"execution_pool" => "warehouse"}},
      plan: plan(%{node_key => node(ref, node_key)})
    }

    assert [%{asset_ref: "MyApp.Assets.Gold.asset", execution_pool: "warehouse"}] =
             StepProjection.build(run, [])
  end

  test "recognizes an incomplete string-keyed pipeline rerun" do
    gold = {MyApp.Assets.Gold, :asset}
    silver = {MyApp.Assets.Silver, :asset}
    gold_key = {gold, nil}
    silver_key = {silver, nil}

    run = %RunState{
      id: "rerun-projection",
      submit_kind: :rerun,
      status: :ok,
      target_refs: [gold, silver],
      metadata: %{"replay_submit_kind" => "pipeline"},
      plan: plan(%{gold_key => node(gold, gold_key), silver_key => node(silver, silver_key)}),
      result: %{
        "node_results" => [
          %{"node_key" => gold_key, "ref" => gold, "status" => "ok", "asset_step_id" => "gold"}
        ]
      }
    }

    assert StepProjection.incomplete?(run)

    assert %{unit: :steps, total: 2, completed: 1, empty?: false} =
             StepProjection.progress(run)
  end

  defp plan(nodes) do
    node_keys = Map.keys(nodes)
    refs = Enum.map(node_keys, &elem(&1, 0))

    %Plan{
      target_refs: refs,
      target_node_keys: node_keys,
      nodes: nodes,
      topo_order: refs,
      stages: [refs],
      node_stages: [node_keys]
    }
  end

  defp node(ref, node_key) do
    %{
      ref: ref,
      node_key: node_key,
      window: nil,
      upstream: [],
      downstream: [],
      stage: 0,
      action: :run
    }
  end
end
