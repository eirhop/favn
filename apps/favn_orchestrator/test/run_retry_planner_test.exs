defmodule FavnOrchestrator.RunRetryPlannerTest do
  use ExUnit.Case, async: false

  alias Favn.Run.NodeResult
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage
  alias FavnOrchestrator.Storage.Adapter.Memory

  setup do
    Memory.reset()

    on_exit(fn -> Memory.reset() end)

    :ok
  end

  test "remaining retry plan excludes successful nodes and keeps failed plus unstarted nodes" do
    refs = [
      {MyApp.Assets.Raw, :succeeded},
      {MyApp.Assets.Raw, :failed},
      {MyApp.Assets.Raw, :not_started}
    ]

    node_keys = Enum.map(refs, &{&1, nil})
    now = DateTime.utc_now()

    run =
      RunState.new(
        id: "retry_remaining_source",
        manifest_version_id: "mv_retry_remaining",
        manifest_content_hash: "hash_retry_remaining",
        asset_ref: List.first(refs),
        target_refs: refs,
        plan: flat_plan(refs),
        submit_kind: :pipeline,
        metadata: %{pipeline_target_refs: refs, pipeline_dependencies: :none}
      )
      |> RunState.transition(
        status: :error,
        error: %{type: :test_failure},
        result: %{
          node_results: [
            NodeResult.new(%{
              node_key: Enum.at(node_keys, 0),
              ref: Enum.at(refs, 0),
              stage: 0,
              status: :ok,
              started_at: now,
              finished_at: now
            }),
            NodeResult.new(%{
              node_key: Enum.at(node_keys, 1),
              ref: Enum.at(refs, 1),
              stage: 0,
              status: :error,
              started_at: now,
              finished_at: now,
              error: %{type: :test_failure}
            })
          ]
        }
      )

    assert :ok = Storage.put_run(run)

    assert {:ok, plan} = FavnOrchestrator.plan_remaining_retry(run.id)

    assert plan.asset_count == 2

    assert [%{source_run_id: "retry_remaining_source", target_refs: target_refs, node_keys: keys}] =
             plan.children

    assert target_refs == Enum.slice(refs, 1, 2)
    assert keys == Enum.slice(node_keys, 1, 2)
  end

  defp flat_plan(refs) do
    node_keys = Enum.map(refs, &{&1, nil})

    nodes =
      refs
      |> Enum.zip(node_keys)
      |> Map.new(fn {ref, node_key} ->
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

    %Favn.Plan{
      target_refs: refs,
      target_node_keys: node_keys,
      nodes: nodes,
      topo_order: refs,
      stages: [refs],
      node_stages: [node_keys]
    }
  end
end
