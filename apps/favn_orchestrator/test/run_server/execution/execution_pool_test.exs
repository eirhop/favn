defmodule FavnOrchestrator.RunServer.Execution.ExecutionPoolTest do
  use ExUnit.Case, async: true

  alias Favn.Plan
  alias FavnOrchestrator.RunServer.Execution.ExecutionPool
  alias FavnOrchestrator.RunState

  @ref {__MODULE__.Asset, :asset}
  @node_key {@ref, nil}

  test "node pool overrides a string-keyed persisted pipeline default" do
    run = run_state(%{@node_key => %{"execution_pool" => :node_pool}})

    assert ExecutionPool.for_node(run, @node_key) == :node_pool
  end

  test "falls back to a string-keyed persisted pipeline default" do
    run = run_state(%{@node_key => %{}})

    assert ExecutionPool.for_node(run, @node_key) == "pipeline_pool"
  end

  test "returns nil for missing plan and policy data" do
    assert ExecutionPool.for_node(%RunState{}, @node_key) == nil
  end

  defp run_state(nodes) do
    %RunState{
      plan: %Plan{nodes: nodes},
      metadata: %{"pipeline_execution_policy" => %{"execution_pool" => "pipeline_pool"}}
    }
  end
end
