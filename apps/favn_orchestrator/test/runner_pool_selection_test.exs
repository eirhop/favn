defmodule FavnOrchestrator.RunnerPoolSelectionTest do
  use ExUnit.Case, async: true

  alias Favn.Plan
  alias FavnOrchestrator.RunnerPoolSelection
  alias FavnOrchestrator.RunState

  @release_default FavnTestSupport.runner_release_id()
  @release_gpu FavnTestSupport.runner_release_id(:alternate)
  @node {{MyApp.Asset, :asset}, nil}

  test "asset override wins over pipeline and final defaults" do
    run = run(:gpu, :duckdb)
    assert RunnerPoolSelection.for_node(run, @node) == :gpu
    assert RunnerPoolSelection.release_for_node!(run, @node) == @release_gpu
  end

  test "pipeline default wins when the asset has no override" do
    run = run(nil, :default)
    assert RunnerPoolSelection.for_node(run, @node) == :default
    assert RunnerPoolSelection.release_for_node!(run, @node) == @release_default
  end

  test "missing declarations select the named default pool" do
    run = run(nil, nil)
    assert RunnerPoolSelection.for_node(run, @node) == :default
  end

  defp run(node_pool, pipeline_pool) do
    node = %{
      ref: {MyApp.Asset, :asset},
      node_key: @node,
      runner_pool: node_pool,
      upstream: [],
      downstream: [],
      stage: 0
    }

    RunState.new(
      id: "run-pool-selection",
      manifest_version_id: "mv-pool-selection",
      manifest_content_hash: String.duplicate("a", 64),
      runner_releases: %{
        "default" => @release_default,
        "duckdb" => @release_default,
        "gpu" => @release_gpu
      },
      asset_ref: {MyApp.Asset, :asset},
      plan: %Plan{
        target_refs: [{MyApp.Asset, :asset}],
        target_node_keys: [@node],
        nodes: %{@node => node},
        topo_order: [{MyApp.Asset, :asset}],
        stages: [[{MyApp.Asset, :asset}]],
        node_stages: [[@node]]
      },
      metadata: %{pipeline_context: %{runner_pool: pipeline_pool}}
    )
  end
end
