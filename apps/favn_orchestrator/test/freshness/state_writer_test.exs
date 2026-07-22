defmodule FavnOrchestrator.Freshness.StateWriterTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest.Version
  alias Favn.Plan
  alias FavnOrchestrator.Freshness.StateWriter
  alias FavnOrchestrator.RunState

  test "carries the plan's pinned evidence generation into success state" do
    ref = {MyApp.Orders, :orders}
    node_key = {ref, nil}

    plan = %Plan{
      target_refs: [ref],
      target_node_keys: [node_key],
      nodes: %{
        node_key => %{
          ref: ref,
          node_key: node_key,
          window: nil,
          upstream: [],
          downstream: [],
          stage: 0,
          evidence_generation_id: "ag_orders"
        }
      },
      topo_order: [ref],
      stages: [[ref]],
      node_stages: [[node_key]]
    }

    version = %Version{
      manifest_version_id: "manifest-orders",
      content_hash: String.duplicate("a", 64)
    }

    run =
      RunState.new(
        id: "run-orders",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        required_runner_release_id: FavnTestSupport.runner_release_id(),
        asset_ref: ref,
        target_refs: [ref],
        plan: plan
      )

    state =
      StateWriter.build_success_state(
        run,
        version,
        node_key,
        %{freshness_key: "latest", reason: :missing},
        %{current_states: %{}}
      )

    assert state.evidence_generation_id == "ag_orders"
  end
end
