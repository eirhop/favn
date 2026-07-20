defmodule FavnOrchestrator.Freshness.StateLoaderTest do
  use ExUnit.Case, async: true

  alias Favn.Plan
  alias FavnOrchestrator.AssetStepIdentity
  alias FavnOrchestrator.Freshness.StateLoader
  alias FavnOrchestrator.Persistence.Results.FreshnessState

  @now ~U[2026-07-20 10:00:00Z]
  @upstream_ref {MyApp.Raw, :orders}
  @target_ref {MyApp.Gold, :orders}
  @upstream_key {@upstream_ref, "window:upstream"}
  @target_key {@target_ref, "window:target"}

  test "restores planned node identities and consumed upstream versions" do
    plan = plan()
    target_id = "asset:gold"
    freshness_key = "window:target"

    persisted = %FreshnessState{
      workspace_id: "workspace",
      deployment_id: "deployment",
      target_id: target_id,
      freshness_key: freshness_key,
      status: :fresh,
      payload: %{
        "freshness_version" => "gold-v2",
        "run_id" => "run-2",
        "node_key_fingerprint" => AssetStepIdentity.node_fingerprint(@target_key),
        "input_fingerprint" => "inputs-v2",
        "input_versions" => [
          %{
            "node_key_fingerprint" => AssetStepIdentity.node_fingerprint(@upstream_key),
            "freshness_version" => "raw-v4",
            "success_run_id" => "run-1"
          }
        ]
      },
      updated_at: @now
    }

    requested = %{{target_id, freshness_key} => {MyApp.Gold, :orders, freshness_key}}

    assert {:ok, [state]} = StateLoader.decode_many([persisted], requested, plan)
    assert state.latest_success_node_key == @target_key
    assert state.freshness_version == "gold-v2"

    assert [
             %{
               upstream_node_key: @upstream_key,
               upstream_ref: @upstream_ref,
               freshness_version: "raw-v4",
               success_run_id: "run-1"
             }
           ] = state.input_versions

    assert %{@target_key => ^state} = StateLoader.index([state])
  end

  test "rejects rows outside the exact requested identity set" do
    persisted = %FreshnessState{
      workspace_id: "workspace",
      deployment_id: "deployment",
      target_id: "unexpected",
      freshness_key: "unexpected",
      status: :fresh,
      payload: %{},
      updated_at: @now
    }

    assert {:error, :unexpected_freshness_identity} =
             StateLoader.decode_many([persisted], %{}, plan())
  end

  defp plan do
    nodes = %{
      @upstream_key => node(@upstream_ref, @upstream_key, [], [@target_key], 0),
      @target_key => node(@target_ref, @target_key, [@upstream_key], [], 1)
    }

    %Plan{
      target_refs: [@target_ref],
      target_node_keys: [@target_key],
      nodes: nodes,
      topo_order: [@upstream_ref, @target_ref],
      stages: [[@upstream_ref], [@target_ref]],
      node_stages: [[@upstream_key], [@target_key]]
    }
  end

  defp node(ref, node_key, upstream, downstream, stage) do
    %{
      ref: ref,
      node_key: node_key,
      window: nil,
      upstream: upstream,
      downstream: downstream,
      stage: stage,
      action: :run
    }
  end
end
