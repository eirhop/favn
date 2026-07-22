defmodule FavnOrchestrator.TargetGenerationsTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Index
  alias Favn.Manifest.Version
  alias Favn.Plan
  alias Favn.Retry.Policy
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.TargetGenerations

  test "pins output and upstream semantic generations into a normal plan" do
    upstream_ref = {MyApp.Raw, :orders}
    target_ref = {MyApp.Gold, :orders}
    upstream_key = {upstream_ref, nil}
    target_key = {target_ref, nil}

    manifest =
      %Manifest{
        assets: [asset(upstream_ref), asset(target_ref)]
      }
      |> FavnTestSupport.with_manifest_contract()
      |> FavnTestSupport.with_manifest_graph()

    assert {:ok, version} = Version.new(manifest, manifest_version_id: "manifest")
    assert {:ok, index} = Index.build_from_version(version)
    assert {:ok, context} = WorkspaceContext.new("workspace", "test", [:workspace_admin])

    plan = %Plan{
      target_refs: [target_ref],
      target_node_keys: [target_key],
      nodes: %{
        upstream_key => node(upstream_ref, upstream_key, [], [target_key], 0),
        target_key => node(target_ref, target_key, [upstream_key], [], 1)
      },
      topo_order: [upstream_ref, target_ref],
      stages: [[upstream_ref], [target_ref]],
      node_stages: [[upstream_key], [target_key]]
    }

    assert {:ok, pinned} =
             TargetGenerations.pin_plan(context, version, index, plan, ~U[2026-07-22 10:00:00Z])

    upstream = pinned.nodes[upstream_key]
    target = pinned.nodes[target_key]

    assert upstream.target_id == Favn.TargetIdentity.for_asset(upstream_ref)
    assert upstream.target_generation_id == nil
    assert is_binary(upstream.evidence_generation_id)
    assert upstream.input_generations == []

    assert target.target_id == Favn.TargetIdentity.for_asset(target_ref)
    assert target.target_generation_id == nil

    assert [input] = target.input_generations
    assert input.target_id == upstream.target_id
    assert input.evidence_generation_id == upstream.evidence_generation_id
  end

  defp asset({module, name} = ref) do
    %Asset{ref: ref, module: module, name: name, type: :elixir}
  end

  defp node(ref, key, upstream, downstream, stage) do
    %{
      ref: ref,
      node_key: key,
      window: nil,
      upstream: upstream,
      downstream: downstream,
      stage: stage,
      execution_pool: nil,
      action: :run,
      retry_policy: Policy.default(),
      retry_policy_source: :default
    }
  end
end
