defmodule FavnRunner.GenerationWorkTest do
  use ExUnit.Case, async: true

  alias Favn.Contracts.RunnerWork
  alias Favn.Contracts.TargetGenerationPin
  alias Favn.Manifest.Asset
  alias Favn.RelationRef
  alias Favn.Run.AssetContext
  alias FavnRunner.ContextBuilder
  alias FavnRunner.GenerationWork

  test "candidate writes and dependency reads use their pinned relation overrides" do
    target_ref = {MyApp.Assets.Target, :asset}
    upstream_ref = {MyApp.Assets.Upstream, :asset}
    stable = relation("target")
    candidate = relation("target__candidate")
    upstream_manifest = relation("upstream")
    upstream_pinned = relation("upstream__active")

    asset = %Asset{ref: target_ref, relation: stable, runtime_config: %{}}

    work = %RunnerWork{
      manifest_version_id: "mv_generation_overrides",
      manifest_content_hash: String.duplicate("b", 64),
      required_runner_release_id: FavnTestSupport.runner_release_id(),
      target_operation: :rebuild_candidate,
      logical_target_id: "target:output",
      target_descriptor_hash: String.duplicate("c", 64),
      target_generation_id: "018f47a0-7b0d-4b1a-8d8b-e18a9a987655",
      active_relation: stable,
      write_relation: candidate,
      rebuild_operation_id: "rebuild-generation",
      rebuild_action_id: "action-generation",
      rebuild_item_id: "item-generation",
      upstream_generation_pins: [
        %TargetGenerationPin{
          asset_ref: upstream_ref,
          target_id: "target:upstream",
          target_generation_id: "018f47a0-7b0d-4b1a-8d8b-e18a9a987654",
          relation: upstream_pinned,
          descriptor_hash: String.duplicate("a", 64)
        }
      ]
    }

    assert :ok = RunnerWork.validate_generation_contract(work)

    assert {%Asset{relation: ^candidate}, relations} =
             GenerationWork.apply_overrides(
               asset,
               %{elem(upstream_ref, 0) => upstream_manifest},
               work
             )

    assert relations[elem(upstream_ref, 0)] == upstream_pinned

    assert {:ok, %{asset: %AssetContext{relation: ^candidate}}} =
             ContextBuilder.build(work, asset, "execution-generation")
  end

  defp relation(name),
    do: RelationRef.new!(connection: :warehouse, schema: "analytics", name: name)
end
