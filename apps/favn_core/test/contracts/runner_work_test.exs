defmodule Favn.Contracts.RunnerWorkTest do
  use ExUnit.Case, async: true

  alias Favn.Contracts.RunnerWork
  alias Favn.Plan.NodeIdentity

  test "references pinned manifest identity" do
    node_identity = %NodeIdentity{
      manifest_version_id: "mv_1",
      node_key: {{MyApp.Asset, :asset}, nil},
      target_refs: [{MyApp.Asset, :asset}],
      planned_asset_refs: [{MyApp.Dependency, :asset}, {MyApp.Asset, :asset}],
      execution_pool: :default
    }

    work =
      %RunnerWork{
        run_id: "run_1",
        manifest_version_id: "mv_1",
        manifest_content_hash: "abc",
        node_identity: node_identity,
        asset_ref: {MyApp.Asset, :asset},
        asset_refs: [{MyApp.Asset, :asset}],
        planned_asset_refs: [{MyApp.Dependency, :asset}, {MyApp.Asset, :asset}],
        attempt: 2,
        max_attempts: 3,
        asset_step_id: "step_1",
        stage: 1,
        params: %{full_refresh: false},
        trigger: %{kind: :manual},
        metadata: %{requested_by: "operator"}
      }

    assert work.manifest_version_id == "mv_1"
    assert work.manifest_content_hash == "abc"
    assert work.asset_ref == {MyApp.Asset, :asset}
    assert work.asset_refs == [{MyApp.Asset, :asset}]

    assert RunnerWork.planned_asset_refs(work) == [
             {MyApp.Dependency, :asset},
             {MyApp.Asset, :asset}
           ]

    assert RunnerWork.node_key(work) == {{MyApp.Asset, :asset}, nil}
    assert RunnerWork.execution_pool(work) == :default
    assert RunnerWork.lifecycle_metadata(work).asset_step_id == "step_1"
  end
end
