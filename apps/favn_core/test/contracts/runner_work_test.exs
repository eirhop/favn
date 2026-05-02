defmodule Favn.Contracts.RunnerWorkTest do
  use ExUnit.Case, async: true

  alias Favn.Contracts.RunnerWork

  test "references pinned manifest identity" do
    work =
      %RunnerWork{
        run_id: "run_1",
        manifest_version_id: "mv_1",
        manifest_content_hash: "abc",
        asset_ref: {MyApp.Asset, :asset},
        asset_refs: [{MyApp.Asset, :asset}],
        planned_asset_refs: [{MyApp.Dependency, :asset}, {MyApp.Asset, :asset}],
        params: %{full_refresh: false},
        trigger: %{kind: :manual},
        metadata: %{requested_by: "operator"}
      }

    assert work.manifest_version_id == "mv_1"
    assert work.manifest_content_hash == "abc"
    assert work.asset_ref == {MyApp.Asset, :asset}
    assert work.asset_refs == [{MyApp.Asset, :asset}]
    assert work.planned_asset_refs == [{MyApp.Dependency, :asset}, {MyApp.Asset, :asset}]
  end
end
