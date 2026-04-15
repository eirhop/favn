defmodule FavnRunner.ManifestResolverTest do
  use ExUnit.Case, async: true

  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Version
  alias FavnRunner.ManifestResolver

  test "resolves target ref from asset_ref" do
    work = %RunnerWork{asset_ref: {MyApp.Asset, :asset}}
    assert {:ok, {MyApp.Asset, :asset}} = ManifestResolver.resolve_target_ref(work)
  end

  test "resolves target ref from asset_refs when asset_ref is nil" do
    work = %RunnerWork{asset_refs: [{MyApp.Asset, :asset}]}
    assert {:ok, {MyApp.Asset, :asset}} = ManifestResolver.resolve_target_ref(work)
  end

  test "rejects multiple target refs" do
    work = %RunnerWork{asset_refs: [{MyApp.Asset, :asset}, {MyApp.OtherAsset, :asset}]}
    assert {:error, :multiple_asset_targets} = ManifestResolver.resolve_target_ref(work)
  end

  test "resolves asset descriptor from manifest version" do
    {:ok, version} = Version.new(build_manifest(), manifest_version_id: "mv_resolve")

    assert {:ok, %Asset{ref: {MyApp.Asset, :asset}}} =
             ManifestResolver.resolve_asset(version, {MyApp.Asset, :asset})
  end

  defp build_manifest do
    asset = %Asset{ref: {MyApp.Asset, :asset}, module: MyApp.Asset, name: :asset}

    %Manifest{
      schema_version: 1,
      runner_contract_version: 1,
      assets: [asset],
      pipelines: [],
      schedules: [],
      graph: %Graph{nodes: [asset.ref], edges: [], topo_order: [asset.ref]},
      metadata: %{}
    }
  end
end
