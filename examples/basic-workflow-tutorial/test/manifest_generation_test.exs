defmodule FavnReferenceWorkload.ManifestGenerationTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest

  @runner_release_id "rr_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

  test "CRM workload compiles into one layered manifest" do
    assert {:ok, %Manifest{} = manifest} =
             Favn.generate_manifest(runner_release_id: @runner_release_id)

    assert length(manifest.assets) == 15
    assert length(manifest.pipelines) == 2

    assert Enum.any?(manifest.assets, fn asset ->
             asset.ref ==
               {FavnReferenceWorkload.Warehouse.Landing.GenerateSeed, :asset} and
               asset.type == :elixir
           end)

    assert Enum.any?(manifest.assets, fn asset ->
             asset.ref ==
               {FavnReferenceWorkload.Warehouse.Landing.WriteExtracts, :deals_daily}
           end)

    assert Enum.any?(manifest.assets, fn asset ->
             asset.ref == {FavnReferenceWorkload.Warehouse.Mart.ExecutiveOverview, :asset}
           end)

    assert Enum.any?(manifest.pipelines, &(&1.name == :bootstrap_crm_demo))
    assert Enum.any?(manifest.pipelines, &(&1.name == :daily_crm_analytics))
  end

  test "manifest content hash is stable across JSON publication boundary" do
    assert {:ok, build} = FavnAuthoring.build_manifest(runner_release_id: @runner_release_id)
    assert {:ok, original} = FavnAuthoring.pin_manifest_version(build.manifest)
    assert {:ok, public_hash} = FavnAuthoring.hash_manifest(build.manifest)
    assert public_hash == original.content_hash

    decoded =
      build.manifest
      |> Favn.Manifest.Serializer.encode_manifest!()
      |> JSON.decode!()

    assert {:ok, verified} =
             Favn.Manifest.Version.from_published(decoded,
               manifest_version_id: original.manifest_version_id,
               content_hash: original.content_hash,
               schema_version: original.schema_version,
               runner_contract_version: original.runner_contract_version,
               required_runner_release_id: original.required_runner_release_id,
               serialization_format: original.serialization_format
             )

    assert verified.content_hash == original.content_hash
  end
end
