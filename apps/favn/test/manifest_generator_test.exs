defmodule Favn.Manifest.GeneratorTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest

  defmodule TestSchedules do
    use Favn.Triggers.Schedules

    schedule(:daily, cron: "0 2 * * *", timezone: "Etc/UTC")
  end

  defmodule TestAsset do
    use Favn.Namespace, relation: [connection: :warehouse, catalog: "raw", schema: "sales"]
    use Favn.Asset

    @meta category: :sales, tags: [:raw]
    @relation true
    def asset(_ctx), do: :ok
  end

  defmodule TestPipeline do
    use Favn.Pipeline

    pipeline :daily_sales do
      asset(TestAsset)
      deps(:all)
      schedule({TestSchedules, :daily})
    end
  end

  test "generates manifest from explicit module lists" do
    assert {:ok, %Manifest{} = manifest} =
             Favn.generate_manifest(
               asset_modules: [TestAsset],
               pipeline_modules: [TestPipeline],
               schedule_modules: [TestSchedules]
             )

    assert manifest.schema_version == 1
    assert manifest.runner_contract_version == 1
    assert length(manifest.assets) == 1
    assert length(manifest.pipelines) == 1
    assert length(manifest.schedules) == 1
    assert manifest.graph.nodes == [{TestAsset, :asset}]

    [asset] = manifest.assets
    assert asset.ref == {TestAsset, :asset}

    [pipeline] = manifest.pipelines
    assert pipeline.name == :daily_sales

    [schedule] = manifest.schedules
    assert schedule.module == TestSchedules
    assert schedule.name == :daily
  end

  test "lists and fetches compiled assets without registry" do
    assert {:ok, [asset]} = Favn.list_assets([TestAsset])
    assert asset.ref == {TestAsset, :asset}

    assert {:ok, fetched} = Favn.get_asset(TestAsset)
    assert fetched.ref == {TestAsset, :asset}
  end

  test "builds, hashes, validates, and pins manifest versions" do
    assert {:ok, build} =
             Favn.build_manifest(
               asset_modules: [TestAsset],
               pipeline_modules: [TestPipeline],
               schedule_modules: [TestSchedules]
             )

    assert is_map(build.manifest)
    assert is_struct(build.manifest, Favn.Manifest)

    assert {:ok, _json} = Favn.serialize_manifest(build)
    assert {:ok, hash} = Favn.hash_manifest(build)
    assert byte_size(hash) == 64

    assert :ok =
             Favn.validate_manifest_compatibility(%{
               schema_version: 1,
               runner_contract_version: 1
             })

    assert {:ok, version} =
             Favn.pin_manifest_version(build,
               manifest_version_id: "mv_test_facade_001",
               inserted_at: ~U[2026-01-01 00:00:00Z]
             )

    assert version.manifest_version_id == "mv_test_facade_001"
    assert version.content_hash == hash
  end
end
