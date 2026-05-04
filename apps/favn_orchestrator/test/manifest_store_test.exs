defmodule FavnOrchestrator.ManifestStoreTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.Version
  alias Favn.Window.Policy
  alias Favn.Window.Spec, as: WindowSpec
  alias FavnOrchestrator
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.Storage.Adapter.Memory

  setup do
    Memory.reset()
    :ok
  end

  test "registers, lists, fetches, and activates manifests" do
    version_a = manifest_version("mv_a", {MyApp.AssetA, :asset})
    version_b = manifest_version("mv_b", {MyApp.AssetB, :asset})

    assert :ok = ManifestStore.register_manifest(version_a)
    assert :ok = ManifestStore.register_manifest(version_b)

    assert {:ok, versions} = ManifestStore.list_manifests()
    assert Enum.map(versions, & &1.manifest_version_id) == ["mv_a", "mv_b"]

    assert {:ok, fetched} = ManifestStore.get_manifest("mv_a")
    assert fetched.content_hash == version_a.content_hash

    assert :ok = ManifestStore.set_active_manifest("mv_b")
    assert {:ok, "mv_b"} = ManifestStore.get_active_manifest()
  end

  test "exposes operator manifest summaries and active-manifest targets" do
    window_spec = WindowSpec.new!(:day, lookback: 2, refresh_from: :hour, required: true)

    version_a =
      manifest_version(
        "mv_a",
        {MyApp.AssetA, :asset},
        [
          %Pipeline{
            module: MyApp.PipelineA,
            name: :pipeline_a,
            selectors: [{MyApp.AssetA, :asset}],
            deps: :all,
            window: Policy.new!(:daily),
            source: :dsl,
            outputs: [],
            config: %{},
            metadata: %{}
          }
        ],
        window_spec
      )

    version_b = manifest_version("mv_b", {MyApp.AssetB, :asset})

    assert :ok = ManifestStore.register_manifest(version_a)
    assert :ok = ManifestStore.register_manifest(version_b)
    assert :ok = ManifestStore.set_active_manifest("mv_a")

    assert {:ok, summaries} = FavnOrchestrator.list_manifest_summaries()
    assert Enum.map(summaries, & &1.manifest_version_id) == ["mv_a", "mv_b"]

    assert {:ok, summary} = FavnOrchestrator.get_manifest_summary("mv_a")
    assert summary.asset_count == 1
    assert summary.pipeline_count == 1

    assert {:ok, targets} = FavnOrchestrator.active_manifest_targets()
    assert targets.manifest_version_id == "mv_a"
    assert [asset] = targets.assets
    assert asset.label == "{MyApp.AssetA, :asset}"

    assert asset.window == %{
             kind: "day",
             lookback: 2,
             refresh_from: "hour",
             required: true,
             timezone: "Etc/UTC"
           }

    refute Map.has_key?(asset.window, "__struct__")

    assert [pipeline] = targets.pipelines
    assert pipeline.target_id == "pipeline:Elixir.MyApp.PipelineA"

    assert pipeline.window == %{
             kind: "day",
             anchor: "previous_complete_period",
             timezone: nil,
             allow_full_load: false
           }
  end

  test "publishes duplicate content under the existing canonical manifest version" do
    original = manifest_version("mv_original", {MyApp.AssetA, :asset})
    duplicate = manifest_version("mv_duplicate", {MyApp.AssetA, :asset})

    assert original.content_hash == duplicate.content_hash

    assert {:ok, :published, ^original} = ManifestStore.publish_manifest(original)
    assert {:ok, :already_published, canonical} = ManifestStore.publish_manifest(duplicate)
    assert canonical.manifest_version_id == "mv_original"

    assert {:error, :manifest_version_not_found} = ManifestStore.get_manifest("mv_duplicate")
  end

  defp manifest_version(manifest_version_id, ref, pipelines \\ [], window \\ nil) do
    manifest = %Manifest{
      assets: [
        %Favn.Manifest.Asset{ref: ref, module: elem(ref, 0), name: elem(ref, 1), window: window}
      ],
      pipelines: pipelines
    }

    {:ok, version} = Version.new(manifest, manifest_version_id: manifest_version_id)
    version
  end
end
