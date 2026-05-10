defmodule FavnOrchestrator.ManifestStoreTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.Version
  alias Favn.Run.AssetResult
  alias Favn.Window.Policy
  alias Favn.Window.Spec, as: WindowSpec
  alias FavnOrchestrator
  alias FavnOrchestrator.AssetFreshnessState
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage
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

    finished_at = DateTime.add(DateTime.utc_now(), -300, :second)
    old_finished_at = DateTime.add(finished_at, -3_600, :second)

    assert :ok =
             Storage.put_run(
               run_state(
                 "run_old_asset_a",
                 {MyApp.AssetA, :asset},
                 :error,
                 old_finished_at,
                 "mv_old"
               )
             )

    assert :ok =
             Storage.put_run(
               run_state("run_asset_a", {MyApp.AssetA, :asset}, :ok, finished_at, "mv_a")
             )

    assert {:ok, freshness} =
             AssetFreshnessState.new(%{
               asset_ref_module: MyApp.AssetA,
               asset_ref_name: :asset,
               freshness_key: "latest",
               status: :ok,
               latest_success_run_id: "run_asset_a",
               latest_success_at: finished_at,
               latest_attempt_status: :ok,
               latest_attempt_at: finished_at,
               manifest_version_id: "mv_a",
               updated_at: finished_at
             })

    assert :ok = Storage.put_asset_freshness_state(freshness)

    assert {:ok, stale_window_freshness} =
             AssetFreshnessState.new(%{
               asset_ref_module: MyApp.AssetA,
               asset_ref_name: :asset,
               freshness_key: "calendar:day:Etc/UTC:2026-05-09",
               status: :error,
               latest_attempt_run_id: "run_old_asset_a",
               latest_attempt_status: :error,
               latest_attempt_at: old_finished_at,
               manifest_version_id: "mv_a",
               updated_at: old_finished_at
             })

    assert :ok = Storage.put_asset_freshness_state(stale_window_freshness)

    assert {:ok, [entry]} = FavnOrchestrator.active_asset_catalogue()
    assert entry.target_id == "asset:Elixir.MyApp.AssetA:asset"
    assert entry.status == :healthy
    assert entry.latest_run_id == "run_asset_a"
    assert entry.latest_run_status == :ok
    assert entry.latest_run_at == finished_at
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

  defp run_state(id, ref, status, finished_at, manifest_version_id) do
    RunState.new(
      id: id,
      manifest_version_id: manifest_version_id,
      manifest_content_hash: "hash_a",
      asset_ref: ref,
      target_refs: [ref]
    )
    |> RunState.transition(
      status: status,
      result: %{
        asset_results: [
          %AssetResult{
            ref: ref,
            stage: 0,
            status: status,
            started_at: DateTime.add(finished_at, -1, :second),
            finished_at: finished_at,
            duration_ms: 1
          }
        ]
      }
    )
    |> Map.put(:updated_at, finished_at)
    |> RunState.with_snapshot_hash()
  end
end
