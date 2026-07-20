defmodule FavnRunner.ManifestStoreTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Version
  alias FavnRunner.ManifestStore

  test "register is idempotent for same manifest_version_id and hash" do
    {:ok, store} = start_supervised({ManifestStore, name: nil})
    {:ok, version} = Version.new(build_manifest(), manifest_version_id: "mv_1")

    assert :ok = ManifestStore.register(version, server: store)
    assert :ok = ManifestStore.register(version, server: store)
    assert {:ok, fetched} = ManifestStore.fetch("mv_1", version.content_hash, server: store)
    assert fetched.content_hash == version.content_hash
  end

  test "register rejects manifest id hash conflicts" do
    {:ok, store} = start_supervised({ManifestStore, name: nil})

    {:ok, version_one} =
      Version.new(build_manifest(%{kind: :one}), manifest_version_id: "mv_conflict")

    {:ok, version_two} =
      Version.new(build_manifest(%{kind: :two}), manifest_version_id: "mv_conflict")

    assert :ok = ManifestStore.register(version_one, server: store)

    assert {:error, {:manifest_version_conflict, "mv_conflict", _, _}} =
             ManifestStore.register(version_two, server: store)
  end

  test "fetch validates expected hash" do
    {:ok, store} = start_supervised({ManifestStore, name: nil})
    {:ok, version} = Version.new(build_manifest(), manifest_version_id: "mv_hash")

    assert :ok = ManifestStore.register(version, server: store)

    assert {:error, :manifest_hash_mismatch} =
             ManifestStore.fetch("mv_hash", "wrong", server: store)
  end

  test "cache evicts oldest immutable versions under its entry budget" do
    {:ok, store} =
      start_supervised({ManifestStore, name: nil, max_entries: 2, max_bytes: 1_000_000})

    versions =
      Enum.map(1..3, fn index ->
        {:ok, version} =
          Version.new(build_manifest(%{index: index}), manifest_version_id: "mv_#{index}")

        version
      end)

    Enum.each(versions, fn version ->
      assert :ok = ManifestStore.register(version, server: store)
    end)

    assert {:error, :manifest_not_found} = ManifestStore.fetch("mv_1", nil, server: store)
    assert {:ok, _version} = ManifestStore.fetch("mv_2", nil, server: store)
    assert {:ok, _version} = ManifestStore.fetch("mv_3", nil, server: store)

    assert %{count: 2, evictions: 1, bytes: bytes, max_bytes: 1_000_000} =
             ManifestStore.diagnostics(server: store)

    assert bytes > 0
  end

  test "cache rejects one version larger than its total byte budget" do
    {:ok, store} = start_supervised({ManifestStore, name: nil, max_entries: 2, max_bytes: 1})
    {:ok, version} = Version.new(build_manifest(), manifest_version_id: "mv_oversized")

    assert {:error, :manifest_exceeds_runner_cache_budget} =
             ManifestStore.register(version, server: store)

    assert %{count: 0, bytes: 0, oversized_rejections: 1} =
             ManifestStore.diagnostics(server: store)
  end

  test "active leases prevent eviction and release restores bounded eviction" do
    {:ok, store} = start_supervised({ManifestStore, name: nil, max_entries: 1})
    {:ok, first} = Version.new(build_manifest(%{index: 1}), manifest_version_id: "mv_leased")
    {:ok, second} = Version.new(build_manifest(%{index: 2}), manifest_version_id: "mv_next")
    lease_id = "run:leased"
    expires_at = DateTime.add(DateTime.utc_now(), 60, :second)

    assert :ok = ManifestStore.acquire(first, lease_id, expires_at, server: store)

    assert {:error, :manifest_cache_capacity_exhausted} =
             ManifestStore.register(second, server: store)

    assert {:ok, _version} = ManifestStore.fetch("mv_leased", first.content_hash, server: store)
    assert %{active_leases: 1, count: 1} = ManifestStore.diagnostics(server: store)

    assert :ok = ManifestStore.release(lease_id, server: store)
    assert :ok = ManifestStore.register(second, server: store)
    assert {:error, :manifest_not_found} = ManifestStore.fetch("mv_leased", nil, server: store)
    assert {:ok, _version} = ManifestStore.fetch("mv_next", second.content_hash, server: store)
  end

  test "short leases renew while live and expired crash leases do not pin cache capacity" do
    {:ok, store} = start_supervised({ManifestStore, name: nil, max_entries: 1})
    {:ok, first} = Version.new(build_manifest(%{index: 1}), manifest_version_id: "mv_short")
    {:ok, second} = Version.new(build_manifest(%{index: 2}), manifest_version_id: "mv_after")
    lease_id = "run:short"

    assert :ok =
             ManifestStore.acquire(
               first,
               lease_id,
               DateTime.add(DateTime.utc_now(), 1, :second),
               server: store
             )

    assert :ok =
             ManifestStore.renew(
               lease_id,
               DateTime.add(DateTime.utc_now(), 60, :second),
               server: store
             )

    assert {:error, :manifest_cache_capacity_exhausted} =
             ManifestStore.register(second, server: store)

    assert :ok = ManifestStore.release(lease_id, server: store)

    assert :ok =
             ManifestStore.acquire(
               first,
               lease_id,
               DateTime.add(DateTime.utc_now(), 1, :millisecond),
               server: store
             )

    Process.sleep(5)
    assert :ok = ManifestStore.register(second, server: store)
    assert %{active_leases: 0, count: 1} = ManifestStore.diagnostics(server: store)
  end

  test "execution bundle is atomic and requires the exact active lease" do
    {:ok, store} = start_supervised({ManifestStore, name: nil})

    asset = %Asset{ref: {ScaleAssets, :one}, module: ScaleAssets, name: :one}
    {:ok, version} = Version.new(build_manifest(%{}, [asset]), manifest_version_id: "mv_bundle")
    expires_at = DateTime.add(DateTime.utc_now(), 60, :second)

    assert :ok = ManifestStore.acquire(version, "run:bundle", expires_at, server: store)

    assert {:ok, handle, %Asset{ref: {ScaleAssets, :one}}, %{}} =
             ManifestStore.fetch_execution_bundle(
               "run:bundle",
               version.manifest_version_id,
               version.content_hash,
               {ScaleAssets, :one},
               nil,
               server: store
             )

    assert handle.manifest_version_id == version.manifest_version_id

    assert {:error, :manifest_lease_not_found} =
             ManifestStore.fetch_execution_bundle(
               "run:missing",
               version.manifest_version_id,
               version.content_hash,
               {ScaleAssets, :one},
               nil,
               server: store
             )
  end

  test "builds one index and performs one bounded lookup per asset request" do
    {:ok, store} =
      start_supervised({ManifestStore, name: nil, max_entries: 2, max_bytes: 20_000_000})

    assets =
      Enum.map(1..2_000, fn index ->
        name = String.to_atom("scale_asset_#{index}")

        dependencies =
          if index == 1,
            do: [],
            else: [{ScaleAssets, String.to_atom("scale_asset_#{index - 1}")}]

        %Asset{
          ref: {ScaleAssets, name},
          module: ScaleAssets,
          name: name,
          depends_on: dependencies
        }
      end)

    {:ok, version} =
      Version.new(build_manifest(%{}, assets), manifest_version_id: "mv_scale")

    assert :ok = ManifestStore.register(version, server: store)

    assert {:ok, handle} =
             ManifestStore.fetch_handle("mv_scale", version.content_hash, server: store)

    Enum.each([1, 500, 1_000, 1_500, 2_000], fn index ->
      ref = {ScaleAssets, String.to_atom("scale_asset_#{index}")}
      assert {:ok, %Asset{ref: ^ref}} = ManifestStore.fetch_asset(handle, ref, server: store)
    end)

    assert %{index_builds: 1, asset_lookups: 5, count: 1} =
             ManifestStore.diagnostics(server: store)
  end

  defp build_manifest(metadata \\ %{}, assets \\ []) do
    {:ok, graph} = Graph.build(assets)

    %Manifest{
      schema_version: 9,
      runner_contract_version: 9,
      assets: assets,
      pipelines: [],
      schedules: [],
      graph: graph,
      metadata: metadata
    }
  end
end
