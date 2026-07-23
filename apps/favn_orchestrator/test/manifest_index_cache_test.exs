defmodule FavnOrchestrator.ManifestIndexCacheTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Version
  alias FavnOrchestrator.ManifestIndexCache

  test "caches an immutable compiled index and reports hits" do
    {:ok, cache} = start_supervised({ManifestIndexCache, name: nil})
    version = version!("mv_hit", %{value: 1})

    assert {:ok, first} = ManifestIndexCache.fetch(version, server: cache)
    assert {:ok, second} = ManifestIndexCache.fetch(version, server: cache)
    assert first == second

    assert %{entries: 1, hits: 1, misses: 1, bytes: bytes} =
             ManifestIndexCache.diagnostics(server: cache)

    assert bytes > 0
  end

  test "evicts oldest indexes at the entry bound" do
    {:ok, cache} =
      start_supervised({ManifestIndexCache, name: nil, max_entries: 1, max_bytes: 1_000_000})

    first = version!("mv_first", %{value: 1})
    second = version!("mv_second", %{value: 2})

    assert {:ok, _index} = ManifestIndexCache.fetch(first, server: cache)
    assert {:ok, _index} = ManifestIndexCache.fetch(second, server: cache)

    assert %{entries: 1, evictions: 1, misses: 2} =
             ManifestIndexCache.diagnostics(server: cache)

    assert {:ok, _rebuilt} = ManifestIndexCache.fetch(first, server: cache)
    assert %{misses: 3, evictions: 2} = ManifestIndexCache.diagnostics(server: cache)
  end

  test "serves an index without retaining it when it exceeds the byte budget" do
    {:ok, cache} =
      start_supervised({ManifestIndexCache, name: nil, max_entries: 1, max_bytes: 1})

    assert {:ok, _index} =
             ManifestIndexCache.fetch(version!("mv_large", %{}), server: cache)

    assert %{entries: 0, oversized_skips: 1, misses: 1} =
             ManifestIndexCache.diagnostics(server: cache)
  end

  defp version!(id, metadata) do
    manifest = %Manifest{
      schema_version: 11,
      runner_contract_version: 11,
      required_runner_release_id: FavnTestSupport.runner_release_id(),
      assets: [],
      pipelines: [],
      schedules: [],
      graph: %Graph{},
      metadata: metadata
    }

    {:ok, version} = Version.new(manifest, manifest_version_id: id)
    version
  end
end
