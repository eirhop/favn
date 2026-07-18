defmodule FavnStoragePostgres.StorageV2.ManifestCacheTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest.Version
  alias FavnOrchestrator.Persistence.Queries.ManifestSelector.ByContentHash
  alias FavnOrchestrator.Persistence.Queries.ManifestSelector.ById
  alias FavnStoragePostgres.Registry.ManifestCache

  test "bounds immutable manifest releases while indexing both stable identities" do
    start_supervised!({ManifestCache, max_entries: 2, max_bytes: 1_000_000})
    first = version("first")
    second = version("second")
    third = version("third")

    assert :ok = ManifestCache.put(first)
    assert :ok = ManifestCache.put(second)

    assert {:ok, ^first} =
             ManifestCache.get(%ById{manifest_version_id: first.manifest_version_id})

    assert {:ok, ^second} = ManifestCache.get(%ByContentHash{content_hash: second.content_hash})

    assert :ok = ManifestCache.put(third)
    assert :miss = ManifestCache.get(%ById{manifest_version_id: first.manifest_version_id})
    assert :miss = ManifestCache.get(%ByContentHash{content_hash: first.content_hash})

    assert {:ok, ^second} =
             ManifestCache.get(%ById{manifest_version_id: second.manifest_version_id})

    assert {:ok, ^third} =
             ManifestCache.get(%ById{manifest_version_id: third.manifest_version_id})

    assert %{
             running?: true,
             entries: 2,
             bytes: bytes,
             max_entries: 2,
             max_bytes: 1_000_000,
             oversized_skips: 0
           } = ManifestCache.diagnostics()

    assert bytes > 0
  end

  test "does not cache a compact manifest index larger than the byte budget" do
    start_supervised!({ManifestCache, max_entries: 2, max_bytes: 1})
    version = version("oversized")

    assert :ok = ManifestCache.put(version)
    assert :miss = ManifestCache.get(%ById{manifest_version_id: version.manifest_version_id})

    assert %{
             entries: 0,
             bytes: 0,
             max_bytes: 1,
             oversized_skips: 1
           } = ManifestCache.diagnostics()
  end

  defp version(identity) do
    %Version{
      manifest_version_id: "manifest-" <> identity,
      content_hash: String.pad_trailing(identity, 64, "0"),
      schema_version: 6,
      runner_contract_version: 6,
      manifest: %Favn.Manifest{},
      serialization_format: "json-v1"
    }
  end
end
