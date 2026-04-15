defmodule FavnRunner.ManifestStoreTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest
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

  defp build_manifest(metadata \\ %{}) do
    %Manifest{
      schema_version: 1,
      runner_contract_version: 1,
      assets: [],
      pipelines: [],
      schedules: [],
      graph: %Graph{},
      metadata: metadata
    }
  end
end
