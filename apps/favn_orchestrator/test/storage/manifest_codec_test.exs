defmodule FavnOrchestrator.Storage.ManifestCodecTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest
  alias Favn.Manifest.Version
  alias FavnOrchestrator.Storage.ManifestCodec

  test "round-trips manifest version records" do
    version = manifest_version("mv_codec")

    assert {:ok, record} = ManifestCodec.to_record(version)
    assert record.manifest_version_id == "mv_codec"

    assert {:ok, decoded} = ManifestCodec.from_record(record)
    assert decoded.manifest_version_id == version.manifest_version_id
    assert decoded.content_hash == version.content_hash
  end

  test "rejects content hash mismatch" do
    version = manifest_version("mv_codec_mismatch")
    assert {:ok, record} = ManifestCodec.to_record(version)

    mismatch = %{record | content_hash: "wrong_hash"}

    assert {:error, {:manifest_content_hash_mismatch, "wrong_hash", _actual}} =
             ManifestCodec.from_record(mismatch)
  end

  defp manifest_version(manifest_version_id) do
    manifest = %Manifest{
      assets: [
        %Favn.Manifest.Asset{ref: {MyApp.Asset, :asset}, module: MyApp.Asset, name: :asset}
      ]
    }

    {:ok, version} = Version.new(manifest, manifest_version_id: manifest_version_id)
    version
  end
end
