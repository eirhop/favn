defmodule FavnOrchestrator.API.ManifestPublication.ConfigTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.API.ManifestPublication.Config

  test "uses bounded defaults" do
    assert {:ok, config} = Config.new([])
    assert config.compressed_limit_bytes == 8 * 1024 * 1024
    assert config.decompressed_limit_bytes == 64 * 1024 * 1024
  end

  test "accepts exact maximum limits and rejects values outside them" do
    assert {:ok, config} =
             Config.new(
               compressed_limit_bytes: Config.maximum_compressed_limit_bytes(),
               decompressed_limit_bytes: Config.maximum_decompressed_limit_bytes()
             )

    assert config.compressed_limit_bytes == Config.maximum_compressed_limit_bytes()
    assert config.decompressed_limit_bytes == Config.maximum_decompressed_limit_bytes()

    assert {:error, {:invalid_manifest_publication_limit, :compressed_limit_bytes, 0, _maximum}} =
             Config.new(compressed_limit_bytes: 0)

    assert {:error,
            {:invalid_manifest_publication_limit, :decompressed_limit_bytes, too_large, maximum}} =
             Config.new(decompressed_limit_bytes: Config.maximum_decompressed_limit_bytes() + 1)

    assert too_large == maximum + 1

    assert {:error, {:invalid_manifest_publication_config, :decompressed_limit_too_small}} =
             Config.new(
               compressed_limit_bytes: 2 * 1_024 * 1_024,
               decompressed_limit_bytes: 1 * 1_024 * 1_024
             )
  end

  test "rejects unknown and malformed configuration" do
    assert {:error, {:unknown_manifest_publication_config, :other}} =
             Config.new(other: 1)

    assert {:error, {:invalid_manifest_publication_config, %{}}} = Config.new(%{})
  end
end
