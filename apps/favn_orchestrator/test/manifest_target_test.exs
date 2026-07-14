defmodule FavnOrchestrator.ManifestTargetTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest.Version
  alias FavnOrchestrator.ManifestTarget

  test "encodes and resolves manifest target ids" do
    version = version_fixture()

    assert ManifestTarget.asset_id({__MODULE__.Asset, :orders}) ==
             "asset:Elixir.FavnOrchestrator.ManifestTargetTest.Asset:orders"

    assert ManifestTarget.pipeline_id(__MODULE__.Pipeline) ==
             "pipeline:Elixir.FavnOrchestrator.ManifestTargetTest.Pipeline"

    assert {:ok, %{ref: {__MODULE__.Asset, :orders}}} =
             ManifestTarget.resolve_asset(
               version,
               ManifestTarget.asset_id({__MODULE__.Asset, :orders})
             )

    assert {:ok, __MODULE__.Pipeline} =
             ManifestTarget.resolve_pipeline_module(
               version,
               ManifestTarget.pipeline_id(__MODULE__.Pipeline)
             )
  end

  test "rejects target ids outside the pinned manifest" do
    version = version_fixture()

    assert {:error, :invalid_asset_target} = ManifestTarget.resolve_asset(version, "asset:other")

    assert {:error, :invalid_pipeline_target} =
             ManifestTarget.resolve_pipeline_module(version, "pipeline:other")
  end

  defp version_fixture do
    %Version{
      manifest_version_id: "manifest-1",
      content_hash: "sha256:test",
      manifest: %{
        assets: [%{ref: {__MODULE__.Asset, :orders}}],
        pipelines: [%{module: __MODULE__.Pipeline}]
      }
    }
  end
end
