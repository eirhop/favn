defmodule Favn.Dev.ControlPlaneImageTest do
  use ExUnit.Case, async: true

  alias Favn.Dev.ControlPlaneImage

  @build_id String.duplicate("a", 64)
  @digest "sha256:" <> String.duplicate("b", 64)

  test "build, version, and immutable references are exact" do
    assert ControlPlaneImage.repository() == "ghcr.io/eirhop/favn-control-plane"
    assert {:ok, "build-" <> @build_id} = ControlPlaneImage.build_tag(@build_id)
    assert {:ok, "v0.5.0-dev"} = ControlPlaneImage.version_tag("0.5.0-dev")

    assert {:ok, "ghcr.io/eirhop/favn-control-plane@" <> @digest} =
             ControlPlaneImage.immutable_reference(@digest)
  end

  test "mutable, foreign, malformed, and ambiguous references fail closed" do
    assert {:error, :invalid_control_plane_build_id} = ControlPlaneImage.build_tag("latest")
    assert {:error, :invalid_favn_version} = ControlPlaneImage.version_tag("main")
    assert {:error, :invalid_favn_version} = ControlPlaneImage.version_tag("1.0.0+build.1")
    assert {:error, :invalid_image_digest} = ControlPlaneImage.immutable_reference("latest")

    official = "ghcr.io/eirhop/favn-control-plane@" <> @digest

    assert {:ok, ^official} =
             ControlPlaneImage.repo_digest([
               "mirror.invalid/favn-control-plane@" <> @digest,
               official
             ])

    assert {:error, :repo_digest_unavailable} = ControlPlaneImage.repo_digest([])

    assert {:error, :repo_digest_unavailable} =
             ControlPlaneImage.repo_digest([
               official,
               "ghcr.io/eirhop/favn-control-plane@sha256:" <> String.duplicate("c", 64)
             ])
  end
end
