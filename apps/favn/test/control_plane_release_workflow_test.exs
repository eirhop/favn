defmodule Favn.ControlPlaneReleaseWorkflowTest do
  use ExUnit.Case, async: true

  @root Path.expand("../../..", __DIR__)
  @script Path.join(@root, "scripts/check_control_plane_release_destinations.sh")
  @digest "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  @other_digest "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

  setup do
    root =
      Path.join(
        System.tmp_dir!(),
        "favn_release_destinations_#{System.unique_integer([:positive, :monotonic])}"
      )

    bin = Path.join(root, "bin")
    File.mkdir_p!(bin)
    write_fake_docker(Path.join(bin, "docker"))
    write_fake_gh(Path.join(bin, "gh"))
    on_exit(fn -> File.rm_rf(root) end)

    %{bin: bin}
  end

  test "treats explicit registry and GitHub 404 responses as unused destinations", %{bin: bin} do
    assert {output, 0} = run_check(bin, "missing", "missing")
    assert output =~ "registry_exists=false"
    assert output =~ "release_exists=false"
  end

  test "accepts existing destinations only when both bind the expected digest", %{bin: bin} do
    assert {output, 0} = run_check(bin, "matching", "matching")
    assert output =~ "registry_exists=true"
    assert output =~ "release_exists=true"
  end

  test "fails closed when an existing registry tag has another digest", %{bin: bin} do
    assert {output, 1} = run_check(bin, "mismatch", "missing")
    assert output =~ "already points to unexpected digest"
  end

  test "fails closed for an unexpected registry lookup error", %{bin: bin} do
    assert {output, 1} = run_check(bin, "error", "missing")
    assert output =~ "registry unavailable"
  end

  test "does not mistake an unrelated local not-found error for a missing image", %{bin: bin} do
    assert {output, 1} = run_check(bin, "local_missing", "missing")
    assert output =~ "credential helper: not found"
  end

  test "fails closed when an existing release omits the expected digest", %{bin: bin} do
    assert {output, 1} = run_check(bin, "missing", "mismatch")
    assert output =~ "does not record the expected image digest"
  end

  test "manual recovery uses the corrected workflow helper with signed-tag source" do
    workflow = File.read!(Path.join(@root, ".github/workflows/control-plane-release.yml"))

    assert workflow =~ "workflow_dispatch:"
    assert workflow =~ "ref: ${{ github.sha }}"
    assert workflow =~ "path: .release-workflow"
    assert workflow =~ ".release-workflow/scripts/check_control_plane_release_destinations.sh"
    assert workflow =~ "ref: ${{ steps.release_ref.outputs.release_tag }}"

    refute workflow =~
             ~r/if registry_digest=.*imagetools inspect.*\| tr -d/
  end

  defp run_check(bin, registry_mode, release_mode) do
    path = bin <> ":" <> System.fetch_env!("PATH")

    System.cmd(
      @script,
      ["ghcr.io/eirhop/favn-control-plane", "v0.5.0-rc.7", @digest, "eirhop/favn"],
      env: [
        {"PATH", path},
        {"FAKE_EXPECTED_DIGEST", @digest},
        {"FAKE_OTHER_DIGEST", @other_digest},
        {"FAKE_REGISTRY_MODE", registry_mode},
        {"FAKE_RELEASE_MODE", release_mode}
      ],
      stderr_to_stdout: true
    )
  end

  defp write_fake_docker(path) do
    File.write!(
      path,
      """
      #!/usr/bin/env bash
      set -euo pipefail

      case "$FAKE_REGISTRY_MODE" in
        missing)
          echo "ERROR: ghcr.io/eirhop/favn-control-plane:v0.5.0-rc.7: not found" >&2
          exit 1
          ;;
        matching)
          printf '"%s"\\n' "$FAKE_EXPECTED_DIGEST"
          ;;
        mismatch)
          printf '"%s"\\n' "$FAKE_OTHER_DIGEST"
          ;;
        error)
          echo "registry unavailable" >&2
          exit 1
          ;;
        local_missing)
          echo "credential helper: not found" >&2
          exit 1
          ;;
      esac
      """
    )

    File.chmod!(path, 0o755)
  end

  defp write_fake_gh(path) do
    File.write!(
      path,
      """
      #!/usr/bin/env bash
      set -euo pipefail

      case "$FAKE_RELEASE_MODE" in
        missing)
          echo "gh: Not Found (HTTP 404)" >&2
          exit 1
          ;;
        matching)
          printf 'Control-plane deployment reference: `%s@%s`\\n' \\
            "ghcr.io/eirhop/favn-control-plane" \\
            "$FAKE_EXPECTED_DIGEST"
          ;;
        mismatch)
          printf 'Control-plane deployment reference: `%s@%s`\\n' \\
            "ghcr.io/eirhop/favn-control-plane" \\
            "$FAKE_OTHER_DIGEST"
          ;;
      esac
      """
    )

    File.chmod!(path, 0o755)
  end
end
