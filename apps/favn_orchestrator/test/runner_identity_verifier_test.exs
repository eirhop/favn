defmodule FavnOrchestrator.RunnerIdentityVerifierTest do
  use ExUnit.Case, async: true

  alias Favn.Contracts.RelationInspectionResult
  alias Favn.Contracts.RunnerResult
  alias Favn.Manifest
  alias Favn.Manifest.Version
  alias FavnOrchestrator.RunnerIdentityVerifier
  alias FavnOrchestrator.RunState

  test "pins the complete run release map and accepts only matching runner-owned results" do
    primary = FavnTestSupport.runner_release_id()
    alternate = FavnTestSupport.runner_release_id(:alternate)
    version = version(primary)

    run =
      RunState.new(
        id: "release-bound-run",
        deployment_id: "release-bound-deployment",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        runner_releases: version.runner_releases,
        asset_ref: {__MODULE__, :asset}
      )

    assert :ok = RunnerIdentityVerifier.verify_run_manifest(run, version)

    assert {:error, {:runner_release_mismatch, _, _}} =
             RunnerIdentityVerifier.verify_run_manifest(
               %{run | runner_releases: %{"default" => alternate}},
               version
             )

    assert :ok =
             RunnerIdentityVerifier.verify_result(primary, %RunnerResult{
               required_runner_release_id: primary
             })

    assert {:error, {:runner_release_mismatch, ^primary, ^alternate}} =
             RunnerIdentityVerifier.verify_inspection_result(
               primary,
               %RelationInspectionResult{required_runner_release_id: alternate}
             )
  end

  test "normalizes malformed runner-owned release identities before they cross the boundary" do
    release_id = FavnTestSupport.runner_release_id()
    malformed = %{"secret" => String.duplicate("x", 100_000)}

    assert {:error, :invalid_runner_release_identity} =
             RunnerIdentityVerifier.verify_result(release_id, %RunnerResult{
               required_runner_release_id: malformed
             })

    assert {:error, :invalid_runner_release_identity} =
             RunnerIdentityVerifier.verify_inspection_result(
               release_id,
               %RelationInspectionResult{required_runner_release_id: malformed}
             )
  end

  defp version(release_id) do
    %Version{
      manifest_version_id: "release-bound-manifest",
      content_hash: String.duplicate("a", 64),
      runner_releases: %{"default" => release_id},
      manifest: %Manifest{runner_releases: %{"default" => release_id}}
    }
  end
end
