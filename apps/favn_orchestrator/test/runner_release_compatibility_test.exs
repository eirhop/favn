defmodule FavnOrchestrator.RunnerReleaseCompatibilityTest do
  use ExUnit.Case, async: true

  alias Favn.Contracts.RelationInspectionResult
  alias Favn.Contracts.RunnerResult
  alias Favn.Manifest
  alias Favn.Manifest.Version
  alias FavnOrchestrator.RunnerReleaseCompatibility
  alias FavnOrchestrator.RunState

  defmodule ReadyClient do
    def register_manifest(_, _), do: :ok
    def ensure_manifest(_, _), do: :ok
    def acquire_manifest(_, _, _, _, _), do: :ok
    def renew_manifest(_, _, _), do: :ok
    def release_manifest(_, _), do: :ok
    def submit_work(_, _), do: {:ok, "exec_1"}
    def await_result(_, _, _), do: {:error, :not_started}
    def cancel_work(_, _, _), do: :ok
    def inspect_relation(_, _), do: {:error, :not_supported}

    def diagnostics(opts) do
      diagnostics =
        case Keyword.fetch(opts, :diagnostics_release) do
          {:ok, release} -> %{release: release}
          :error -> %{runner_release_id: Keyword.get(opts, :runner_release_id)}
        end

      if Keyword.get(opts, :omit_readiness?, false) do
        {:ok, diagnostics}
      else
        {:ok,
         Map.merge(diagnostics, %{
           available?: true,
           ready?: Keyword.get(opts, :ready?, true)
         })}
      end
    end
  end

  test "requires a ready runner with the exact manifest release" do
    primary = FavnTestSupport.runner_release_id()
    alternate = FavnTestSupport.runner_release_id(:alternate)
    version = version(primary)

    assert :ok =
             RunnerReleaseCompatibility.verify_runner(ReadyClient, version,
               runner_release_id: primary
             )

    assert {:error, {:runner_release_mismatch, ^primary, ^alternate}} =
             RunnerReleaseCompatibility.verify_runner(ReadyClient, version,
               runner_release_id: alternate
             )

    assert {:error, :runner_not_ready} =
             RunnerReleaseCompatibility.verify_runner(ReadyClient, version,
               runner_release_id: primary,
               ready?: false
             )

    assert {:error, :runner_not_ready} =
             RunnerReleaseCompatibility.verify_runner(
               ReadyClient,
               version,
               runner_release_id: primary,
               omit_readiness?: true
             )

    assert {:error, :runner_release_info_unavailable} =
             RunnerReleaseCompatibility.verify_runner(ReadyClient, version,
               diagnostics_release: "malformed"
             )
  end

  test "emits bounded diagnostic latency and release identity" do
    release_id = FavnTestSupport.runner_release_id()
    version = version(release_id)
    handler_id = {__MODULE__, self(), make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        [:favn, :orchestrator, :runner_release_diagnostics_checked],
        fn _event, measurements, metadata, pid ->
          send(pid, {:runner_release_diagnostics, measurements, metadata})
        end,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    assert :ok =
             RunnerReleaseCompatibility.verify_runner(ReadyClient, version,
               runner_release_id: release_id
             )

    assert_receive {:runner_release_diagnostics, %{duration_ms: duration_ms}, metadata}
    assert duration_ms >= 0
    assert metadata.status == :ready
    assert metadata.manifest_version_id == version.manifest_version_id
    assert metadata.required_runner_release_id == release_id
    assert metadata.runner_release_id == release_id
  end

  test "pins runs and accepts only matching runner-owned results" do
    primary = FavnTestSupport.runner_release_id()
    alternate = FavnTestSupport.runner_release_id(:alternate)
    version = version(primary)

    run =
      RunState.new(
        id: "release-bound-run",
        deployment_id: "release-bound-deployment",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        required_runner_release_id: primary,
        asset_ref: {__MODULE__, :asset}
      )

    assert :ok = RunnerReleaseCompatibility.verify_run_manifest(run, version)

    assert {:error, :legacy_runner_release_unbound} =
             RunnerReleaseCompatibility.verify_run_manifest(
               %{run | required_runner_release_id: nil},
               version
             )

    assert :ok =
             RunnerReleaseCompatibility.verify_result(primary, %RunnerResult{
               required_runner_release_id: primary
             })

    assert {:error, {:runner_release_mismatch, ^primary, ^alternate}} =
             RunnerReleaseCompatibility.verify_inspection_result(
               primary,
               %RelationInspectionResult{required_runner_release_id: alternate}
             )
  end

  test "normalizes malformed runner-owned release identities before they cross the boundary" do
    release_id = FavnTestSupport.runner_release_id()
    malformed = %{"secret" => String.duplicate("x", 100_000)}

    assert {:error, :invalid_runner_release_identity} =
             RunnerReleaseCompatibility.verify_result(release_id, %RunnerResult{
               required_runner_release_id: malformed
             })

    assert {:error, :invalid_runner_release_identity} =
             RunnerReleaseCompatibility.verify_inspection_result(
               release_id,
               %RelationInspectionResult{required_runner_release_id: malformed}
             )
  end

  defp version(release_id) do
    manifest = FavnTestSupport.with_manifest_contract(%Manifest{}, release_id)
    {:ok, version} = Version.new(manifest, manifest_version_id: "release-bound-manifest")
    version
  end
end
