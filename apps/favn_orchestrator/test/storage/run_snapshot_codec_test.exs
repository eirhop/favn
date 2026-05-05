defmodule FavnOrchestrator.Storage.RunSnapshotCodecTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Version
  alias Favn.Run.AssetResult
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage.ManifestCodec
  alias FavnOrchestrator.Storage.RunSnapshotCodec

  defmodule UnexpectedRunnerError do
    defexception [:message, :token]
  end

  test "encodes run snapshots as explicit JSON-safe DTOs" do
    version = manifest_version("mv_run_snapshot_dto", __MODULE__.Asset)
    run = run_state("run_snapshot_dto", version, __MODULE__.Asset)

    assert {:ok, payload} = RunSnapshotCodec.encode_run(run)
    decoded = Jason.decode!(payload)

    assert decoded["format"] == "favn.run_snapshot.storage.v1"

    assert decoded["asset_ref"] == %{
             "module" => Atom.to_string(__MODULE__.Asset),
             "name" => "asset"
           }

    refute payload =~ "__type__"
    refute payload =~ "__struct__"
    refute payload =~ ~s("tuple")
    refute payload =~ ~s("atom")

    assert {:ok, manifest_record} = ManifestCodec.to_record(version)

    assert {:ok, restored} =
             RunSnapshotCodec.decode_run(
               %{run_blob: payload, manifest_version_id: version.manifest_version_id},
               manifest_record
             )

    assert restored.id == run.id
    assert restored.asset_ref == {__MODULE__.Asset, :asset}
    assert restored.status == :pending
  end

  test "normalizes unexpected exception structs before persistence" do
    version = manifest_version("mv_run_snapshot_exception", __MODULE__.Asset)

    error = %UnexpectedRunnerError{
      message: "database password=super-secret token=secret-token failed",
      token: "secret-token"
    }

    now = DateTime.utc_now()

    asset_result = %AssetResult{
      ref: {__MODULE__.Asset, :asset},
      stage: 0,
      status: :error,
      started_at: now,
      finished_at: now,
      duration_ms: 1,
      error: error,
      attempt_count: 1,
      max_attempts: 1,
      attempts: [%{attempt: 1, status: :error, error: error}]
    }

    run =
      "run_snapshot_exception"
      |> run_state(version, __MODULE__.Asset)
      |> RunState.transition(
        status: :error,
        error: error,
        result: %{status: :error, asset_results: [asset_result], metadata: %{password: "secret"}}
      )

    assert {:ok, payload} = RunSnapshotCodec.encode_run(run)
    refute payload =~ "super-secret"
    refute payload =~ "secret-token"
    refute payload =~ "__struct__"

    assert {:ok, manifest_record} = ManifestCodec.to_record(version)

    assert {:ok, restored} =
             RunSnapshotCodec.decode_run(
               %{run_blob: payload, manifest_version_id: version.manifest_version_id},
               manifest_record
             )

    assert restored.status == :error
    assert restored.error["type"] == Atom.to_string(UnexpectedRunnerError)
    assert restored.error["message"] == "[REDACTED]"

    assert [%AssetResult{error: asset_error, attempts: [attempt]}] = restored.result.asset_results

    assert asset_error["message"] == "[REDACTED]"
    attempt_error = Map.get(attempt, "error") || Map.fetch!(attempt, :error)
    assert attempt_error["message"] == "[REDACTED]"
  end

  test "rejects refs that are not present in the associated manifest" do
    version = manifest_version("mv_run_snapshot_bad_ref", __MODULE__.Asset)
    run = run_state("run_snapshot_bad_ref", version, __MODULE__.Asset)

    assert {:ok, payload} = RunSnapshotCodec.encode_run(run)
    assert {:ok, manifest_record} = ManifestCodec.to_record(version)

    tampered =
      payload
      |> Jason.decode!()
      |> put_in(["asset_ref", "module"], "Elixir.Unknown.Asset")
      |> Jason.encode!()

    assert {:error, {:unknown_atom, "Elixir.Unknown.Asset"}} =
             RunSnapshotCodec.decode_run(
               %{run_blob: tampered, manifest_version_id: version.manifest_version_id},
               manifest_record
             )
  end

  defp manifest_version(manifest_version_id, module) do
    manifest = %Manifest{
      assets: [%Asset{ref: {module, :asset}, module: module, name: :asset}]
    }

    {:ok, version} = Version.new(manifest, manifest_version_id: manifest_version_id)
    version
  end

  defp run_state(run_id, version, module) do
    RunState.new(
      id: run_id,
      manifest_version_id: version.manifest_version_id,
      manifest_content_hash: version.content_hash,
      asset_ref: {module, :asset},
      target_refs: [{module, :asset}]
    )
  end
end
