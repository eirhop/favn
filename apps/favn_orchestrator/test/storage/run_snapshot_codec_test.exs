defmodule FavnOrchestrator.Storage.RunSnapshotCodecTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Version
  alias Favn.Run.AssetResult
  alias FavnOrchestrator.Projector
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

  test "free-form params and trigger ref-shaped maps remain JSON data" do
    version = manifest_version("mv_run_snapshot_free_form", __MODULE__.Asset)

    free_form_ref = %{"module" => Atom.to_string(__MODULE__.Asset), "name" => "asset"}

    run =
      RunState.new(
        id: "run_snapshot_free_form",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: {__MODULE__.Asset, :asset},
        params: %{"payload" => free_form_ref},
        trigger: %{"payload" => free_form_ref}
      )

    assert {:ok, payload} = RunSnapshotCodec.encode_run(run)
    assert {:ok, manifest_record} = ManifestCodec.to_record(version)

    assert {:ok, restored} =
             RunSnapshotCodec.decode_run(
               %{run_blob: payload, manifest_version_id: version.manifest_version_id},
               manifest_record
             )

    assert restored.params == %{"payload" => free_form_ref}
    assert restored.trigger == %{"payload" => free_form_ref}
  end

  test "pipeline replay metadata remains usable after DTO roundtrip" do
    version = manifest_version("mv_run_snapshot_pipeline_metadata", __MODULE__.Asset)

    run =
      RunState.new(
        id: "run_snapshot_pipeline_metadata",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: {__MODULE__.Asset, :asset},
        metadata: %{
          replay_submit_kind: :pipeline,
          replay_mode: :exact_replay,
          pipeline_submit_ref: __MODULE__.Asset,
          pipeline_target_refs: [{__MODULE__.Asset, :asset}],
          pipeline_context: %{
            id: "pipeline_1",
            name: "daily",
            run_kind: :pipeline,
            resolved_refs: [{__MODULE__.Asset, :asset}],
            deps: :all
          }
        },
        submit_kind: :rerun
      )

    assert {:ok, payload} = RunSnapshotCodec.encode_run(run)
    assert {:ok, manifest_record} = ManifestCodec.to_record(version)

    assert {:ok, restored} =
             RunSnapshotCodec.decode_run(
               %{run_blob: payload, manifest_version_id: version.manifest_version_id},
               manifest_record
             )

    assert restored.metadata.replay_submit_kind == :pipeline
    assert restored.metadata.replay_mode == :exact_replay
    assert restored.metadata.pipeline_submit_ref == __MODULE__.Asset
    assert restored.metadata.pipeline_target_refs == [{__MODULE__.Asset, :asset}]
    assert restored.metadata.pipeline_context.resolved_refs == [{__MODULE__.Asset, :asset}]

    projected = Projector.project_run(restored)
    assert projected.replay_mode == :exact_replay
    assert projected.pipeline.resolved_refs == [{__MODULE__.Asset, :asset}]
    assert projected.submit_ref == __MODULE__.Asset
  end

  test "operational in-flight execution ids remain atom-keyed after DTO roundtrip" do
    version = manifest_version("mv_run_snapshot_in_flight", __MODULE__.Asset)

    run =
      RunState.new(
        id: "run_snapshot_in_flight",
        manifest_version_id: version.manifest_version_id,
        manifest_content_hash: version.content_hash,
        asset_ref: {__MODULE__.Asset, :asset},
        metadata: %{in_flight_execution_ids: ["exec_1", "exec_2"]}
      )

    assert {:ok, payload} = RunSnapshotCodec.encode_run(run)
    assert {:ok, manifest_record} = ManifestCodec.to_record(version)

    assert {:ok, restored} =
             RunSnapshotCodec.decode_run(
               %{run_blob: payload, manifest_version_id: version.manifest_version_id},
               manifest_record
             )

    assert restored.metadata.in_flight_execution_ids == ["exec_1", "exec_2"]
    refute Map.has_key?(restored.metadata, "in_flight_execution_ids")
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
    assert restored.error["message"] == "database password=[REDACTED] token=[REDACTED] failed"

    assert [%AssetResult{error: asset_error, attempts: [attempt]}] = restored.result.asset_results

    assert asset_error["message"] == "database password=[REDACTED] token=[REDACTED] failed"
    attempt_error = Map.get(attempt, "error") || Map.fetch!(attempt, :error)
    assert attempt_error["message"] == "database password=[REDACTED] token=[REDACTED] failed"
  end

  test "restores nil asset result datetimes as nil" do
    version = manifest_version("mv_run_snapshot_nil_asset_result_datetimes", __MODULE__.Asset)
    now = DateTime.utc_now()

    asset_result = %AssetResult{
      ref: {__MODULE__.Asset, :asset},
      stage: 0,
      status: :running,
      started_at: now,
      finished_at: nil,
      duration_ms: 0,
      error: nil,
      attempt_count: 1,
      max_attempts: 1,
      attempts: [],
      next_retry_at: nil
    }

    run =
      "run_snapshot_nil_asset_result_datetimes"
      |> run_state(version, __MODULE__.Asset)
      |> RunState.transition(status: :running, result: %{asset_results: [asset_result]})

    assert {:ok, payload} = RunSnapshotCodec.encode_run(run)
    assert {:ok, manifest_record} = ManifestCodec.to_record(version)

    assert {:ok, restored} =
             RunSnapshotCodec.decode_run(
               %{run_blob: payload, manifest_version_id: version.manifest_version_id},
               manifest_record
             )

    assert [%AssetResult{} = restored_result] = restored.result.asset_results
    assert restored_result.finished_at == nil
    assert restored_result.next_retry_at == nil
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

  test "rejects stale manifest content hash before trusting manifest refs" do
    version = manifest_version("mv_run_snapshot_stale_manifest", __MODULE__.Asset)
    run = run_state("run_snapshot_stale_manifest", version, __MODULE__.Asset)

    assert {:ok, payload} = RunSnapshotCodec.encode_run(run)
    assert {:ok, manifest_record} = ManifestCodec.to_record(version)

    stale_manifest_json =
      manifest_record.manifest_json
      |> Jason.decode!()
      |> put_in(["metadata", "changed"], true)
      |> Jason.encode!()

    stale_manifest_record = %{manifest_record | manifest_json: stale_manifest_json}

    assert {:error, {:manifest_content_hash_mismatch, _, _}} =
             RunSnapshotCodec.decode_run(
               %{run_blob: payload, manifest_version_id: version.manifest_version_id},
               stale_manifest_record
             )
  end

  test "rejects run manifest content hash mismatch" do
    version = manifest_version("mv_run_snapshot_content_hash_mismatch", __MODULE__.Asset)
    run = run_state("run_snapshot_content_hash_mismatch", version, __MODULE__.Asset)

    assert {:ok, payload} = RunSnapshotCodec.encode_run(run)
    assert {:ok, manifest_record} = ManifestCodec.to_record(version)

    tampered =
      payload
      |> Jason.decode!()
      |> Map.put("manifest_content_hash", String.duplicate("f", 64))
      |> Jason.encode!()

    assert {:error, {:run_manifest_content_hash_mismatch, version_hash, other_hash}} =
             RunSnapshotCodec.decode_run(
               %{run_blob: tampered, manifest_version_id: version.manifest_version_id},
               manifest_record
             )

    assert version_hash == version.content_hash
    assert other_hash == String.duplicate("f", 64)
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
