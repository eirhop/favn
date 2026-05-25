defmodule FavnOrchestrator.Storage.RunStateCodecTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage.RunStateCodec

  test "normalizes run state and refreshes snapshot hash" do
    run_state =
      RunState.new(
        id: "run_codec",
        manifest_version_id: "mv_codec",
        manifest_content_hash: "hash_codec",
        asset_ref: {MyApp.Asset, :asset}
      )

    tampered = %{run_state | snapshot_hash: "bad"}

    assert {:ok, normalized} = RunStateCodec.normalize(tampered)
    assert normalized.snapshot_hash == RunState.with_snapshot_hash(run_state).snapshot_hash
  end

  test "accepts backfill submit kinds and partial status" do
    run_state =
      RunState.new(
        id: "run_codec_backfill",
        manifest_version_id: "mv_codec",
        manifest_content_hash: "hash_codec",
        asset_ref: {MyApp.Asset, :asset},
        submit_kind: :backfill_pipeline
      )
      |> RunState.transition(status: :partial)

    assert run_state.submit_kind == :backfill_pipeline
    assert run_state.status == :partial

    assert {:ok, record} = RunStateCodec.to_record(run_state)
    assert {:ok, decoded} = RunStateCodec.from_record(record)
    assert decoded.submit_kind == :backfill_pipeline
    assert decoded.status == :partial

    asset_run_state =
      RunState.new(
        id: "run_codec_backfill_asset",
        manifest_version_id: "mv_codec",
        manifest_content_hash: "hash_codec",
        asset_ref: {MyApp.Asset, :asset},
        submit_kind: :backfill_asset
      )

    assert asset_run_state.submit_kind == :backfill_asset
  end

  test "terminal detection accepts JSON-restored metadata values" do
    run_state =
      RunState.new(
        id: "run_codec_terminal_metadata",
        manifest_version_id: "mv_codec",
        manifest_content_hash: "hash_codec",
        asset_ref: {MyApp.Asset, :asset},
        metadata: %{"terminal_event_type" => "run_failed"}
      )
      |> RunState.transition(status: :error, result: nil)

    assert RunState.finalized?(run_state)
    refute RunState.execution_admissible?(run_state)
  end

  test "normalizes legacy finalized snapshots that predate terminal metadata" do
    run_state =
      RunState.new(
        id: "run_codec_legacy_terminal",
        manifest_version_id: "mv_codec",
        manifest_content_hash: "hash_codec",
        asset_ref: {MyApp.Asset, :asset}
      )
      |> RunState.transition(
        status: :error,
        result: %{status: :error, asset_results: [], metadata: %{}}
      )

    refute RunState.finalized?(run_state)

    assert {:ok, normalized} = RunStateCodec.from_record(Map.from_struct(run_state))
    assert normalized.status == :error
    assert normalized.metadata[:terminal_event_type] == :run_failed
    assert RunState.finalized?(normalized)
    refute RunState.execution_admissible?(normalized)
  end

  test "does not finalize active intermediate failure snapshots without terminal result" do
    run_state =
      RunState.new(
        id: "run_codec_intermediate_failure",
        manifest_version_id: "mv_codec",
        manifest_content_hash: "hash_codec",
        asset_ref: {MyApp.Asset, :asset}
      )
      |> RunState.transition(status: :error, error: %{type: :step_failed})

    assert RunState.terminal_status?(run_state.status)
    refute RunState.finalized?(run_state)

    assert {:ok, normalized} = RunStateCodec.from_record(Map.from_struct(run_state))
    refute RunState.finalized?(normalized)
    assert RunState.execution_admissible?(normalized)
  end

  test "rejects invalid run identity" do
    run_state =
      RunState.new(
        id: "run_codec_invalid",
        manifest_version_id: "mv_codec",
        manifest_content_hash: "hash_codec",
        asset_ref: {MyApp.Asset, :asset}
      )

    assert {:error, :invalid_run_id} = RunStateCodec.normalize(%{run_state | id: nil})

    assert {:error, :invalid_manifest_version_id} =
             RunStateCodec.normalize(%{run_state | manifest_version_id: ""})

    assert {:error, :invalid_manifest_content_hash} =
             RunStateCodec.normalize(%{run_state | manifest_content_hash: nil})

    assert {:error, :invalid_asset_ref} =
             RunStateCodec.normalize(%{run_state | asset_ref: :invalid})

    assert {:error, :invalid_event_seq} = RunStateCodec.normalize(%{run_state | event_seq: -1})
  end
end
