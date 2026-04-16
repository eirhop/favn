defmodule FavnOrchestrator.Storage.RunEventCodecTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Storage.RunEventCodec

  test "normalizes valid run events" do
    occurred_at = DateTime.utc_now()

    event = %{
      run_id: "run_event",
      sequence: 2,
      event_type: :run_updated,
      occurred_at: occurred_at,
      status: :running,
      manifest_version_id: "mv_1",
      manifest_content_hash: "hash_1",
      asset_ref: {MyApp.Asset, :asset},
      data: %{attempt: 1}
    }

    assert {:ok, normalized} = RunEventCodec.normalize("run_event", event)
    assert normalized.sequence == 2
    assert normalized.event_type == :run_updated
    assert normalized.occurred_at == occurred_at
    assert normalized.data == %{attempt: 1}
  end

  test "accepts ISO8601 timestamps" do
    now = DateTime.utc_now() |> DateTime.to_iso8601()

    assert {:ok, normalized} =
             RunEventCodec.normalize("run_iso", %{
               sequence: 1,
               event_type: :run_started,
               occurred_at: now
             })

    assert %DateTime{} = normalized.occurred_at
  end

  test "rejects invalid run events" do
    assert {:error, {:invalid_run_event_field, :sequence, 0}} =
             RunEventCodec.normalize("run_1", %{sequence: 0, event_type: :run_started})

    assert {:error, {:invalid_run_event_field, :run_id, "other"}} =
             RunEventCodec.normalize("run_1", %{
               sequence: 1,
               event_type: :run_started,
               run_id: "other"
             })

    assert {:error, {:invalid_run_event_field, :event_type, nil}} =
             RunEventCodec.normalize("run_1", %{sequence: 1, event_type: nil})

    assert {:error, {:invalid_run_event_field, :occurred_at, 1}} =
             RunEventCodec.normalize("run_1", %{
               sequence: 1,
               event_type: :run_started,
               occurred_at: 1
             })
  end
end
