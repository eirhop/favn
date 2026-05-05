defmodule FavnOrchestrator.Storage.RunEventCodecTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Storage.RunEventCodec

  defmodule UnexpectedEventError do
    defexception [:message, :password]
  end

  test "normalizes valid run events" do
    occurred_at = DateTime.utc_now()

    event = %{
      schema_version: 2,
      run_id: "run_event",
      sequence: 2,
      event_type: :run_updated,
      entity: :step,
      occurred_at: occurred_at,
      stage: 3,
      status: :running,
      manifest_version_id: "mv_1",
      manifest_content_hash: "hash_1",
      asset_ref: {MyApp.Asset, :asset},
      data: %{attempt: 1}
    }

    assert {:ok, normalized} = RunEventCodec.normalize("run_event", event)
    assert normalized.sequence == 2
    assert normalized.event_type == :run_updated
    assert normalized.schema_version == 2
    assert normalized.entity == :step
    assert normalized.stage == 3
    assert normalized.occurred_at == occurred_at
    assert normalized.data == %{"attempt" => 1}
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

  test "derives top-level asset_ref and stage from event data" do
    assert {:ok, normalized} =
             RunEventCodec.normalize("run_step", %{
               sequence: 2,
               event_type: :step_started,
               data: %{asset_ref: {MyApp.Asset, :asset}, stage: 1}
             })

    assert normalized.entity == :step
    assert normalized.asset_ref == {MyApp.Asset, :asset}
    assert normalized.stage == 1
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

  test "encodes run events as explicit JSON-safe DTOs" do
    event = %{
      run_id: "run_event_dto",
      sequence: 1,
      event_type: :step_failed,
      entity: :step,
      occurred_at: DateTime.utc_now(),
      status: :error,
      asset_ref: {MyApp.Asset, :asset},
      stage: 0,
      data: %{
        asset_ref: {MyApp.Asset, :asset},
        error: %UnexpectedEventError{message: "password=secret", password: "secret"},
        stacktrace: [{__MODULE__, :test, 1}]
      }
    }

    assert {:ok, normalized} = RunEventCodec.normalize("run_event_dto", event)
    assert {:ok, payload} = RunEventCodec.encode(normalized)

    decoded = Jason.decode!(payload)
    assert decoded["format"] == "favn.run_event.storage.v1"
    refute payload =~ "__type__"
    refute payload =~ "__struct__"
    refute payload =~ "password=secret"
    refute payload =~ ~s("tuple")
    refute payload =~ ~s("atom")

    assert {:ok, restored} = RunEventCodec.decode(payload)
    assert restored.event_type == :step_failed
    assert restored.entity == :step
    assert restored.asset_ref == {MyApp.Asset, :asset}
    assert restored.data["error"]["message"] == "[REDACTED]"
    assert restored.data["stacktrace"] == [[Atom.to_string(__MODULE__), "test", 1]]
  end
end
