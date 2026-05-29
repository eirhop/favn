defmodule FavnOrchestrator.Storage.AuditEventCodecTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Audit.Event
  alias FavnOrchestrator.Storage.AuditEventCodec

  test "round-trips audit event storage DTOs" do
    assert {:ok, event} =
             Event.new(%{
               id: "aud_test",
               occurred_at: ~U[2026-05-29 00:00:00Z],
               action: "operator.asset_run.submit",
               outcome: :accepted,
               actor_id: "act_1",
               session_id: "ses_1",
               source: :live_view,
               manifest_version_id: "mv_1",
               target_type: :asset,
               target_id: "asset:Elixir.MyApp.Assets.Gold:asset",
               resource_type: :run,
               resource_id: "run_1",
               payload: %{"refresh_mode" => "force_all"},
               request_context: %{},
               metadata: %{}
             })

    assert {:ok, encoded} = AuditEventCodec.encode(event)
    assert {:ok, decoded} = AuditEventCodec.decode(encoded)

    assert decoded == event
  end

  test "rejects unsupported schema versions precisely" do
    payload =
      Jason.encode!(%{
        "format" => "favn.audit.event.storage.v1",
        "schema_version" => 2,
        "id" => "aud_future"
      })

    assert {:error, {:unsupported_audit_event_schema_version, 2}} =
             AuditEventCodec.decode(payload)
  end
end
