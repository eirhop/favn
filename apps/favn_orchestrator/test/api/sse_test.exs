defmodule FavnOrchestrator.API.SSETest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.API.SSE
  alias FavnOrchestrator.Storage.RunEventCodec

  test "encodes valid event and id fields" do
    assert {:ok, "run_updated"} = SSE.field(:event, :run_updated)
    assert {:ok, "custom.run:updated-1"} = SSE.field(:event, "custom.run:updated-1")
    assert {:ok, "run:run_1:10"} = SSE.field(:id, "run:run_1:10")
  end

  test "rejects CR LF and forged SSE fields" do
    for value <- ["run\nupdated", "run\rupdated", "run_updated\n\nid: forged\nevent: forged"] do
      assert {:error, {:invalid_sse_field, :event, ^value}} = SSE.field(:event, value)
    end
  end

  test "run event normalization and SSE reject the same forged payload" do
    event_type = "run_updated\n\nid: forged\nevent: forged"

    assert {:error, {:invalid_run_event_field, :event_type, ^event_type}} =
             RunEventCodec.normalize("run_forged_sse", %{
               sequence: 1,
               event_type: event_type,
               occurred_at: DateTime.utc_now()
             })

    assert {:error, {:invalid_sse_field, :event, ^event_type}} = SSE.field(:event, event_type)
  end
end
