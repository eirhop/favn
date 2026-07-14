defmodule FavnOrchestrator.API.SSETest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.API.SSE
  alias FavnOrchestrator.API.SSE.Cursor
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

  test "parses global and run cursors without accepting another run's cursor" do
    assert {:ok, nil} = Cursor.global(nil)
    assert {:ok, 12} = Cursor.global(" global:12 ")
    assert {:ok, 0} = Cursor.run("", "run-1")
    assert {:ok, 4} = Cursor.run("run:run-1:4", "run-1")
    assert {:error, :cursor_invalid} = Cursor.run("run:run-2:4", "run-1")
  end

  test "distinguishes unsafe headers from invalid cursor structure" do
    assert {:error, :invalid_last_event_id} = Cursor.global("global:1\n\nevent:forged")
    assert {:error, :invalid_last_event_id} = Cursor.global(String.duplicate("a", 129))
    assert {:error, :cursor_invalid} = Cursor.global("run:run-1:4")
    assert {:error, :cursor_invalid} = Cursor.global("global:0")
  end

  test "closes a live stream rather than advancing past an unsequenced global event" do
    assert SSE.delivery_error_action(:missing_global_sequence) == :close
    assert SSE.delivery_error_action(:temporary_chunk_failure) == :continue
  end
end
