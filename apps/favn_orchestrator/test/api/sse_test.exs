defmodule FavnOrchestrator.API.SSETest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.API.SSE

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
end
