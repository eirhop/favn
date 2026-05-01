defmodule FavnOrchestrator.Storage.WriteSemanticsTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Storage.WriteSemantics

  test "returns :insert when no existing snapshot exists" do
    assert :insert = WriteSemantics.decide(nil, nil, 1, "hash")
  end

  test "returns :replace for monotonic event sequence increases" do
    assert :replace = WriteSemantics.decide(1, "hash_a", 2, "hash_b")
  end

  test "returns stale_write when incoming event sequence regresses" do
    assert {:error, :stale_write} = WriteSemantics.decide(2, "hash_a", 1, "hash_b")
  end

  test "returns :idempotent for same event sequence and snapshot hash" do
    assert :idempotent = WriteSemantics.decide(2, "hash_a", 2, "hash_a")
  end

  test "returns conflicting_snapshot for same event sequence with different hash" do
    assert {:error, :conflicting_snapshot} = WriteSemantics.decide(2, "hash_a", 2, "hash_b")
  end

  test "inserts a run event when no event exists for the sequence" do
    assert :insert = WriteSemantics.decide_run_event_append(nil, %{sequence: 1})
  end

  test "returns :idempotent for an identical run event sequence write" do
    event = %{run_id: "run_1", sequence: 1, event_type: :run_started}

    assert :idempotent = WriteSemantics.decide_run_event_append(event, event)
  end

  test "returns conflicting_event_sequence for same sequence with different event content" do
    existing = %{run_id: "run_1", sequence: 1, event_type: :run_started}
    incoming = %{existing | event_type: :run_updated}

    assert {:error, :conflicting_event_sequence} =
             WriteSemantics.decide_run_event_append(existing, incoming)
  end
end
