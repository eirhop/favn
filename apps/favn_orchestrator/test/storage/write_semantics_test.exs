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
end
