defmodule FavnOrchestrator.Storage.ExecutionLeaseCodecTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Storage.ExecutionLeaseCodec

  test "normalizes supported atom and string scope kinds" do
    assert {:ok, %{kind: :run, key: "run-1", limit: 1}} =
             ExecutionLeaseCodec.normalize_scope(%{kind: :run, key: "run-1", limit: 1})

    assert {:ok, %{kind: :pool, key: "default", limit: 4}} =
             ExecutionLeaseCodec.normalize_scope(%{
               "kind" => "pool",
               "key" => "default",
               "limit" => 4
             })
  end

  test "rejects unsupported scope kinds" do
    assert {:error, {:invalid_execution_lease_field, :kind}} =
             ExecutionLeaseCodec.normalize_scope(%{kind: :invented, key: "run-1", limit: 1})
  end

  test "rejects empty scope keys" do
    assert {:error, {:invalid_execution_lease_field, :key}} =
             ExecutionLeaseCodec.normalize_scope(%{kind: :run, key: "", limit: 1})
  end

  test "rejects non-positive scope limits" do
    assert {:error, {:invalid_execution_lease_field, :limit}} =
             ExecutionLeaseCodec.normalize_scope(%{kind: :run, key: "run-1", limit: 0})
  end
end
