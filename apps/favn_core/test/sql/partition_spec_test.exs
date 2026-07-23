defmodule Favn.SQL.PartitionSpecTest do
  use ExUnit.Case, async: true

  alias Favn.SQL.PartitionSpec

  test "normalizes ordered identity and transformed partition keys" do
    assert %PartitionSpec{
             keys: [
               %{column: "tenant_id", transform: :identity, bucket_count: nil},
               %{column: "occurred_at", transform: :year, bucket_count: nil},
               %{column: "occurred_at", transform: :month, bucket_count: nil},
               %{column: "account_id", transform: :bucket, bucket_count: 32}
             ]
           } =
             PartitionSpec.normalize!([
               :tenant_id,
               {:year, :occurred_at},
               {:month, "occurred_at"},
               {:bucket, 32, :account_id}
             ])
  end

  test "rehydrates the canonical manifest shape" do
    value = %{
      "keys" => [
        %{"column" => "occurred_at", "transform" => "day", "bucket_count" => nil},
        %{"column" => "account_id", "transform" => "bucket", "bucket_count" => 8}
      ]
    }

    assert PartitionSpec.from_value!(value) ==
             PartitionSpec.normalize!([{:day, :occurred_at}, {:bucket, 8, :account_id}])
  end

  test "rejects noncanonical and malformed manifest key containers cleanly" do
    assert_raise ArgumentError, ~r/keys must be a non-empty list/, fn ->
      PartitionSpec.from_value!(%{
        "keys" => %{"column" => "tenant_id", "transform" => "identity"}
      })
    end

    assert_raise ArgumentError, ~r/keys must be a non-empty list/, fn ->
      PartitionSpec.normalize!(%PartitionSpec{keys: nil})
    end
  end

  test "rejects empty, duplicate, raw, and invalid bucket declarations" do
    assert_raise ArgumentError, ~r/at least one/, fn ->
      PartitionSpec.normalize!([])
    end

    assert_raise ArgumentError, ~r/duplicate/, fn ->
      PartitionSpec.normalize!([:tenant_id, :tenant_id])
    end

    assert_raise ArgumentError, ~r/invalid partition key/, fn ->
      PartitionSpec.normalize!([{:sql, "year(occurred_at)"}])
    end

    assert_raise ArgumentError, ~r/bucket count/, fn ->
      PartitionSpec.normalize!([{:bucket, 0, :account_id}])
    end
  end
end
