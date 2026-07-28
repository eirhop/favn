defmodule FavnStoragePostgres.Types.JsonValueTest do
  use ExUnit.Case, async: true

  alias FavnStoragePostgres.Types.JsonValue

  test "accepts every native JSON shape" do
    values = [
      %{"message" => "failed", "details" => [1, true, nil]},
      %{message: "failed"},
      ["failed", %{"retryable" => false}],
      "upstream_blocked",
      42,
      4.2,
      true,
      false
    ]

    for value <- values do
      assert {:ok, ^value} = JsonValue.cast(value)
      assert {:ok, ^value} = JsonValue.load(value)
      assert {:ok, ^value} = JsonValue.dump(value)
    end
  end

  test "rejects values that PostgreSQL JSONB cannot encode" do
    for value <- [:upstream_blocked, {:error, :failed}, ~D[2026-07-27]] do
      assert :error = JsonValue.cast(value)
      assert :error = JsonValue.load(value)
      assert :error = JsonValue.dump(value)
    end

    assert :error = JsonValue.dump(%{"invalid" => :atom})
  end
end
