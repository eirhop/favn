defmodule FavnOrchestrator.Storage.JsonSafeTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Storage.JsonSafe

  test "preserves JSON scalar nil and booleans" do
    assert JsonSafe.data(nil) == nil
    assert JsonSafe.data(true) == true
    assert JsonSafe.data(false) == false

    assert JsonSafe.data(%{nil_value: nil, true_value: true, false_value: false}) == %{
             "nil_value" => nil,
             "true_value" => true,
             "false_value" => false
           }
  end

  test "bounds strings by bytes without producing invalid UTF-8" do
    value = String.duplicate("å", 5_000)

    normalized = JsonSafe.data(value)

    assert byte_size(normalized) <= 8_192
    assert String.valid?(normalized)
    assert String.ends_with?(normalized, "...")
  end
end
