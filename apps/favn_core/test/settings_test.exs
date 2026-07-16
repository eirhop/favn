defmodule Favn.SettingsTest do
  use ExUnit.Case, async: true

  alias Favn.Settings

  test "normalizes JSON-like values while preserving ergonomic top-level keys" do
    assert Settings.normalize!(
             source: :orders,
             enabled: true,
             request: %{headers: %{accept: :json}, retries: [1, 2, nil]}
           ) == %{
             source: "orders",
             enabled: true,
             request: %{
               "headers" => %{"accept" => "json"},
               "retries" => [1, 2, nil]
             }
           }
  end

  test "shallow-merges declarations and lets nil override" do
    assert Settings.merge_all!([
             [source: "orders", request: %{path: "/v1"}],
             [source: nil, request: %{path: "/v2"}]
           ]) == %{source: nil, request: %{"path" => "/v2"}}
  end

  test "rejects unsafe values and bounded payload violations" do
    assert_raise ArgumentError, ~r/JSON-like/, fn ->
      Settings.normalize!(callback: fn -> :ok end)
    end

    too_many = Map.new(1..(Settings.max_entries() + 1), &{String.to_atom("key_#{&1}"), &1})
    assert_raise ArgumentError, ~r/at most 128/, fn -> Settings.normalize!(too_many) end

    assert_raise ArgumentError, ~r/at most 65536 bytes/, fn ->
      Settings.normalize!(payload: String.duplicate("x", Settings.max_encoded_bytes()))
    end
  end

  test "rejects keys that cannot roundtrip through persisted manifests" do
    assert_raise ArgumentError, ~r/must be an identifier/, fn ->
      Settings.normalize!(%{:"bad-key" => true})
    end

    assert_raise ArgumentError, ~r/at most 128 bytes/, fn ->
      key = String.to_atom("k" <> String.duplicate("x", Settings.max_key_bytes()))
      Settings.normalize!(%{key => true})
    end

    assert_raise ArgumentError, ~r/duplicate normalized key/, fn ->
      Settings.normalize!(request: %{:path => "/v1", "path" => "/v2"})
    end
  end
end
