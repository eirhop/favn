defmodule FavnOrchestrator.Storage.JsonSafeTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Storage.JsonSafe

  defmodule ArbitraryStruct do
    defstruct [:name, :password, :metadata]
  end

  defmodule HTTPError do
    defstruct [:message, :status, :url, :token]
  end

  test "preserves JSON scalar nil booleans and numbers" do
    assert JsonSafe.data(nil) == nil
    assert JsonSafe.data(true) == true
    assert JsonSafe.data(false) == false
    assert JsonSafe.data(42) == 42
    assert JsonSafe.data(1.5) == 1.5
  end

  test "normalizes atom and string keys to string keys" do
    assert JsonSafe.data(%{:atom_key => :atom_value, "string_key" => "string_value"}) == %{
             "atom_key" => "atom_value",
             "string_key" => "string_value"
           }
  end

  test "normalizes tuples to lists except module name refs" do
    assert JsonSafe.data({:ok, 1, "two"}) == ["ok", 1, "two"]

    assert JsonSafe.data({Favn.SampleAsset, :daily_orders}) == %{
             "module" => "Elixir.Favn.SampleAsset",
             "name" => "daily_orders"
           }
  end

  test "normalizes module name refs through ref/1" do
    assert JsonSafe.ref({Favn.SampleAsset, :daily_orders}) == %{
             "module" => "Elixir.Favn.SampleAsset",
             "name" => "daily_orders"
           }

    assert JsonSafe.ref({"Favn.SampleAsset", "daily_orders"}) == nil
    assert JsonSafe.ref(:not_a_ref) == nil
  end

  test "normalizes arbitrary structs without leaking struct metadata" do
    normalized =
      JsonSafe.data(%ArbitraryStruct{
        name: :example,
        password: "super-secret",
        metadata: %{token: "token-value"}
      })

    assert normalized == %{
             "name" => "example",
             "password" => "[REDACTED]",
             "metadata" => %{"token" => "[REDACTED]"}
           }

    refute_struct_leak(normalized)
    assert_json_compatible!(normalized)
  end

  test "normalizes exceptions as structured sanitized errors" do
    normalized = JsonSafe.data(%RuntimeError{message: "password=swordfish"})

    assert normalized["kind"] == "error"
    assert normalized["type"] == "Elixir.RuntimeError"
    assert normalized["message"] == "password=[REDACTED]"
    assert normalized["redacted"] == true
    assert normalized["truncated"] == false
    assert is_binary(normalized["reason"])
    refute String.contains?(normalized["reason"], "swordfish")
    refute_struct_leak(normalized)
    assert_json_compatible!(normalized)
  end

  test "normalizes explicit error terms as structured sanitized maps" do
    normalized =
      JsonSafe.error(%{
        kind: :error,
        reason: {:bad_token, "abc123"},
        message: "token=abc123 failed"
      })

    assert normalized["kind"] == "error"
    assert normalized["type"] == "tuple"
    assert normalized["message"] == "token=[REDACTED] failed"
    assert normalized["redacted"] == true
    refute String.contains?(normalized["reason"], "abc123")
    assert_json_compatible!(normalized)
  end

  test "normalizes DateTimes to ISO 8601 strings" do
    {:ok, datetime, 0} = DateTime.from_iso8601("2026-05-05T10:11:12Z")

    assert JsonSafe.data(datetime) == "2026-05-05T10:11:12Z"
  end

  test "normalizes nested maps and lists" do
    normalized =
      JsonSafe.data(%{
        outer: [
          %{inner: [:ok, {1, 2}, {Favn.SampleAsset, :daily_orders}]}
        ]
      })

    assert normalized == %{
             "outer" => [
               %{
                 "inner" => [
                   "ok",
                   [1, 2],
                   %{"module" => "Elixir.Favn.SampleAsset", "name" => "daily_orders"}
                 ]
               }
             ]
           }

    assert_json_compatible!(normalized)
  end

  test "bounds strings by bytes without producing invalid UTF-8" do
    value = String.duplicate("å", 5_000)

    normalized = JsonSafe.data(value)

    assert byte_size(normalized) <= 8_192
    assert String.valid?(normalized)
    assert String.ends_with?(normalized, "...")
  end

  test "preserves safe runtime config diagnostics" do
    diagnostic = %{
      type: :missing_runtime_config,
      phase: :asset_runtime_config,
      message: "missing required asset runtime config",
      details: %{
        asset_ref: {MyApp.Assets.Raw, :asset},
        errors: [
          %{
            type: :missing_env,
            provider: :env,
            env: "MERCATUS_TOKEN",
            key: "MERCATUS_TOKEN",
            scope: :mercatus,
            field: :token,
            secret?: true,
            message: "missing_env MERCATUS_TOKEN"
          }
        ]
      }
    }

    assert JsonSafe.error(diagnostic) == %{
             "type" => "missing_runtime_config",
             "phase" => "asset_runtime_config",
             "message" => "missing required asset runtime config",
             "details" => %{
               "asset_ref" => %{"module" => "Elixir.MyApp.Assets.Raw", "name" => "asset"},
               "errors" => [
                 %{
                   "type" => "missing_env",
                   "provider" => "env",
                   "env" => "MERCATUS_TOKEN",
                   "key" => "MERCATUS_TOKEN",
                   "scope" => "mercatus",
                   "field" => "token",
                   "secret?" => true,
                   "message" => "missing_env MERCATUS_TOKEN"
                 }
               ]
             }
           }
  end

  test "preserves sanitized generic struct errors" do
    error = %HTTPError{
      message: "request failed token=abc123",
      status: 401,
      url: "https://user:pass@example.test/path",
      token: "abc123"
    }

    assert %{
             "kind" => "error",
             "type" => type,
             "message" => message,
             "reason" => reason,
             "redacted" => true,
             "truncated" => false
           } = JsonSafe.error(error)

    assert type == Atom.to_string(HTTPError)
    assert message =~ "request failed token=[REDACTED]"
    assert message =~ "status: 401"
    assert message =~ "type: FavnOrchestrator.Storage.JsonSafeTest.HTTPError"
    assert message =~ ~s(token: "[REDACTED]")
    assert message =~ ~s(url: "[REDACTED]")
    assert reason == message
    refute message =~ "abc123"
    refute message =~ "user:pass"
  end

  test "bounds map entries and nested depth" do
    many_entries = Map.new(1..60, &{"key_#{&1}", &1})
    deeply_nested = Enum.reduce(1..12, "leaf", fn index, acc -> %{level: index, child: acc} end)

    bounded_entries = JsonSafe.data(many_entries)
    bounded_depth = JsonSafe.data(deeply_nested)

    assert map_size(bounded_entries) == 50
    assert contains_value?(bounded_depth, "[TRUNCATED]")
    assert_json_compatible!(bounded_entries)
    assert_json_compatible!(bounded_depth)
  end

  test "redacts sensitive-looking keys" do
    sensitive_keys = ~w(password token secret credential authorization cookie dsn url)

    normalized =
      sensitive_keys
      |> Map.new(&{&1, "sensitive-value"})
      |> Map.put(:api_key, "sensitive-value")
      |> Map.put(:secret_flag, false)
      |> Map.put("nested", %{database_url: "postgres://user:pass@example/db"})
      |> JsonSafe.data()

    for key <- sensitive_keys do
      assert normalized[key] == "[REDACTED]"
    end

    assert normalized["api_key"] == "[REDACTED]"
    assert normalized["secret_flag"] == false
    assert normalized["nested"]["database_url"] == "[REDACTED]"
    assert_json_compatible!(normalized)
  end

  defp assert_json_compatible!(value) do
    assert {:ok, _json} = Jason.encode(value)
    value
  end

  defp refute_struct_leak(value) do
    refute contains_key?(value, "__struct__")
    refute contains_key?(value, :__struct__)
  end

  defp contains_key?(%{} = value, key) do
    Map.has_key?(value, key) or
      Enum.any?(value, fn {_child_key, child_value} -> contains_key?(child_value, key) end)
  end

  defp contains_key?(value, key) when is_list(value),
    do: Enum.any?(value, &contains_key?(&1, key))

  defp contains_key?(_value, _key), do: false

  defp contains_value?(%{} = value, expected) do
    Enum.any?(value, fn {_key, child_value} -> contains_value?(child_value, expected) end)
  end

  defp contains_value?(value, expected) when is_list(value),
    do: Enum.any?(value, &contains_value?(&1, expected))

  defp contains_value?(expected, expected), do: true
  defp contains_value?(_value, _expected), do: false
end
