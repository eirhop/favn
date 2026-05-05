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
end
