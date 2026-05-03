defmodule FavnOrchestrator.Auth.ServiceTokensTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.Auth.ServiceTokens

  @token_a "alpha-credential-value-1234567890abcd"
  @token_b "bravo-credential-value-1234567890abcd"

  setup do
    previous_tokens = Application.get_env(:favn_orchestrator, :api_service_tokens)
    previous_token_env = Application.get_env(:favn_orchestrator, :api_service_tokens_env)

    on_exit(fn ->
      restore_env(:api_service_tokens, previous_tokens)
      restore_env(:api_service_tokens_env, previous_token_env)
    end)

    :ok
  end

  test "parses named service tokens into redacted token hashes" do
    assert {:ok, [config]} = ServiceTokens.from_env_string("favn_web:#{@token_a}")
    assert config.service_identity == "favn_web"
    assert config.enabled == true
    assert config.token_hash == ServiceTokens.hash_token(@token_a)
    refute inspect(config) =~ @token_a
  end

  test "rejects token-only blank duplicate and weak service token entries" do
    assert {:error, {:invalid_env, "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS", "identity:token"}} =
             ServiceTokens.from_env_string(@token_a)

    assert {:error, {:invalid_env, "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS", :blank_identity}} =
             ServiceTokens.from_env_string(":#{@token_a}")

    assert {:error, {:invalid_env, "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS", :duplicate_identity}} =
             ServiceTokens.from_env_string("favn_web:#{@token_a},favn_web:#{@token_b}")

    assert {:error, {:invalid_secret_env, "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS", :weak}} =
             ServiceTokens.from_env_string("favn_web:replace-with-32-plus-char-service-token")
  end

  test "runtime config uses raw env string only through service token parser" do
    Application.put_env(:favn_orchestrator, :api_service_tokens, [])
    Application.put_env(:favn_orchestrator, :api_service_tokens_env, "favn_web:#{@token_a}")

    assert {:ok, [%{service_identity: "favn_web"}]} = ServiceTokens.runtime_config()

    assert {:ok, "favn_web"} =
             ServiceTokens.authenticate(@token_a, ServiceTokens.configured_tokens())
  end

  test "runtime config rejects raw placeholder env values" do
    Application.put_env(:favn_orchestrator, :api_service_tokens, [])

    Application.put_env(
      :favn_orchestrator,
      :api_service_tokens_env,
      "favn_web:replace-with-32-plus-char-service-token"
    )

    assert {:error, {:invalid_secret_env, "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS", :weak}} =
             ServiceTokens.runtime_config()
  end

  test "raw application token config is validated consistently" do
    Application.put_env(:favn_orchestrator, :api_service_tokens, [
      [service_identity: "favn_web", token: @token_a, enabled: true]
    ])

    assert :ok = ServiceTokens.validate_runtime_config()

    Application.put_env(:favn_orchestrator, :api_service_tokens, [
      [
        service_identity: "favn_web",
        token: "replace-with-32-plus-char-service-token",
        enabled: true
      ]
    ])

    assert {:error, {:invalid_secret_env, "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS", :weak}} =
             ServiceTokens.validate_runtime_config()
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn_orchestrator, key)
  defp restore_env(key, value), do: Application.put_env(:favn_orchestrator, key, value)
end
