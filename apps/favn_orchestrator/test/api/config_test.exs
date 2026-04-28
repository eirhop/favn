defmodule FavnOrchestrator.API.ConfigTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.API.Config

  test "validate/0 requires service tokens when API server enabled" do
    previous_server = Application.get_env(:favn_orchestrator, :api_server)
    previous_tokens = Application.get_env(:favn_orchestrator, :api_service_tokens)

    Application.put_env(:favn_orchestrator, :api_server, enabled: true)
    Application.put_env(:favn_orchestrator, :api_service_tokens, [])

    on_exit(fn ->
      restore_env(:favn_orchestrator, :api_server, previous_server)
      restore_env(:favn_orchestrator, :api_service_tokens, previous_tokens)
    end)

    assert {:error, {:invalid_api_config, :missing_service_tokens}} = Config.validate()
  end

  test "validate/0 allows disabled API server without tokens" do
    previous_server = Application.get_env(:favn_orchestrator, :api_server)
    previous_tokens = Application.get_env(:favn_orchestrator, :api_service_tokens)

    Application.put_env(:favn_orchestrator, :api_server, enabled: false)
    Application.put_env(:favn_orchestrator, :api_service_tokens, [])

    on_exit(fn ->
      restore_env(:favn_orchestrator, :api_server, previous_server)
      restore_env(:favn_orchestrator, :api_service_tokens, previous_tokens)
    end)

    assert :ok = Config.validate()
  end

  test "validate/0 rejects invalid bind_ip when API server enabled" do
    previous_server = Application.get_env(:favn_orchestrator, :api_server)
    previous_tokens = Application.get_env(:favn_orchestrator, :api_service_tokens)

    Application.put_env(:favn_orchestrator, :api_server,
      enabled: true,
      bind_ip: "not-an-ip"
    )

    Application.put_env(:favn_orchestrator, :api_service_tokens, ["token"])

    on_exit(fn ->
      restore_env(:favn_orchestrator, :api_server, previous_server)
      restore_env(:favn_orchestrator, :api_service_tokens, previous_tokens)
    end)

    assert {:error, {:invalid_api_config, {:invalid_bind_ip, "not-an-ip"}}} = Config.validate()
  end

  test "bind_ip/1 accepts IPv4 tuples and strings" do
    assert {:ok, {127, 0, 0, 1}} = Config.bind_ip(bind_ip: {127, 0, 0, 1})
    assert {:ok, {127, 0, 0, 1}} = Config.bind_ip(bind_ip: "127.0.0.1")
    assert {:error, {:invalid_bind_ip, "127.0.0.999"}} = Config.bind_ip(bind_ip: "127.0.0.999")
  end

  defp restore_env(app, key, nil), do: Application.delete_env(app, key)
  defp restore_env(app, key, value), do: Application.put_env(app, key, value)
end
