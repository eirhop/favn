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

  defp restore_env(app, key, nil), do: Application.delete_env(app, key)
  defp restore_env(app, key, value), do: Application.put_env(app, key, value)
end
