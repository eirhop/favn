defmodule FavnOrchestrator.API.ConfigTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.API.Config

  test "validate/0 requires service tokens when API server enabled" do
    previous_server = Application.get_env(:favn_orchestrator, :api_server)
    previous_tokens = Application.get_env(:favn_orchestrator, :api_service_tokens)
    previous_token_env = Application.get_env(:favn_orchestrator, :api_service_tokens_env)

    Application.put_env(:favn_orchestrator, :api_server, enabled: true)
    Application.put_env(:favn_orchestrator, :api_service_tokens, [])
    Application.delete_env(:favn_orchestrator, :api_service_tokens_env)

    on_exit(fn ->
      restore_env(:favn_orchestrator, :api_server, previous_server)
      restore_env(:favn_orchestrator, :api_service_tokens, previous_tokens)
      restore_env(:favn_orchestrator, :api_service_tokens_env, previous_token_env)
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

    Application.put_env(:favn_orchestrator, :api_service_tokens, [
      [service_identity: "favn_web", token: "token", enabled: true]
    ])

    on_exit(fn ->
      restore_env(:favn_orchestrator, :api_server, previous_server)
      restore_env(:favn_orchestrator, :api_service_tokens, previous_tokens)
    end)

    assert {:error, {:invalid_api_config, {:invalid_bind_ip, "not-an-ip"}}} = Config.validate()
  end

  test "validate/0 rejects invalid manifest-publication limits when API server enabled" do
    previous_server = Application.get_env(:favn_orchestrator, :api_server)
    previous_tokens = Application.get_env(:favn_orchestrator, :api_service_tokens)
    previous_manifest = Application.get_env(:favn_orchestrator, :manifest_publication)

    Application.put_env(:favn_orchestrator, :api_server, enabled: true)

    Application.put_env(:favn_orchestrator, :api_service_tokens, [
      [service_identity: "favn_web", token: "token", enabled: true]
    ])

    Application.put_env(:favn_orchestrator, :manifest_publication, compressed_limit_bytes: 0)

    on_exit(fn ->
      restore_env(:favn_orchestrator, :api_server, previous_server)
      restore_env(:favn_orchestrator, :api_service_tokens, previous_tokens)
      restore_env(:favn_orchestrator, :manifest_publication, previous_manifest)
    end)

    assert {:error,
            {:invalid_api_config,
             {:invalid_manifest_publication_limit, :compressed_limit_bytes, 0, _maximum}}} =
             Config.validate()
  end

  test "bind_ip/1 accepts IPv4 tuples and strings" do
    assert {:ok, {127, 0, 0, 1}} = Config.bind_ip([])
    assert {:ok, {127, 0, 0, 1}} = Config.bind_ip(bind_ip: {127, 0, 0, 1})
    assert {:ok, {127, 0, 0, 1}} = Config.bind_ip(bind_ip: "127.0.0.1")
    assert {:error, {:invalid_bind_ip, "127.0.0.999"}} = Config.bind_ip(bind_ip: "127.0.0.999")
  end

  test "server_options/1 defaults to loopback and validates the port" do
    assert {:ok, default_options} = Config.server_options([])
    assert default_options[:port] == 4101
    assert default_options[:ip] == {127, 0, 0, 1}
    assert default_options[:thousand_island_options][:num_connections] == 1_024
    assert default_options[:thousand_island_options][:read_timeout] == 60_000
    assert default_options[:thousand_island_options][:shutdown_timeout] == 5_000

    assert {:ok, explicit_options} = Config.server_options(host: "0.0.0.0", port: 4444)
    assert explicit_options[:port] == 4444
    assert explicit_options[:ip] == {0, 0, 0, 0}

    assert {:error, {:invalid_port, 0}} = Config.server_options(port: 0)
    assert {:error, {:invalid_port, "4101"}} = Config.server_options(port: "4101")
  end

  defp restore_env(app, key, nil), do: Application.delete_env(app, key)
  defp restore_env(app, key, value), do: Application.put_env(app, key, value)
end
