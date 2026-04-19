defmodule Favn.Dev.ConfigTest do
  use ExUnit.Case, async: true

  alias Favn.Dev.Config

  test "resolve/1 returns defaults" do
    config = Config.resolve([])

    assert config.storage == :memory
    assert config.sqlite_path == ".favn/data/orchestrator.sqlite3"
    assert config.orchestrator_api_enabled == true
    assert config.orchestrator_port == 4101
    assert config.web_port == 4173
    assert config.orchestrator_base_url == "http://127.0.0.1:4101"
    assert config.web_base_url == "http://127.0.0.1:4173"
    assert config.service_token == nil
    assert config.web_session_secret == nil
  end

  test "resolve/1 applies runtime overrides" do
    config =
      Config.resolve(
        storage: :sqlite,
        sqlite_path: ".favn/data/dev.sqlite",
        orchestrator_port: 4201,
        web_port: 4273,
        service_token: "dev-token"
      )

    assert config.storage == :sqlite
    assert config.sqlite_path == ".favn/data/dev.sqlite"
    assert config.orchestrator_port == 4201
    assert config.web_port == 4273
    assert config.orchestrator_base_url == "http://127.0.0.1:4201"
    assert config.web_base_url == "http://127.0.0.1:4273"
    assert config.service_token == "dev-token"
  end
end
