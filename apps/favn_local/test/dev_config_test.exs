defmodule Favn.Dev.ConfigTest do
  use ExUnit.Case, async: true

  alias Favn.Dev.Config

  test "resolve/1 returns defaults" do
    config = Config.resolve([])

    assert config.storage == :memory
    assert config.sqlite_path == ".favn/data/orchestrator.sqlite3"
    assert config.postgres.hostname == "127.0.0.1"
    assert config.postgres.port == 5432
    assert config.orchestrator_api_enabled == true
    assert config.orchestrator_port == 4101
    assert config.web_port == 4173
    assert config.orchestrator_base_url == "http://127.0.0.1:4101"
    assert config.web_base_url == "http://127.0.0.1:4173"
    assert config.scheduler_enabled == false
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
        scheduler: true,
        service_token: "dev-token"
      )

    assert config.storage == :sqlite
    assert config.sqlite_path == ".favn/data/dev.sqlite"
    assert config.orchestrator_port == 4201
    assert config.web_port == 4273
    assert config.orchestrator_base_url == "http://127.0.0.1:4201"
    assert config.web_base_url == "http://127.0.0.1:4273"
    assert config.scheduler_enabled == true
    assert config.service_token == "dev-token"
  end

  test "resolve/1 lets CLI scheduler option override local config" do
    Application.put_env(:favn, :local, scheduler: true)

    on_exit(fn ->
      Application.delete_env(:favn, :local)
    end)

    assert Config.resolve([]).scheduler_enabled == true
    assert Config.resolve(scheduler: false).scheduler_enabled == false
  end

  test "resolve/1 supports postgres storage and postgres options" do
    config =
      Config.resolve(
        storage: :postgres,
        postgres: [
          hostname: "db",
          port: 6543,
          username: "u",
          password: "p",
          database: "favn_dev",
          ssl: true
        ]
      )

    assert config.storage == :postgres
    assert config.postgres.hostname == "db"
    assert config.postgres.port == 6543
    assert config.postgres.username == "u"
    assert config.postgres.password == "p"
    assert config.postgres.database == "favn_dev"
    assert config.postgres.ssl == true
  end

  test "resolve/1 reads :local config and lets it override :dev" do
    Application.put_env(:favn, :dev, storage: :memory, sqlite_path: "dev.sqlite")
    Application.put_env(:favn, :local, storage: :sqlite, sqlite_path: "local.sqlite")

    on_exit(fn ->
      Application.delete_env(:favn, :dev)
      Application.delete_env(:favn, :local)
    end)

    config = Config.resolve([])
    assert config.storage == :sqlite
    assert config.sqlite_path == "local.sqlite"
  end
end
