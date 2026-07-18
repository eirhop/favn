defmodule Favn.Dev.ConfigTest do
  use ExUnit.Case, async: false

  alias Favn.Dev.Config

  setup do
    database_url = System.get_env("FAVN_DATABASE_URL")
    System.delete_env("FAVN_DATABASE_URL")

    on_exit(fn ->
      if database_url,
        do: System.put_env("FAVN_DATABASE_URL", database_url),
        else: System.delete_env("FAVN_DATABASE_URL")
    end)

    :ok
  end

  test "resolve/1 returns defaults" do
    config = Config.resolve([])

    assert config.postgres.hostname == "127.0.0.1"
    assert config.postgres.port == 5432
    assert config.postgres.url == "ecto://postgres:postgres@127.0.0.1:5432/favn"
    assert config.workspace_id == "local-dev"
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
        workspace_id: "customer-dev",
        orchestrator_port: 4201,
        web_port: 4273,
        scheduler: true,
        service_token: "dev-token"
      )

    assert config.workspace_id == "customer-dev"
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

    assert config.postgres.hostname == "db"
    assert config.postgres.port == 6543
    assert config.postgres.username == "u"
    assert config.postgres.password == "p"
    assert config.postgres.database == "favn_dev"
    assert config.postgres.ssl == true
  end

  test "resolve/1 strictly parses positive integer strings" do
    config =
      Config.resolve(
        orchestrator_port: " 4201 ",
        web_port: "4273",
        postgres: [
          port: " 6543 ",
          pool_size: "12"
        ]
      )

    assert config.orchestrator_port == 4201
    assert config.web_port == 4273
    assert config.postgres.port == 6543
    assert config.postgres.pool_size == 12
  end

  test "resolve/1 falls back to defaults for malformed integer strings" do
    config =
      Config.resolve(
        orchestrator_port: "4101abc",
        web_port: "4173.0",
        postgres: [
          port: "5432abc",
          pool_size: "0"
        ]
      )

    assert config.orchestrator_port == 4101
    assert config.web_port == 4173
    assert config.postgres.port == 5432
    assert config.postgres.pool_size == 10
  end

  test "resolve/1 reads :local config and lets it override :dev" do
    Application.put_env(:favn, :dev, workspace_id: "dev")
    Application.put_env(:favn, :local, workspace_id: "local")

    on_exit(fn ->
      Application.delete_env(:favn, :dev)
      Application.delete_env(:favn, :local)
    end)

    config = Config.resolve([])
    assert config.workspace_id == "local"
  end
end
