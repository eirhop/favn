defmodule Favn.Dev.ConfigTest do
  use ExUnit.Case, async: true

  alias Favn.Dev.Config

  test "resolve/1 returns defaults" do
    config = Config.resolve([])

    assert config.workspace_id == "local-dev"
    assert config.orchestrator_port == 4101
    assert config.web_port == 4173
    assert config.scheduler_enabled == false
  end

  test "resolve/1 applies runtime overrides" do
    config =
      Config.resolve(
        workspace_id: "customer-dev",
        orchestrator_port: 4201,
        web_port: 4273,
        scheduler: true
      )

    assert config.workspace_id == "customer-dev"
    assert config.orchestrator_port == 4201
    assert config.web_port == 4273
    assert config.scheduler_enabled == true
  end

  test "resolve/1 lets CLI scheduler option override local config" do
    Application.put_env(:favn, :local, scheduler: true)

    on_exit(fn ->
      Application.delete_env(:favn, :local)
    end)

    assert Config.resolve([]).scheduler_enabled == true
    assert Config.resolve(scheduler: false).scheduler_enabled == false
  end

  test "resolve/1 strictly parses positive integer strings" do
    config =
      Config.resolve(
        orchestrator_port: " 4201 ",
        web_port: "4273"
      )

    assert config.orchestrator_port == 4201
    assert config.web_port == 4273
  end

  test "resolve/1 falls back to defaults for malformed integer strings" do
    config =
      Config.resolve(
        orchestrator_port: "4101abc",
        web_port: "4173.0"
      )

    assert config.orchestrator_port == 4101
    assert config.web_port == 4173
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
