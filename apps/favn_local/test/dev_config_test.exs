defmodule Favn.Dev.ConfigTest do
  use ExUnit.Case, async: false

  alias Favn.Dev.Config

  test "resolve/1 returns defaults" do
    config = Config.resolve([])

    assert config.workspace_id == "local-dev"
    assert config.orchestrator_port == 4101
    assert config.web_port == 4173
    assert config.scheduler_enabled == false
    assert config.compose_file == "deploy/compose.local.yml"
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

  test "Compose selection uses CLI, then local config, then the project default" do
    root_dir = temp_project!()
    default = write_compose!(root_dir, "deploy/compose.local.yml")
    configured = write_compose!(root_dir, "deploy/compose.configured.yml")
    cli = write_compose!(root_dir, "deploy/compose.cli.yml")

    on_exit(fn ->
      Application.delete_env(:favn, :local)
      File.rm_rf(root_dir)
    end)

    assert {:ok, ^default} = Config.resolve_compose_file(root_dir: root_dir)

    Application.put_env(:favn, :local, compose_file: "deploy/compose.configured.yml")
    assert {:ok, ^configured} = Config.resolve_compose_file(root_dir: root_dir)

    assert {:ok, ^cli} =
             Config.resolve_compose_file(
               root_dir: root_dir,
               compose_file: "deploy/compose.cli.yml"
             )
  end

  test "Compose selection rejects missing, external, and symlinked files" do
    root_dir = temp_project!()
    outside = Path.join(System.tmp_dir!(), "outside-#{System.unique_integer([:positive])}.yml")
    File.write!(outside, "services: {}\n")
    link = Path.join(root_dir, "compose.link.yml")
    File.ln_s!(outside, link)

    on_exit(fn ->
      File.rm_rf(root_dir)
      File.rm(outside)
    end)

    missing = Path.join(root_dir, "deploy/compose.local.yml")

    assert {:error, {:compose_file_missing, ^missing}} =
             Config.resolve_compose_file(root_dir: root_dir)

    assert {:error, {:compose_file_outside_project, ^outside}} =
             Config.resolve_compose_file(root_dir: root_dir, compose_file: outside)

    assert {:error, {:compose_file_symlink, ^link}} =
             Config.resolve_compose_file(root_dir: root_dir, compose_file: link)
  end

  defp temp_project! do
    root_dir =
      Path.join(System.tmp_dir!(), "favn_config_test_#{System.unique_integer([:positive])}")

    File.mkdir_p!(root_dir)
    File.write!(Path.join(root_dir, "mix.exs"), "defmodule Temp.MixProject do\nend\n")
    root_dir
  end

  defp write_compose!(root_dir, relative) do
    path = Path.join(root_dir, relative)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, "services: {}\n")
    path
  end
end
