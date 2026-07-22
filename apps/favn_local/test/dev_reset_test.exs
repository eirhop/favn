defmodule Favn.Dev.ResetTest do
  use ExUnit.Case, async: true

  alias Favn.Dev.{ComposeProject, Paths, Reset, State}

  setup do
    root_dir =
      Path.join(System.tmp_dir!(), "favn_dev_reset_test_#{System.unique_integer([:positive])}")

    File.mkdir_p!(root_dir)
    on_exit(fn -> File.rm_rf(root_dir) end)
    %{root_dir: root_dir}
  end

  test "run/1 requires confirmation and describes preserved consumer resources", %{
    root_dir: root_dir
  } do
    assert :ok = State.ensure_layout(root_dir: root_dir)

    assert {:error, {:confirmation_required, resources}} = Reset.run(root_dir: root_dir)
    assert resources.compose_project == ComposeProject.project_name(root_dir)
    assert resources.generated_state == Path.join(root_dir, ".favn")
    assert resources.preserved_data == Path.join(root_dir, ".favn/data")
    assert resources.runner_images == []
  end

  test "confirmed reset removes generated state but preserves durable data", %{root_dir: root_dir} do
    assert :ok = State.ensure_layout(root_dir: root_dir)
    data = Path.join(root_dir, ".favn/data/sentinel.duckdb")
    generated = Path.join(root_dir, ".favn/logs/generated.log")
    File.write!(data, "durable")
    File.write!(generated, "{}")

    assert :ok = Reset.run(root_dir: root_dir, yes: true)
    assert File.read!(data) == "durable"
    refute File.exists?(generated)
  end

  test "confirmed reset preserves a selected Compose template below .favn", %{
    root_dir: root_dir
  } do
    assert :ok = State.ensure_layout(root_dir: root_dir)
    compose = Path.join(root_dir, ".favn/compose/team.yml")
    companion = Path.rootname(compose) <> ".env.example"
    File.write!(compose, "services: {}\n")
    File.write!(companion, "FAVN_EXAMPLE=value\n")
    File.write!(Paths.compose_failure_log_path(root_dir), "generated\n")

    assert :ok =
             Reset.run(root_dir: root_dir, compose_file: compose, yes: true)

    assert File.read!(compose) == "services: {}\n"
    assert File.read!(companion) == "FAVN_EXAMPLE=value\n"
    refute File.exists?(Paths.compose_failure_log_path(root_dir))
  end

  test "confirmed reset rejects a symlinked state root", %{root_dir: root_dir} do
    external = root_dir <> "-external"
    File.mkdir_p!(external)
    File.write!(Path.join(external, "sentinel"), "keep\n")
    File.ln_s!(external, Paths.favn_dir(root_dir))

    on_exit(fn -> File.rm_rf(external) end)

    assert {:error, {:unsafe_generated_state, favn_dir}} =
             Reset.run(root_dir: root_dir, yes: true)

    assert favn_dir == Paths.favn_dir(root_dir)
    assert File.read!(Path.join(external, "sentinel")) == "keep\n"
  end

  test "reset discovers running Favn roles when runtime state is missing", %{
    root_dir: root_dir
  } do
    assert :ok = State.ensure_layout(root_dir: root_dir)
    project = ComposeProject.project_name(root_dir)

    command_runner = project_role_runner(project, true)

    assert {:error, {:reset_roles_running, ["partial-runner"]}} =
             Reset.run(
               root_dir: root_dir,
               yes: true,
               docker_executable: "docker",
               docker_command_runner: command_runner
             )

    assert File.dir?(Paths.favn_dir(root_dir))
  end

  test "reset refuses while a recorded Favn role is running and never invokes down", %{
    root_dir: root_dir
  } do
    assert :ok = State.write_runtime(runtime_state(root_dir), root_dir: root_dir)
    parent = self()

    command_runner = fn "docker", args, _opts ->
      send(parent, {:docker, args})

      case args do
        [
          "compose",
          _project_flag,
          _project,
          _file_flag,
          _file,
          _env_flag,
          _env,
          "ps",
          "--status",
          "running",
          "--services"
        ] ->
          {"team-runner\n", 0}

        other ->
          {"unexpected: #{inspect(other)}", 99}
      end
    end

    assert {:error, {:reset_roles_running, ["team-runner"]}} =
             Reset.run(
               root_dir: root_dir,
               yes: true,
               docker_executable: "docker",
               docker_command_runner: command_runner
             )

    assert_received {:docker, args}
    refute "down" in args
    refute "--volumes" in args
    refute "--remove-orphans" in args
    assert File.exists?(Paths.runtime_path(root_dir))
  end

  defp runtime_state(root_dir) do
    project = ComposeProject.project_name(root_dir)

    %{
      "schema_version" => 6,
      "kind" => "docker_compose",
      "compose_contract_version" => 1,
      "compose_profile" => "local",
      "compose_file" => Path.join(root_dir, "deploy/compose.local.yml"),
      "compose_project" => project,
      "compose_services" => %{
        "postgres" => "team-db",
        "control-plane-ops" => "team-ops",
        "control-plane-verify" => "team-verify",
        "runner" => "team-runner",
        "control-plane" => "team-control"
      },
      "workspace_id" => "local-dev",
      "view_url" => "http://127.0.0.1:4173",
      "orchestrator_url" => "http://127.0.0.1:4101",
      "control_plane_image_reference" => "control@sha256:#{String.duplicate("a", 64)}",
      "runner_image_reference" => "favn-local-runner-#{project}:rr_#{String.duplicate("b", 64)}"
    }
  end

  defp project_role_runner(project, running?) do
    fn "docker", args, _opts ->
      case args do
        [
          "container",
          "ls",
          "--all",
          "--quiet",
          "--filter",
          "label=com.docker.compose.project=" <> ^project,
          "--filter",
          "label=io.favn.compose.contract-version=1",
          "--filter",
          "label=io.favn.compose.profile=local",
          "--filter",
          "label=io.favn.compose.role"
        ] ->
          {"runner-id\n", 0}

        ["container", "inspect", "runner-id"] ->
          {JSON.encode!([project_role_inspection(project, running?)]), 0}

        other ->
          {"unexpected: #{inspect(other)}", 99}
      end
    end
  end

  defp project_role_inspection(project, running?) do
    %{
      "Id" => "runner-id",
      "Name" => "/partial-runner",
      "Config" => %{
        "Labels" => %{
          "com.docker.compose.project" => project,
          "io.favn.compose.contract-version" => "1",
          "io.favn.compose.profile" => "local",
          "io.favn.compose.role" => "runner"
        }
      },
      "State" => %{"Running" => running?}
    }
  end
end
