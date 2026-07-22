defmodule Favn.Dev.StopTest do
  use ExUnit.Case, async: true

  alias Favn.Dev.{ComposeProject, State}

  setup do
    root_dir =
      Path.join(System.tmp_dir!(), "favn_dev_stop_test_#{System.unique_integer([:positive])}")

    File.mkdir_p!(root_dir)
    on_exit(fn -> File.rm_rf(root_dir) end)
    %{root_dir: root_dir}
  end

  test "stop/1 is idempotent before installation", %{root_dir: root_dir} do
    assert :ok = Favn.Dev.stop(root_dir: root_dir)
  end

  test "stop/1 gracefully stops Compose services and preserves project state", %{
    root_dir: root_dir
  } do
    project_name = ComposeProject.project_name(root_dir)

    assert :ok =
             State.write_install(%{"schema_version" => 5, "marker" => "preserved"},
               root_dir: root_dir
             )

    assert :ok = State.write_runtime(runtime_state(root_dir, project_name), root_dir: root_dir)
    parent = self()

    runner = fn "docker", args, _opts ->
      send(parent, {:docker, args})
      {"", 0}
    end

    assert :ok =
             Favn.Dev.stop(
               root_dir: root_dir,
               docker_executable: "docker",
               docker_command_runner: runner
             )

    assert_receive {:docker, command}
    assert Enum.take(command, -4) == ["stop", "--timeout", "180", "control-plane"]
    assert_receive {:docker, command}
    assert Enum.take(command, -4) == ["stop", "--timeout", "180", "runner"]
    assert_receive {:docker, command}
    assert Enum.take(command, -4) == ["stop", "--timeout", "30", "postgres"]
    assert {:error, :not_found} = State.read_runtime(root_dir: root_dir)
    assert {:ok, _install} = State.read_install(root_dir: root_dir)
  end

  test "stop discovers and stops Favn roles left by a partial start", %{root_dir: root_dir} do
    project = ComposeProject.project_name(root_dir)
    parent = self()

    runner = fn "docker", args, _opts ->
      send(parent, {:docker, args})

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
          {JSON.encode!([project_role_inspection(project)]), 0}

        ["container", "stop", "--time", "180", "runner-id"] ->
          {"runner-id\n", 0}

        other ->
          {"unexpected: #{inspect(other)}", 99}
      end
    end

    assert :ok =
             Favn.Dev.stop(
               root_dir: root_dir,
               docker_executable: "docker",
               docker_command_runner: runner
             )

    assert_received {:docker, ["container", "ls" | _filters]}
    assert_received {:docker, ["container", "inspect", "runner-id"]}
    assert_received {:docker, ["container", "stop", "--time", "180", "runner-id"]}
  end

  defp runtime_state(root_dir, project_name) do
    %{
      "schema_version" => 6,
      "kind" => "docker_compose",
      "compose_contract_version" => 1,
      "compose_profile" => "local",
      "compose_file" => Path.join(root_dir, "deploy/compose.local.yml"),
      "compose_project" => project_name,
      "compose_services" => %{
        "postgres" => "postgres",
        "control-plane-ops" => "control-plane-ops",
        "control-plane-verify" => "control-plane-verify",
        "runner" => "runner",
        "control-plane" => "control-plane"
      },
      "workspace_id" => "local-dev",
      "view_url" => "http://127.0.0.1:4173",
      "orchestrator_url" => "http://127.0.0.1:4101",
      "control_plane_image_reference" =>
        "ghcr.io/eirhop/favn-control-plane@sha256:#{String.duplicate("a", 64)}",
      "runner_image_reference" => "favn-local-runner-#{project_name}:rr_fixture"
    }
  end

  defp project_role_inspection(project) do
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
      "State" => %{"Running" => true}
    }
  end
end
