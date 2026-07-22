defmodule Favn.Dev.StopTest do
  use ExUnit.Case, async: true

  alias Favn.Dev.{ComposeProject, Paths, State}

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

    project = %{
      "project_name" => project_name,
      "compose_path" => Paths.compose_path(root_dir),
      "env_path" => Paths.compose_env_path(root_dir)
    }

    assert :ok = State.write_install(%{"compose" => project}, root_dir: root_dir)
    assert :ok = State.write_runtime(%{"kind" => "docker_compose"}, root_dir: root_dir)
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
end
