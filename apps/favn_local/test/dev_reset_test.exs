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

  test "run/1 requires explicit confirmation and reports the exact scope", %{root_dir: root_dir} do
    assert :ok = State.ensure_layout(root_dir: root_dir)

    assert {:error, {:confirmation_required, resources}} = Reset.run(root_dir: root_dir)
    assert resources.compose_project == Favn.Dev.ComposeProject.project_name(root_dir)
    assert resources.postgres_volume == resources.compose_project <> "-postgres-data"
    assert resources.local_state == Path.join(root_dir, ".favn")
    assert resources.runner_images == []
    assert File.dir?(Path.join(root_dir, ".favn"))
  end

  test "run/1 removes local state only after yes confirmation", %{root_dir: root_dir} do
    assert :ok = State.ensure_layout(root_dir: root_dir)
    assert :ok = File.write(Path.join(root_dir, ".favn/runtime.json"), "{}")

    assert :ok = Reset.run(root_dir: root_dir, yes: true)
    refute File.exists?(Path.join(root_dir, ".favn"))
  end

  test "run/1 rejects copied Compose identity before invoking Docker", %{root_dir: root_dir} do
    other_root = root_dir <> "-other"
    File.mkdir_p!(Paths.compose_dir(other_root))

    compose_path = Paths.compose_path(other_root)
    env_path = Paths.compose_env_path(other_root)
    runner_env_path = Paths.compose_runner_env_path(other_root)
    postgres_init_path = Paths.compose_postgres_init_path(other_root)
    File.write!(compose_path, "services: {}\n")
    File.write!(env_path, "FAVN_RUNNER_IMAGE='runner'\n")
    File.write!(runner_env_path, "")
    File.write!(postgres_init_path, "#!/bin/sh\n")

    other_project = ComposeProject.project_name(other_root)

    assert :ok =
             State.write_install(
               %{
                 "compose" => %{
                   "schema_version" => 1,
                   "project_name" => other_project,
                   "network_name" => other_project <> "-network",
                   "postgres_volume_name" => other_project <> "-postgres-data",
                   "compose_path" => compose_path,
                   "env_path" => env_path,
                   "runner_env_path" => runner_env_path,
                   "postgres_init_path" => postgres_init_path,
                   "compose_sha256" => sha256(File.read!(compose_path))
                 }
               },
               root_dir: root_dir
             )

    parent = self()

    command_runner = fn executable, args, _opts ->
      send(parent, {:docker_command, executable, args})
      {"", 0}
    end

    assert {:error, :install_stale} =
             Reset.run(
               root_dir: root_dir,
               yes: true,
               docker_executable: "docker",
               docker_command_runner: command_runner
             )

    refute_receive {:docker_command, _executable, _args}
    assert File.dir?(Paths.favn_dir(root_dir))
    File.rm_rf(other_root)
  end

  defp sha256(contents), do: :crypto.hash(:sha256, contents) |> Base.encode16(case: :lower)
end
