defmodule Favn.Dev.RunnerImageTest do
  use ExUnit.Case, async: true

  alias Favn.Dev.RunnerImage
  alias Favn.Dev.Maintainer.RunnerBuildCapability

  setup do
    root_dir =
      Path.join(System.tmp_dir!(), "favn_runner_image_test_#{System.unique_integer([:positive])}")

    File.mkdir_p!(root_dir)
    on_exit(fn -> File.rm_rf(root_dir) end)
    %{root_dir: root_dir}
  end

  test "runner release build output is redacted while streamed and returned", %{
    root_dir: root_dir
  } do
    parent = self()
    secret = "runner-build-secret-value"
    database_url = "postgresql://build-user:build-password@db.internal/favn"

    command_runner = fn _mix, _args, command_opts ->
      writer = Keyword.fetch!(command_opts, :output_writer)
      writer.("building with runner-build-")
      writer.("secret-value and #{database_url}\n")
      {"build failed with #{secret} and #{database_url}", 9}
    end

    assert {:error, {:runner_release_build_failed, 9, returned}} =
             RunnerImage.ensure(
               %{"project_name" => "redaction-test"},
               root_dir: root_dir,
               env_file_loaded: %{"FAVN_BUILD_CREDENTIAL" => secret},
               progress_fun: &send(parent, {:streamed, &1}),
               runner_command_runner: command_runner
             )

    assert_receive {:streamed, streamed}
    assert streamed =~ "[REDACTED]"
    assert streamed =~ "[REDACTED_URL]"
    refute streamed =~ secret
    refute streamed =~ database_url

    assert returned =~ "[REDACTED]"
    assert returned =~ "[REDACTED_URL]"
    refute returned =~ secret
    refute returned =~ database_url
  end

  test "maintainer runner builds receive the scoped internal capability", %{root_dir: root_dir} do
    parent = self()
    revision = String.duplicate("a", 40)

    capability = %RunnerBuildCapability{
      consumer_root: root_dir,
      checkout: Path.join(root_dir, "favn"),
      revision: revision,
      dirty: true,
      fingerprint: String.duplicate("b", 64)
    }

    command_runner = fn _mix, args, command_opts ->
      send(parent, {:runner_build, args, Keyword.fetch!(command_opts, :env)})
      {"stopped after capability capture", 9}
    end

    assert {:error, {:runner_release_build_failed, 9, _output}} =
             RunnerImage.ensure(
               %{"project_name" => "maintainer-capability"},
               root_dir: root_dir,
               maintainer_runner_build: capability,
               runner_command_runner: command_runner
             )

    assert_receive {:runner_build, ["favn.build.runner", "--root-dir", ^root_dir], environment}
    assert {"MIX_ENV", "prod"} in environment

    assert Enum.any?(environment, fn
             {"FAVN_INTERNAL_MAINTAINER_RUNNER_BUILD", value} -> value != ""
             _other -> false
           end)

    token =
      environment
      |> Enum.find_value(fn
        {"FAVN_INTERNAL_MAINTAINER_RUNNER_BUILD", value} -> value
        _other -> nil
      end)

    capability_path =
      Path.join([root_dir, ".favn", "build", "maintainer-runner-capabilities", token])

    refute File.exists?(capability_path)
    refute File.exists?(capability_path <> ".consuming")
  end

  test "ordinary runner builds explicitly unset an inherited maintainer capability", %{
    root_dir: root_dir
  } do
    parent = self()

    command_runner = fn _mix, _args, command_opts ->
      send(parent, {:environment, Keyword.fetch!(command_opts, :env)})
      {"stopped after environment capture", 9}
    end

    assert {:error, {:runner_release_build_failed, 9, _output}} =
             RunnerImage.ensure(
               %{"project_name" => "ordinary-capability-scrub"},
               root_dir: root_dir,
               runner_command_runner: command_runner
             )

    assert_receive {:environment, environment}
    assert {"FAVN_INTERNAL_MAINTAINER_RUNNER_BUILD", nil} in environment
  end
end
