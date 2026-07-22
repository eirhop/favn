defmodule Favn.Dev.RunnerImageTest do
  use ExUnit.Case, async: true

  alias Favn.Dev.RunnerImage

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
end
