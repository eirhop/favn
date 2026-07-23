defmodule Favn.Dev.RunnerImageTest do
  use ExUnit.Case, async: true

  alias Favn.Dev.RunnerImage
  alias Favn.Manifest.Compatibility
  alias Favn.RunnerRelease

  setup do
    root_dir =
      Path.join(
        System.tmp_dir!(),
        "favn_runner_image_test_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(root_dir)
    on_exit(fn -> File.rm_rf(root_dir) end)
    %{root_dir: root_dir}
  end

  test "selects an existing compatible customer image without building it", context do
    reference = "registry.example/customer-runner:test"
    release_id = FavnTestSupport.runner_release_id()
    parent = self()

    runner = fn _docker, ["image", "inspect", ^reference], _opts ->
      send(parent, :inspected)
      {Jason.encode!([inspection(release_id)]), 0}
    end

    assert {:ok, selected} =
             RunnerImage.ensure(
               %{"runner_image" => reference},
               root_dir: context.root_dir,
               docker_executable: "/usr/bin/docker",
               docker_command_runner: runner
             )

    assert_received :inspected
    assert selected.selected_reference == reference
    assert selected.image_reference == "sha256:" <> String.duplicate("a", 64)
    assert selected.image_id == selected.image_reference
    assert selected.runner_release_id == release_id
    assert selected.status == :selected
  end

  test "requires the user to select and build the image", context do
    assert {:error, :runner_image_required} = RunnerImage.ensure(%{})

    assert {:error, {:docker_image_unavailable, "customer:missing"}} =
             RunnerImage.ensure(
               %{"runner_image" => "customer:missing"},
               root_dir: context.root_dir,
               docker_executable: "/usr/bin/docker",
               docker_command_runner: fn _docker, _args, _opts -> {"missing", 1} end
             )
  end

  test "rejects missing or incompatible runner labels", context do
    reference = "customer:invalid"

    runner = fn _docker, ["image", "inspect", ^reference], _opts ->
      invalid =
        inspection("latest")
        |> put_in(["Config", "Labels", "io.favn.runner-contract-version"], "999")

      {Jason.encode!([invalid]), 0}
    end

    assert {:error, {:runner_image_release_id_invalid, "latest"}} =
             RunnerImage.ensure(
               %{"runner_image" => reference},
               root_dir: context.root_dir,
               docker_executable: "/usr/bin/docker",
               docker_command_runner: runner
             )
  end

  defp inspection(release_id) do
    %{
      "Id" => "sha256:" <> String.duplicate("a", 64),
      "RepoDigests" => [],
      "Architecture" => "amd64",
      "Os" => "linux",
      "Config" => %{
        "User" => "10001:10001",
        "Labels" => %{
          "io.favn.runner-release-id" => release_id,
          "io.favn.version" => RunnerRelease.current_favn_version(),
          "io.favn.runner-contract-version" =>
            Integer.to_string(Compatibility.current_runner_contract_version()),
          "io.favn.target" => RunnerRelease.current_target()
        }
      }
    }
  end
end
