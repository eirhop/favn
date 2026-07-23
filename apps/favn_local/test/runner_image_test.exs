defmodule Favn.Dev.RunnerImageTest do
  use ExUnit.Case, async: true

  alias Favn.Dev.{Docker, RunnerImage}
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

  test "reports unavailable explicitly selected images", context do
    assert {:error, {:docker_image_unavailable, "customer:missing"}} =
             RunnerImage.ensure(
               %{"runner_image" => "customer:missing"},
               root_dir: context.root_dir,
               docker_executable: "/usr/bin/docker",
               docker_command_runner: fn _docker, _args, _opts -> {"missing", 1} end
             )
  end

  test "builds the generated customer Dockerfile when no image is selected", context do
    dockerfile = Path.join(context.root_dir, "deploy/runner/Dockerfile")
    File.mkdir_p!(Path.dirname(dockerfile))
    File.write!(dockerfile, "FROM scratch\n")

    release_id = FavnTestSupport.runner_release_id()
    project_name = "favn-sample-project"
    reference = "favn-local/#{project_name}-runner:dev"
    parent = self()

    runner = fn
      _docker, ["build" | args], _opts ->
        send(parent, {:built, args})
        {"built", 0}

      _docker, ["image", "inspect", ^reference], _opts ->
        {Jason.encode!([inspection(release_id)]), 0}
    end

    assert {:ok, built} =
             RunnerImage.ensure(
               %{"project_name" => project_name, "runner_image" => nil},
               root_dir: context.root_dir,
               runner_release_id_fun: fn -> release_id end,
               docker_executable: "/usr/bin/docker",
               docker_command_runner: runner
             )

    assert_received {:built, args}
    assert ["--pull", "--platform", "linux/amd64"] = Enum.slice(args, 0, 3)
    assert "FAVN_PROJECT_ROOT=." in args
    assert "FAVN_RUNNER_RELEASE_ID=#{release_id}" in args
    assert dockerfile in args
    assert reference in args
    assert built.status == :built
    assert built.selected_reference == reference
  end

  test "passes a maintainer checkout as a separate Docker build context", context do
    favn_context = Path.join(context.root_dir, "favn-context")
    parent = self()

    runner = fn _docker, ["build" | args], _opts ->
      send(parent, {:built, args})
      {"built", 0}
    end

    assert :ok =
             Docker.build_runner(
               "favn-local/sample-runner:dev",
               Path.join(context.root_dir, "deploy/runner/Dockerfile"),
               context.root_dir,
               ".",
               FavnTestSupport.runner_release_id(),
               runner_favn_context: favn_context,
               docker_executable: "/usr/bin/docker",
               docker_command_runner: runner
             )

    assert_received {:built, args}
    assert "--build-context" in args
    assert "favn-checkout=#{favn_context}" in args
    assert "FAVN_BUILD_SOURCE=maintainer-source" in args
  end

  test "requires the generated Dockerfile for the automatic build", context do
    project = %{"project_name" => "favn-sample-project", "runner_image" => nil}
    missing = Path.join(context.root_dir, "deploy/runner/Dockerfile")

    assert {:error, {:runner_dockerfile_missing, ^missing}} =
             RunnerImage.ensure(project, root_dir: context.root_dir)
  end

  test "rejects an automatic build that ignores the generated release ID", context do
    dockerfile = Path.join(context.root_dir, "deploy/runner/Dockerfile")
    File.mkdir_p!(Path.dirname(dockerfile))
    File.write!(dockerfile, "FROM scratch\n")

    expected = FavnTestSupport.runner_release_id(:primary)
    actual = FavnTestSupport.runner_release_id(:alternate)
    reference = "favn-local/favn-sample-project-runner:dev"

    runner = fn
      _docker, ["build" | _args], _opts ->
        {"built", 0}

      _docker, ["image", "inspect", ^reference], _opts ->
        {Jason.encode!([inspection(actual)]), 0}
    end

    assert {:error, {:runner_image_release_id_mismatch, %{expected: ^expected, actual: ^actual}}} =
             RunnerImage.ensure(
               %{"project_name" => "favn-sample-project", "runner_image" => nil},
               root_dir: context.root_dir,
               runner_release_id_fun: fn -> expected end,
               docker_executable: "/usr/bin/docker",
               docker_command_runner: runner
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
