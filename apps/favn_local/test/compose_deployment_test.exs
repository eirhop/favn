defmodule Favn.Dev.ComposeDeploymentTest do
  use ExUnit.Case, async: true

  alias Favn.Dev.{ComposeDeployment, Docker}

  @control "ghcr.io/eirhop/favn-control-plane@sha256:#{String.duplicate("a", 64)}"
  @release "rr_#{String.duplicate("b", 64)}"
  @image_id "sha256:#{String.duplicate("c", 64)}"

  setup do
    root_dir =
      Path.join(
        System.tmp_dir!(),
        "compose_deployment_test_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(Path.join(root_dir, ".favn/compose"))
    compose_file = Path.join(root_dir, "deploy/compose.local.yml")
    env_file = Path.join(root_dir, ".favn/compose/.env")
    File.mkdir_p!(Path.dirname(compose_file))
    File.write!(compose_file, "services: {}\n")
    File.write!(env_file, "")

    project_name = "favn-test-project"
    runner_reference = "favn-local-runner-#{project_name}:#{@release}"

    project = %{
      "project_name" => project_name,
      "compose_path" => compose_file,
      "env_path" => env_file,
      "workspace_id" => "local-dev",
      "view_url" => "http://127.0.0.1:4173",
      "orchestrator_url" => "http://127.0.0.1:4101"
    }

    runner = %{
      runner_release_id: @release,
      image_reference: runner_reference,
      image_id: @image_id
    }

    install = %{"source" => "official", "image_reference" => @control}

    on_exit(fn -> File.rm_rf(root_dir) end)

    %{
      install: install,
      project: project,
      root_dir: root_dir,
      runner: runner,
      runner_reference: runner_reference
    }
  end

  test "discovers renamed roles and permits extra consumer configuration", context do
    rendered = rendered_compose(context.runner_reference)
    runner = docker_runner(rendered, context.runner)

    assert {:ok, deployment} =
             ComposeDeployment.resolve(
               context.project,
               context.install,
               context.runner,
               root_dir: context.root_dir,
               required_profile: :local,
               docker_executable: "docker",
               docker_command_runner: runner
             )

    assert deployment.services.runner == "team-runner"
    assert deployment.services.control_plane == "team-control"
    assert deployment.services.postgres == "team-db"
    assert ComposeDeployment.relative_compose_file(deployment) == "deploy/compose.local.yml"
    refute Map.has_key?(deployment.services, :redis)
  end

  test "fails closed for missing, duplicate, unknown, and inconsistent roles", context do
    base = rendered_compose(context.runner_reference)

    missing = update_in(base, ["services"], &Map.delete(&1, "team-runner"))
    assert_error(context, missing, {:missing_compose_roles, [:runner]})

    duplicate =
      put_in(base, ["services", "second-runner"], base["services"]["team-runner"])

    assert_error(context, duplicate, {:duplicate_compose_role, :runner})

    unknown =
      put_in(
        base,
        ["services", "team-runner", "labels", "io.favn.compose.role"],
        "scheduler"
      )

    assert_error(context, unknown, {:unknown_compose_role, "scheduler"})

    inconsistent =
      put_in(
        base,
        ["services", "team-runner", "labels", "io.favn.compose.profile"],
        "single-host"
      )

    assert {:error, {:inconsistent_compose_profile, "team-runner"}} =
             resolve(context, inconsistent)
  end

  test "fails when a role selects a different immutable image", context do
    rendered =
      put_in(
        rendered_compose(context.runner_reference),
        ["services", "team-control", "image"],
        "ghcr.io/example/forged@sha256:#{String.duplicate("d", 64)}"
      )

    assert {:error, {:compose_role_image_mismatch, :control_plane, _actual, @control}} =
             resolve(context, rendered)
  end

  test "Docker lifecycle commands accept only the typed deployment", context do
    assert_raise FunctionClauseError, fn ->
      Docker.compose(context.project, ["ps"], [])
    end
  end

  defp assert_error(context, rendered, expected) do
    assert {:error, ^expected} = resolve(context, rendered)
  end

  defp resolve(context, rendered) do
    ComposeDeployment.resolve(
      context.project,
      context.install,
      context.runner,
      root_dir: context.root_dir,
      required_profile: :local,
      docker_executable: "docker",
      docker_command_runner: docker_runner(rendered, context.runner)
    )
  end

  defp rendered_compose(runner_reference) do
    labels = fn role ->
      %{
        "io.favn.compose.contract-version" => "1",
        "io.favn.compose.profile" => "local",
        "io.favn.compose.role" => role
      }
    end

    %{
      "services" => %{
        "team-db" => %{"image" => "postgres:18", "labels" => labels.("postgres")},
        "team-ops" => %{"image" => @control, "labels" => labels.("control-plane-ops")},
        "team-verify" => %{
          "image" => @control,
          "labels" => labels.("control-plane-verify")
        },
        "team-runner" => %{
          "image" => runner_reference,
          "labels" => labels.("runner"),
          "volumes" => [%{"source" => "/customer", "target" => "/customer"}],
          "depends_on" => %{"redis" => %{"condition" => "service_healthy"}}
        },
        "team-control" => %{
          "image" => @control,
          "labels" => labels.("control-plane")
        },
        "redis" => %{"image" => "redis:8", "networks" => ["default"]}
      },
      "volumes" => %{"customer-cache" => %{}},
      "networks" => %{"default" => %{}, "customer" => %{}}
    }
  end

  defp docker_runner(rendered, runner) do
    fn "docker", args, _opts ->
      case args do
        ["compose", "version", "--short"] ->
          {"2.39.1\n", 0}

        [
          "compose",
          "--project-name",
          _project,
          "--file",
          _file,
          "--env-file",
          _env,
          "--profile",
          "*",
          "config",
          "--format",
          "json"
        ] ->
          {JSON.encode!(rendered), 0}

        ["image", "inspect", reference] when reference == runner.image_reference ->
          inspection = %{
            "Id" => runner.image_id,
            "RepoDigests" => [],
            "Architecture" => "amd64",
            "Os" => "linux",
            "Config" => %{
              "User" => "10001:10001",
              "Labels" => %{"io.favn.runner-release-id" => runner.runner_release_id}
            }
          }

          {JSON.encode!([inspection]), 0}

        other ->
          {"unexpected Docker command: #{inspect(other)}", 97}
      end
    end
  end
end
