defmodule Favn.Local.UserOwnedRunnerContainerTest do
  use ExUnit.Case, async: false

  alias Favn.Dev.{
    ComposeDeployment,
    ComposeLifecycle,
    ComposeProject,
    Docker,
    Install,
    Reset,
    State
  }

  alias Favn.Dev.Build.Manifest
  alias Favn.Dev.Init

  @moduletag :integration
  @moduletag :container
  @moduletag timeout: 1_200_000

  setup do
    candidate =
      System.get_env("FAVN_CONTROL_PLANE_CANDIDATE") ||
        raise "FAVN_CONTROL_PLANE_CANDIDATE must name the repository-built candidate image"

    {:ok, candidate_image} = Docker.inspect_image(candidate)
    repo_root = Path.expand("../../../..", __DIR__)
    root_dir = Path.join(repo_root, "apps/favn_local/acceptance/customer_runner_fixture")
    release_id = FavnTestSupport.runner_release_id(:primary)
    project_name = ComposeProject.project_name(root_dir)
    image = "favn-local/#{project_name}-runner:dev"

    on_exit(fn ->
      cleanup_project_resources(project_name)
      _ = System.cmd("docker", ["image", "rm", "--force", image], stderr_to_stdout: true)
      File.rm_rf(Path.join(root_dir, ".favn"))
      File.rm_rf(Path.join(root_dir, ".data"))
      File.rm_rf(Path.join(root_dir, "deploy"))
      File.rm(Path.join(root_dir, "mix.lock"))
    end)

    File.cp!(Path.join(repo_root, "mix.lock"), Path.join(root_dir, "mix.lock"))

    assert {:ok, _scaffold} =
             Init.run(
               root_dir: root_dir,
               app: :favn_runner_acceptance_customer,
               include: "duckdb-adbc@1.5.4",
               project_dependencies: [
                 {:favn, path: "../../../favn"},
                 {:favn_duckdb_adbc, path: "../../../favn_duckdb_adbc"}
               ]
             )

    opts = [
      root_dir: root_dir,
      runner_build_context: repo_root,
      runner_release_id_fun: fn -> release_id end,
      favn_version: Favn.RunnerRelease.current_favn_version(),
      candidate_control_plane: %{"reference" => candidate, "image_id" => candidate_image.id},
      web_port: free_port(),
      orchestrator_port: free_port(),
      progress_fun: fn _message -> :ok end,
      ready_timeout_ms: 180_000,
      compose_command_timeout_ms: 600_000,
      manifest_build_fun: &build_manifest/2,
      foreground: false
    ]

    assert {:ok, :installed} = Install.run(opts)

    on_exit(fn ->
      _ = ComposeLifecycle.stop(opts)
      _ = Reset.run(Keyword.put(opts, :yes, true))
    end)

    %{opts: opts, release_id: release_id, runner_image: image}
  end

  test "the scaffolded customer runner is built and starts with the control-plane candidate",
       context do
    assert {:ok, started} = ComposeLifecycle.start(context.opts)
    assert started.runner_release_id == context.release_id
    assert {:ok, runner_image} = Docker.inspect_image(context.runner_image)
    assert runner_image.id == started.runner_image_id
    assert runner_image.labels["io.favn.runner-release-id"] == context.release_id

    assert %{stack_status: :running, services: services} =
             ComposeLifecycle.status(context.opts)

    assert services.postgres.health == :healthy
    assert services.runner.health == :healthy
    assert services.control_plane.health == :healthy

    deployment = deployment!(context.opts)
    assert_control_plane_runtime_contract(deployment)
    assert_customer_code_loaded_but_not_started(deployment)
    assert_duckdb_adbc_driver_operational(deployment)
  end

  defp build_manifest(release_id, opts) do
    case Manifest.run(
           root_dir: Keyword.fetch!(opts, :root_dir),
           runner_release_id: release_id,
           allow_non_prod_build: true,
           skip_compile: true
         ) do
      {:ok, _manifest} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp deployment!(opts) do
    assert {:ok, runtime} = State.read_runtime(opts)
    assert {:ok, deployment} = ComposeDeployment.from_runtime(runtime, opts)
    deployment
  end

  defp assert_control_plane_runtime_contract(deployment) do
    container =
      compose_container_id!(deployment, ComposeDeployment.service!(deployment, :control_plane))

    expression = """
    loaded = Application.loaded_applications() |> Enum.map(&elem(&1, 0))

    if :favn_runner in loaded or Code.ensure_loaded?(FavnRunner) or Code.ensure_loaded?(Mix) do
      raise "control-plane runtime includes runner or Mix code"
    end
    """

    assert {output, 0} =
             System.cmd(
               "docker",
               [
                 "exec",
                 container,
                 "/app/bin/favn_control_plane",
                 "rpc",
                 expression
               ],
               stderr_to_stdout: true
             )

    refute output =~ "control-plane runtime includes"
  end

  defp assert_customer_code_loaded_but_not_started(deployment) do
    container = compose_container_id!(deployment, ComposeDeployment.service!(deployment, :runner))

    expression = """
    loaded = Application.loaded_applications() |> Enum.map(&elem(&1, 0))
    started = Application.started_applications() |> Enum.map(&elem(&1, 0))

    unless :favn_runner_acceptance_customer in loaded and
             :favn_runner_acceptance_customer not in started do
      raise "customer application load boundary violated"
    end
    """

    assert {output, 0} =
             System.cmd(
               "docker",
               [
                 "exec",
                 container,
                 "/opt/favn/bin/favn_runner",
                 "rpc",
                 expression
               ],
               stderr_to_stdout: true
             )

    refute output =~ "customer application load boundary violated"
  end

  defp assert_duckdb_adbc_driver_operational(deployment) do
    container = compose_container_id!(deployment, ComposeDeployment.service!(deployment, :runner))

    expression = """
    client = FavnDuckdbADBC.Runtime.client_module()
    opts = FavnDuckdbADBC.Runtime.driver_opts()

    {:ok, database} = client.open(":memory:", opts)

    try do
      {:ok, connection} = client.connection(database)

      try do
        {:ok, result} = client.query(connection, "SELECT 42 AS answer", [])
        [%{"answer" => 42}] = client.fetch_all(result, 1, 1024, bounded?: true)
      after
        client.release(connection)
      end
    after
      client.release(database)
    end
    """

    assert {output, 0} =
             System.cmd(
               "docker",
               [
                 "exec",
                 container,
                 "/opt/favn/bin/favn_runner",
                 "rpc",
                 expression
               ],
               stderr_to_stdout: true
             )

    refute output =~ "** ("
  end

  defp compose_container_id!(deployment, service) do
    assert {container, 0} = Docker.compose(deployment, ["ps", "--quiet", service])
    container = String.trim(container)
    assert container != ""
    container
  end

  defp cleanup_project_resources(project_name) do
    {containers, 0} =
      System.cmd("docker", [
        "container",
        "ls",
        "--all",
        "--quiet",
        "--filter",
        "label=com.docker.compose.project=#{project_name}"
      ])

    case String.split(containers, "\n", trim: true) do
      [] -> :ok
      ids -> _ = System.cmd("docker", ["container", "rm", "--force" | ids])
    end

    _ = System.cmd("docker", ["network", "rm", project_name <> "-network"])
    _ = System.cmd("docker", ["volume", "rm", "--force", project_name <> "-postgres-data"])
    :ok
  end

  defp free_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    port
  end
end
