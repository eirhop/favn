defmodule Favn.Local.ComposeAcceptanceTest do
  use ExUnit.Case, async: false

  alias Favn.Dev.{ComposeLifecycle, Doctor, Docker, Install, Paths, Reset, State}

  @moduletag :integration
  @moduletag :acceptance
  @moduletag timeout: 1_200_000

  @runner_environment %{
    "FAVN_ACCEPTANCE_DOLLARS" => "$HOME and ${UNSET_VALUE}",
    "FAVN_ACCEPTANCE_QUOTES" => "\"double\" and 'single'",
    "FAVN_ACCEPTANCE_SLASH_HASH" => "C:\\runtime\\path # literal",
    "FAVN_ACCEPTANCE_MULTILINE" => "first line\nsecond line"
  }

  setup do
    candidate =
      System.get_env("FAVN_CONTROL_PLANE_CANDIDATE") ||
        raise "FAVN_CONTROL_PLANE_CANDIDATE must name the repository-built candidate image"

    {:ok, image} = Docker.inspect_image(candidate)

    root_dir =
      Path.join(
        Path.expand("../../../../_build/test-artifacts", __DIR__),
        "favn_local_compose_acceptance_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(root_dir)
    File.mkdir_p!(Path.join(root_dir, "config"))
    File.write!(Path.join(root_dir, "mix.exs"), "defmodule Fixture.MixProject do end\n")
    File.write!(Path.join(root_dir, "config/config.exs"), "import Config\n")

    for app <- ~w(favn_runner favn_orchestrator favn_view) do
      File.mkdir_p!(Path.join(root_dir, "apps/#{app}"))
      File.write!(Path.join(root_dir, "apps/#{app}/mix.exs"), "defmodule Fixture do end")
    end

    File.write!(Path.join(root_dir, "mix.lock"), "lock")

    {:ok, build_state} = Agent.start_link(fn -> [] end)

    opts = [
      root_dir: root_dir,
      favn_version: Favn.RunnerRelease.current_favn_version(),
      candidate_control_plane: %{"reference" => candidate, "image_id" => image.id},
      web_port: free_port(),
      orchestrator_port: free_port(),
      progress_fun: fn _message -> :ok end,
      ready_timeout_ms: 180_000,
      runner_release_build_timeout_ms: 1_200_000,
      docker_build_timeout_ms: 1_200_000,
      compose_command_timeout_ms: 600_000,
      env_file_loaded: @runner_environment,
      runner_build_fun: fn build_opts ->
        build_runner(build_opts, Agent.get(build_state, & &1))
      end,
      foreground: false
    ]

    assert {:ok, :installed} = Install.run(opts)

    on_exit(fn ->
      _ = Reset.run(Keyword.put(opts, :yes, true))
      File.rm_rf(root_dir)
    end)

    %{opts: opts, root_dir: root_dir, build_state: build_state}
  end

  test "the production-like local stack starts, deploys, stops, and restores", context do
    assert {:ok, first} = ComposeLifecycle.start(context.opts)

    assert %{stack_status: :running, services: services} =
             ComposeLifecycle.status(context.opts)

    assert services["postgres"].health == :healthy
    assert services["runner"].health == :healthy
    assert services["control-plane"].health == :healthy

    assert {:ok, install} = State.read_install(root_dir: context.root_dir)
    project = install["compose"]
    compose = File.read!(install["compose"]["compose_path"])
    refute compose =~ "/home/"
    refute compose =~ "5432:5432"
    refute compose =~ "4369:4369"
    refute compose =~ "9100:9100"

    running = inspect_services(project)
    assert running["runner"]["Mounts"] == []
    assert running["control-plane"]["Mounts"] == []
    assert running["runner"]["HostConfig"]["PortBindings"] == %{}
    assert running["postgres"]["HostConfig"]["PortBindings"] == %{}

    assert Map.take(container_environment(running["runner"]), Map.keys(@runner_environment)) ==
             @runner_environment

    assert %{"HostIp" => "127.0.0.1"} =
             hd(running["control-plane"]["HostConfig"]["PortBindings"]["4000/tcp"])

    assert %{"HostIp" => "127.0.0.1"} =
             hd(running["control-plane"]["HostConfig"]["PortBindings"]["4101/tcp"])

    assert Enum.all?(running, fn {_service, inspection} ->
             inspection["HostConfig"]["NetworkMode"] == project["network_name"]
           end)

    assert :ok = ComposeLifecycle.reload(context.opts)
    reloaded = inspect_services(project)

    for service <- ["postgres", "runner", "control-plane"] do
      assert reloaded[service]["Image"] == running[service]["Image"]
      assert reloaded[service]["State"]["StartedAt"] == running[service]["State"]["StartedAt"]
    end

    Agent.update(context.build_state, fn _modules -> [Favn.Dev.Paths] end)
    assert :ok = ComposeLifecycle.reload(context.opts)
    replaced = inspect_services(project)

    assert replaced["runner"]["Image"] != reloaded["runner"]["Image"]

    assert replaced["runner"]["State"]["StartedAt"] !=
             reloaded["runner"]["State"]["StartedAt"]

    for service <- ["postgres", "control-plane"] do
      assert replaced[service]["Image"] == reloaded[service]["Image"]
      assert replaced[service]["State"]["StartedAt"] == reloaded[service]["State"]["StartedAt"]
    end

    assert {:ok, replacement_latest} = State.read_runner_latest(root_dir: context.root_dir)
    assert replacement_latest["runner_release_id"] != first.runner_release_id
    assert {:ok, replacement_runtime} = State.read_runtime(root_dir: context.root_dir)

    Agent.update(context.build_state, fn _modules -> [Favn.Dev.Paths, Favn.Dev.Config] end)

    assert {:error, {:in_flight_runs, ["run-blocking-reload"]}} =
             ComposeLifecycle.reload(
               context.opts
               |> Keyword.put(:runner_drain_timeout_ms, 0)
               |> Keyword.put(:in_flight_fun, fn _project ->
                 {:ok, ["run-blocking-reload"]}
               end)
             )

    blocked = inspect_services(project)
    assert blocked["runner"]["Image"] == replaced["runner"]["Image"]
    assert blocked["runner"]["State"]["StartedAt"] == replaced["runner"]["State"]["StartedAt"]

    assert {:ok, after_blocked_latest} = State.read_runner_latest(root_dir: context.root_dir)
    assert after_blocked_latest["runner_release_id"] == replacement_latest["runner_release_id"]
    assert {:ok, after_blocked_runtime} = State.read_runtime(root_dir: context.root_dir)

    assert after_blocked_runtime["active_manifest_version_id"] ==
             replacement_runtime["active_manifest_version_id"]

    assert {:ok, diagnostics} = ComposeLifecycle.diagnostics(context.opts)
    assert diagnostics["status"] == "ok"

    assert {:ok, secrets} = State.read_secrets(root_dir: context.root_dir)
    encoded_diagnostics = JSON.encode!(diagnostics)

    refute secrets
           |> Map.values()
           |> Enum.filter(&(is_binary(&1) and &1 != ""))
           |> Enum.any?(&String.contains?(encoded_diagnostics, &1))

    {_doctor_result, doctor_checks} = normalize_doctor_result(Doctor.run(context.opts))

    for check_name <- ["docker", "control-plane install", "compose isolation", "compose runtime"] do
      assert %{status: :ok} = Enum.find(doctor_checks, &(&1.name == check_name))
    end

    logs = ExUnit.CaptureIO.capture_io(fn -> assert :ok = ComposeLifecycle.logs(context.opts) end)

    refute secrets
           |> Map.values()
           |> Enum.filter(&(is_binary(&1) and &1 != ""))
           |> Enum.any?(&String.contains?(logs, &1))

    Agent.update(context.build_state, fn _modules -> [Favn.Dev.Paths] end)

    assert :ok = ComposeLifecycle.stop(context.opts)
    assert ComposeLifecycle.status(context.opts).stack_status == :stopped

    assert {:ok, second} = ComposeLifecycle.start(context.opts)
    assert second.runner_release_id == replacement_latest["runner_release_id"]
    assert second.runner_image_id == replacement_latest["image_id"]
    assert second.manifest_version_id == replacement_latest["manifest_version_id"]

    assert :ok = ComposeLifecycle.stop(context.opts)
    assert File.exists?(Paths.compose_path(context.root_dir))

    sentinel_volume = "favn-unrelated-#{System.unique_integer([:positive])}"
    {_output, 0} = System.cmd("docker", ["volume", "create", sentinel_volume])

    try do
      assert {:error, {:confirmation_required, resources}} = Reset.run(context.opts)
      assert resources.compose_project == project["project_name"]
      assert :ok = Reset.run(Keyword.put(context.opts, :yes, true))
      refute File.exists?(Paths.favn_dir(context.root_dir))
      {_output, 0} = System.cmd("docker", ["volume", "inspect", sentinel_volume])
    after
      _ = System.cmd("docker", ["volume", "rm", "--force", sentinel_volume])
    end
  end

  defp build_runner(opts, extra_modules) do
    Favn.Dev.build_runner(
      Keyword.merge(opts,
        skip_compile: true,
        skip_project_root_check: true,
        allow_non_prod_build: true,
        allow_unpinned_favn: true,
        extra_modules: extra_modules
      )
    )
  end

  defp normalize_doctor_result({:ok, checks}), do: {:ok, checks}
  defp normalize_doctor_result({:error, checks}), do: {:error, checks}

  defp free_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    port
  end

  defp inspect_services(project) do
    Map.new(["postgres", "runner", "control-plane"], fn service ->
      {container, 0} = Docker.compose(project, ["ps", "--quiet", service])
      container = String.trim(container)
      assert container != ""

      {encoded, 0} = System.cmd("docker", ["container", "inspect", container])
      {:ok, [inspection]} = JSON.decode(encoded)
      {service, inspection}
    end)
  end

  defp container_environment(inspection) do
    inspection["Config"]["Env"]
    |> Map.new(fn entry ->
      [key, value] = String.split(entry, "=", parts: 2)
      {key, value}
    end)
  end
end
