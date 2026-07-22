defmodule Favn.Dev.ComposeLifecycleTest do
  use ExUnit.Case, async: false

  alias Favn.Dev.{ComposeLifecycle, Install, Lock, Paths, Reset, RunnerImage, State}
  alias Favn.RunnerRelease

  @version "0.5.0-dev"
  @control_build_id String.duplicate("a", 64)
  @control_image_id "sha256:" <> String.duplicate("b", 64)

  setup do
    root_dir =
      Path.join(
        Path.expand("../../../_build/test-artifacts", __DIR__),
        "favn_compose_lifecycle_test_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(root_dir)

    {:ok, state} =
      Agent.start_link(fn ->
        %{runner: descriptor("one"), commands: [], services: %{}, events: []}
      end)

    docker = docker_runner(state)

    opts = [
      root_dir: root_dir,
      favn_version: @version,
      docker_executable: "docker",
      docker_command_runner: docker,
      candidate_control_plane: %{
        "reference" => "favn-control-plane-candidate:#{@control_build_id}",
        "image_id" => @control_image_id
      }
    ]

    assert {:ok, :installed} = Install.run(opts)

    on_exit(fn -> File.rm_rf(root_dir) end)

    %{root_dir: root_dir, state: state, opts: opts}
  end

  test "ordered startup uses release operations and immutable local images", context do
    opts =
      context
      |> lifecycle_opts()
      |> Keyword.put(:liveness_fun, fn _url ->
        record_event(context.state, :control_plane_live)
        :ok
      end)
      |> Keyword.put(:readiness_fun, fn _url ->
        record_event(context.state, :control_plane_ready)
        :ok
      end)

    assert {:ok, result} = ComposeLifecycle.start(opts)
    assert result.runner_release_id == Agent.get(context.state, & &1.runner.runner_release_id)
    events = events(context.state)

    assert event_index(events, :control_plane_live) <
             event_index(events, {:deploy, result.runner_release_id})

    assert event_index(events, {:deploy, result.runner_release_id}) <
             event_index(events, :control_plane_ready)

    assert %{stack_status: :running, services: services, runtime: %{"status" => "ok"}} =
             ComposeLifecycle.status(opts)

    assert services["postgres"].health == :healthy
    assert services["runner"].health == :healthy
    assert services["control-plane"].health == :healthy

    commands = commands(context.state)

    assert command_index(commands, ["up", "--detach", "--wait", "postgres"]) <
             operation_index(commands, "migrate")

    assert operation_index(commands, "migrate") < operation_index(commands, "grant-runtime")
    assert operation_index(commands, "grant-runtime") < operation_index(commands, "verify-schema")

    assert operation_index(commands, "verify-schema") <
             operation_index(commands, "provision-workspace")

    assert operation_index(commands, "provision-workspace") <
             command_index(commands, ["up", "--detach", "--wait", "--no-deps", "runner"])

    assert {:ok, runtime} = State.read_runtime(root_dir: context.root_dir)
    assert runtime["kind"] == "docker_compose"
    assert runtime["runner_release_id"] == result.runner_release_id
    assert runtime["active_manifest_version_id"] == result.manifest_version_id

    assert {:ok, install} = State.read_install(root_dir: context.root_dir)
    env = File.read!(install["compose"]["env_path"])

    expected_image =
      RunnerImage.image_reference(install["compose"]["project_name"], result.runner_release_id)

    assert env =~ "FAVN_RUNNER_IMAGE='#{expected_image}'"

    runner_env = File.read!(install["compose"]["runner_env_path"])
    assert runner_env =~ ~s(FAVN_CUSTOM_TOKEN='runtime-secret')
    refute File.read!(install["compose"]["compose_path"]) =~ "runtime-secret"
  end

  test "manifest-only reload leaves the runner container unchanged", context do
    opts = lifecycle_opts(context)
    assert {:ok, _result} = ComposeLifecycle.start(opts)
    before = commands(context.state)

    assert :ok = ComposeLifecycle.reload(opts)
    after_reload = commands(context.state) -- before

    refute Enum.any?(after_reload, &command_suffix?(&1, ["stop", "--timeout", "180", "runner"]))
    refute Enum.any?(after_reload, &Enum.member?(&1, "--force-recreate"))
  end

  test "a successful stop clears a now-defunct local maintenance lease", context do
    opts = lifecycle_opts(context)
    assert {:ok, _result} = ComposeLifecycle.start(opts)

    assert :ok =
             State.write_maintenance(
               %{
                 "schema_version" => 1,
                 "kind" => "runner_replacement",
                 "token" => String.duplicate("a", 43)
               },
               root_dir: context.root_dir
             )

    assert :ok = ComposeLifecycle.stop(opts)
    assert {:error, :not_found} = State.read_maintenance(root_dir: context.root_dir)
  end

  test "start applies scheduler choice and reload refreshes the effective runner environment",
       context do
    opts =
      context
      |> lifecycle_opts()
      |> Keyword.put(:scheduler, true)

    assert {:ok, _result} = ComposeLifecycle.start(opts)
    {:ok, install} = State.read_install(root_dir: context.root_dir)
    assert File.read!(install["compose"]["env_path"]) =~ "FAVN_SCHEDULER_ENABLED='true'"

    assert :ok =
             ComposeLifecycle.reload(
               Keyword.put(opts, :env_file_loaded, %{"FAVN_CUSTOM_TOKEN" => "rotated-secret"})
             )

    assert File.read!(install["compose"]["runner_env_path"]) =~
             ~s(FAVN_CUSTOM_TOKEN='rotated-secret')
  end

  test "runtime change checks for active work before replacing only runner", context do
    opts = lifecycle_opts(context)
    assert {:ok, _result} = ComposeLifecycle.start(opts)

    Agent.update(context.state, &Map.put(&1, :runner, descriptor("two")))
    assert :ok = ComposeLifecycle.reload(opts)

    assert_receive :runner_replacement_begin
    assert_receive :runner_replacement_status
    assert_receive {:runner_replacement_verify, replacement_release_id}
    assert {:deploy, replacement_release_id} in events(context.state)
    assert_receive :runner_replacement_finish

    commands = commands(context.state)
    assert Enum.any?(commands, &command_suffix?(&1, ["stop", "--timeout", "180", "runner"]))

    assert Enum.any?(commands, fn command ->
             command_suffix?(command, [
               "up",
               "--detach",
               "--wait",
               "--no-deps",
               "--force-recreate",
               "runner"
             ])
           end)

    refute Enum.any?(
             commands,
             &command_suffix?(&1, ["stop", "--timeout", "180", "control-plane"])
           )
  end

  test "blocked drain leaves the running runner selection and state unchanged", context do
    opts = lifecycle_opts(context)
    assert {:ok, initial} = ComposeLifecycle.start(opts)
    command_count = length(commands(context.state))

    Agent.update(context.state, &Map.put(&1, :runner, descriptor("blocked")))

    assert {:error, {:in_flight_runs, ["run-active"]}} =
             ComposeLifecycle.reload(
               opts
               |> Keyword.put(:in_flight_fun, fn _project -> {:ok, ["run-active"]} end)
               |> Keyword.put(:runner_drain_timeout_ms, 0)
             )

    new_commands = commands(context.state) |> Enum.drop(command_count)

    refute Enum.any?(
             new_commands,
             &command_suffix?(&1, ["stop", "--timeout", "180", "runner"])
           )

    refute Enum.any?(new_commands, &Enum.member?(&1, "--force-recreate"))

    assert {:ok, latest} = State.read_runner_latest(root_dir: context.root_dir)
    assert latest["runner_release_id"] == initial.runner_release_id

    assert {:ok, install} = State.read_install(root_dir: context.root_dir)
    env = File.read!(install["compose"]["env_path"])

    expected_image =
      RunnerImage.image_reference(install["compose"]["project_name"], initial.runner_release_id)

    assert env =~ "FAVN_RUNNER_IMAGE='#{expected_image}'"
    assert_receive :runner_replacement_finish
    assert {:error, :not_found} = State.read_maintenance(root_dir: context.root_dir)
  end

  test "failed replacement keeps maintenance active when rollback cannot be verified", context do
    opts = lifecycle_opts(context)
    assert {:ok, initial} = ComposeLifecycle.start(opts)
    Agent.update(context.state, &Map.put(&1, :runner, descriptor("rollback-failure")))
    base_runner = docker_runner(context.state)

    failing_runner = fn executable, args, command_opts ->
      if command_suffix?(args, [
           "up",
           "--detach",
           "--wait",
           "--no-deps",
           "--force-recreate",
           "runner"
         ]) do
        Agent.update(context.state, fn state ->
          Map.update!(state, :commands, &(&1 ++ [args]))
        end)

        {"runner recreation failed", 1}
      else
        base_runner.(executable, args, command_opts)
      end
    end

    assert {:error,
            {:runner_replacement_rollback_failed,
             {:compose_command_failed, :runner_replacement, 1, _replacement_output},
             {:compose_command_failed, :runner_rollback, 1, _rollback_output}}} =
             ComposeLifecycle.reload(Keyword.put(opts, :docker_command_runner, failing_runner))

    refute_received :runner_replacement_finish
    assert {:ok, maintenance} = State.read_maintenance(root_dir: context.root_dir)
    assert maintenance["kind"] == "runner_replacement"
    assert is_binary(maintenance["token"])

    assert {:ok, latest} = State.read_runner_latest(root_dir: context.root_dir)
    assert latest["runner_release_id"] == initial.runner_release_id

    assert :ok = ComposeLifecycle.reload(opts)
    assert_receive :runner_replacement_finish
    assert {:error, :not_found} = State.read_maintenance(root_dir: context.root_dir)

    assert {:ok, recovered} = State.read_runner_latest(root_dir: context.root_dir)
    assert recovered["runner_release_id"] != initial.runner_release_id
  end

  test "an interrupted CLI resumes from the persisted pre-build runner identity", context do
    opts = lifecycle_opts(context)
    assert {:ok, initial} = ComposeLifecycle.start(opts)
    assert {:ok, initial_state} = State.read_runner_latest(root_dir: context.root_dir)
    Agent.update(context.state, &Map.put(&1, :runner, descriptor("interrupted")))

    parent = self()

    {:ok, killing_client} =
      Agent.start_link(fn -> parent end, name: __MODULE__.KillingOrchestratorClient)

    on_exit(fn ->
      if Process.alive?(killing_client), do: Agent.stop(killing_client)
    end)

    interrupted_opts =
      Keyword.put(opts, :orchestrator_client, __MODULE__.KillingOrchestratorClient)

    pid = spawn(fn -> ComposeLifecycle.reload(interrupted_opts) end)
    monitor = Process.monitor(pid)

    assert_receive {:begin_before_kill, persisted_token}, 5_000
    assert_receive {:DOWN, ^monitor, :process, ^pid, :killed}, 5_000

    assert {:ok, persisted} = State.read_maintenance(root_dir: context.root_dir)
    assert persisted["phase"] == "active"
    assert persisted["token"] == persisted_token
    assert persisted["previous_runner"]["runner_release_id"] == initial.runner_release_id
    assert persisted["previous_runner"]["state"] == initial_state

    assert persisted["active_manifest"] == %{
             "manifest_version_id" => initial.manifest_version_id,
             "required_runner_release_id" => initial.runner_release_id
           }

    assert {:ok, interrupted_latest} = State.read_runner_latest(root_dir: context.root_dir)
    refute interrupted_latest["runner_release_id"] == initial.runner_release_id

    base_runner = docker_runner(context.state)
    {:ok, failures} = Agent.start_link(fn -> 0 end)

    fail_candidate_once = fn executable, args, command_opts ->
      force_recreate? =
        command_suffix?(args, [
          "up",
          "--detach",
          "--wait",
          "--no-deps",
          "--force-recreate",
          "runner"
        ])

      failure_number =
        if force_recreate? do
          Agent.get_and_update(failures, fn count -> {count, count + 1} end)
        end

      if force_recreate? and failure_number == 0 do
        Agent.update(context.state, fn state ->
          Map.update!(state, :commands, &(&1 ++ [args]))
        end)

        {"interrupted candidate failed", 1}
      else
        base_runner.(executable, args, command_opts)
      end
    end

    assert {:error, {:compose_command_failed, :runner_replacement, 1, _output}} =
             ComposeLifecycle.reload(
               Keyword.put(opts, :docker_command_runner, fail_candidate_once)
             )

    assert_receive {:runner_replacement_verify, verified_release_id}
    assert verified_release_id == initial.runner_release_id
    assert_receive :runner_replacement_finish
    assert {:error, :not_found} = State.read_maintenance(root_dir: context.root_dir)

    assert {:ok, restored} = State.read_runner_latest(root_dir: context.root_dir)
    assert restored == initial_state
  end

  test "rollback remains in maintenance when the active manifest changed", context do
    opts = lifecycle_opts(context)
    assert {:ok, initial} = ComposeLifecycle.start(opts)
    Agent.update(context.state, &Map.put(&1, :runner, descriptor("manifest-mismatch")))

    {:ok, active_manifest} =
      Agent.start_link(fn ->
        %{
          "manifest_version_id" => initial.manifest_version_id,
          "required_runner_release_id" => initial.runner_release_id
        }
      end)

    failing_deploy = fn _project, runner, _deploy_opts ->
      Agent.update(active_manifest, fn _previous ->
        %{
          "manifest_version_id" => runner.manifest_version_id,
          "required_runner_release_id" => runner.runner_release_id
        }
      end)

      {:error, :activation_response_lost}
    end

    reload_opts =
      opts
      |> Keyword.put(:active_manifest_fun, fn _project ->
        {:ok, Agent.get(active_manifest, & &1)}
      end)
      |> Keyword.put(:deploy_fun, failing_deploy)

    assert {:error,
            {:runner_replacement_rollback_failed, :activation_response_lost,
             {:active_manifest_mismatch, expected, actual}}} =
             ComposeLifecycle.reload(reload_opts)

    assert expected["manifest_version_id"] == initial.manifest_version_id
    refute actual["manifest_version_id"] == initial.manifest_version_id
    refute_received :runner_replacement_finish

    assert {:ok, maintenance} = State.read_maintenance(root_dir: context.root_dir)
    assert maintenance["phase"] == "active"

    assert {:ok, restored} = State.read_runner_latest(root_dir: context.root_dir)
    assert restored["runner_release_id"] == initial.runner_release_id
  end

  test "a lost begin response resumes with the persisted maintenance lease", context do
    opts = lifecycle_opts(context)
    assert {:ok, _initial} = ComposeLifecycle.start(opts)
    Agent.update(context.state, &Map.put(&1, :runner, descriptor("resumed")))

    parent = self()

    {:ok, recovery_state} =
      Agent.start_link(fn -> %{attempts: 0, parent: parent} end,
        name: __MODULE__.RecoveringOrchestratorClient
      )

    on_exit(fn ->
      if Process.alive?(recovery_state), do: Agent.stop(recovery_state)
    end)

    {:ok, probes} = Agent.start_link(fn -> %{liveness: 0, readiness: 0} end)

    recovery_opts =
      opts
      |> Keyword.put(:orchestrator_client, __MODULE__.RecoveringOrchestratorClient)
      |> Keyword.put(:liveness_fun, fn _url ->
        Agent.update(probes, &Map.update!(&1, :liveness, fn count -> count + 1 end))
        :ok
      end)
      |> Keyword.put(:readiness_fun, fn _url ->
        Agent.update(probes, &Map.update!(&1, :readiness, fn count -> count + 1 end))
        :ok
      end)

    assert {:error, :begin_response_lost} =
             Task.async(fn -> ComposeLifecycle.reload(recovery_opts) end)
             |> Task.await(30_000)

    assert {:ok, persisted} = State.read_maintenance(root_dir: context.root_dir)
    persisted_token = persisted["token"]
    assert persisted["phase"] == "active"

    assert persisted["previous_runner"]["runner_release_id"] !=
             Agent.get(context.state, & &1.runner.runner_release_id)

    assert_receive {:begin_attempt, 1, ^persisted_token}

    assert :ok =
             Task.async(fn -> ComposeLifecycle.reload(recovery_opts) end)
             |> Task.await(30_000)

    assert_receive {:begin_attempt, 2, ^persisted_token}
    assert {:error, :not_found} = State.read_maintenance(root_dir: context.root_dir)
    assert Agent.get(probes, & &1) == %{liveness: 1, readiness: 2}
  end

  test "failed startup preserves bounded Compose diagnostics before cleanup", context do
    base_runner = docker_runner(context.state)
    {:ok, secrets} = State.read_secrets(root_dir: context.root_dir)
    secret = secrets["service_token"]

    failing_runner = fn executable, args, opts ->
      cond do
        Enum.take(args, -5) == [
          "up",
          "--detach",
          "--wait",
          "--no-deps",
          "control-plane"
        ] ->
          {"control-plane failed token=#{secret} ecto://user:password@postgres/db", 1}

        Enum.take(args, -5) == ["logs", "--tail", "200", "--no-color", "control-plane"] ->
          {"service secret=#{secret}", 0}

        true ->
          base_runner.(executable, args, opts)
      end
    end

    opts = lifecycle_opts(context) |> Keyword.put(:docker_command_runner, failing_runner)

    assert {:error, {:compose_command_failed, :control_plane, 1, output}} =
             ComposeLifecycle.start(opts)

    assert output =~ "control-plane failed"
    assert output =~ "[REDACTED]"
    refute output =~ secret
    refute output =~ "ecto://user:password@postgres/db"

    path = Paths.compose_failure_log_path(context.root_dir)
    persisted = File.read!(path)
    assert persisted =~ "control-plane failed"
    refute persisted =~ secret
    refute persisted =~ "ecto://user:password@postgres/db"
    assert Bitwise.band(File.stat!(path).mode, 0o777) == 0o600

    assert {:ok, last_failure} = State.read_last_failure(root_dir: context.root_dir)
    refute last_failure["error"] =~ secret
    refute last_failure["error"] =~ "ecto://user:password@postgres/db"

    assert Enum.any?(commands(context.state), fn command ->
             command_suffix?(command, [
               "stop",
               "--timeout",
               "30",
               "control-plane",
               "runner",
               "postgres"
             ])
           end)
  end

  test "logs accepts only services in the installed Compose topology", context do
    opts = lifecycle_opts(context)

    assert :ok = ComposeLifecycle.logs(Keyword.put(opts, :service, :control_plane))

    assert Enum.any?(commands(context.state), fn command ->
             command_suffix?(command, [
               "logs",
               "--tail",
               "100",
               "--no-color",
               "control-plane"
             ])
           end)

    assert {:error, {:invalid_service, :orchestrator}} =
             ComposeLifecycle.logs(Keyword.put(opts, :service, :orchestrator))
  end

  test "start rejects full and partial running stacks before runtime mutation", context do
    {:ok, install} = State.read_install(root_dir: context.root_dir)
    env_path = install["compose"]["env_path"]
    original_env = File.read!(env_path)

    cases = [
      {%{"postgres" => :running, "runner" => :running, "control-plane" => :running},
       :stack_already_running},
      {%{"runner" => :running, "control-plane" => :running},
       {:stack_partially_running,
        %{"postgres" => :stopped, "runner" => :running, "control-plane" => :running}}}
    ]

    for {services, expected_error} <- cases do
      Agent.update(context.state, fn state ->
        %{state | services: services, commands: []}
      end)

      assert {:error, ^expected_error} = ComposeLifecycle.start(lifecycle_opts(context))
      assert File.read!(env_path) == original_env
      assert {:error, :not_found} = State.read_runner_latest(root_dir: context.root_dir)

      refute Enum.any?(commands(context.state), fn command ->
               Enum.member?(command, "up") or Enum.member?(command, "stop")
             end)

      refute Enum.any?(commands(context.state), fn command ->
               Enum.any?(command, &String.starts_with?(&1, "favn-local-runner-"))
             end)
    end
  end

  test "lifecycle mutations fail boundedly while another command owns the project lock",
       context do
    parent = self()

    holder =
      Task.async(fn ->
        Lock.with_lock(context.opts, fn ->
          send(parent, :lock_acquired)

          receive do
            :release_lock -> :ok
          end
        end)
      end)

    assert_receive :lock_acquired
    command_count = length(commands(context.state))

    assert {:error, {:lock_failed, :timeout}} =
             ComposeLifecycle.start(
               context
               |> lifecycle_opts()
               |> Keyword.put(:lock_timeout_ms, 0)
             )

    assert length(commands(context.state)) == command_count
    send(holder.pid, :release_lock)
    assert :ok = Task.await(holder)
  end

  test "reset refuses without confirmation and removes only scoped resources with yes", context do
    resources = Reset.plan(root_dir: context.root_dir)

    assert {:error, {:confirmation_required, ^resources}} =
             Reset.run(root_dir: context.root_dir)

    assert File.dir?(Paths.favn_dir(context.root_dir))

    assert :ok =
             Reset.run(
               root_dir: context.root_dir,
               yes: true,
               docker_executable: "docker",
               docker_command_runner: docker_runner(context.state)
             )

    refute File.exists?(Paths.favn_dir(context.root_dir))
    assert Enum.any?(commands(context.state), &Enum.member?(&1, "--volumes"))
  end

  defp lifecycle_opts(context) do
    context.opts ++
      [
        runner_build_fun: &write_runner(&1, context.state),
        readiness_fun: fn _url -> :ok end,
        deploy_fun: fn project, runner, opts ->
          record_event(context.state, {:deploy, runner.runner_release_id})
          deploy(project, runner, opts)
        end,
        orchestrator_client: __MODULE__.FakeOrchestratorClient,
        active_manifest_fun: fn _project ->
          with {:ok, runtime} <- State.read_runtime(root_dir: context.root_dir) do
            {:ok,
             %{
               "manifest_version_id" => runtime["active_manifest_version_id"],
               "required_runner_release_id" => runtime["runner_release_id"]
             }}
          end
        end,
        progress_fun: fn _message -> :ok end,
        env_file_loaded: %{"FAVN_CUSTOM_TOKEN" => "runtime-secret"},
        ready_poll_interval_ms: 1
      ]
  end

  defp write_runner(opts, state) do
    descriptor = Agent.get(state, & &1.runner)
    root_dir = Paths.root_dir(opts)
    dist_dir = Paths.dist_runner_dir(root_dir, descriptor.runner_release_id)

    manifest_version_id =
      descriptor.runner_release_id
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.encode16(case: :lower)

    manifest_dir = Paths.dist_manifest_dir(root_dir, manifest_version_id)
    descriptor_path = Path.join(dist_dir, "runner-release.json")

    File.mkdir_p!(dist_dir)
    File.mkdir_p!(manifest_dir)
    {:ok, encoded} = RunnerRelease.encode(descriptor)
    File.write!(descriptor_path, encoded)
    File.write!(Path.join(manifest_dir, "manifest-index.json"), "{}\n")

    State.write_runner_latest(
      %{
        "schema_version" => 1,
        "runner_release_id" => descriptor.runner_release_id,
        "dist_dir" => dist_dir,
        "descriptor_path" => descriptor_path,
        "manifest_dir" => manifest_dir,
        "manifest_version_id" => manifest_version_id
      },
      opts
    )
  end

  defp deploy(_project, runner, _opts) do
    published = %{
      manifest_version_id: runner.manifest_version_id,
      required_runner_release_id: runner.runner_release_id,
      status: "published"
    }

    {:ok, %{published: published, activated: %{activated?: true}}}
  end

  defmodule FakeOrchestratorClient do
    def in_flight_runs(_url, _token, _context), do: {:ok, []}
    def diagnostics(_url, _token, _context), do: {:ok, %{"status" => "ok"}}

    def begin_runner_replacement(_url, _token, maintenance_token) do
      send(self(), :runner_replacement_begin)
      {:ok, maintenance_token}
    end

    def runner_replacement_status(_url, _token) do
      send(self(), :runner_replacement_status)

      {:ok,
       %{
         "maintenance?" => true,
         "maintenance_kind" => "runner_replacement",
         "active_admissions" => 0
       }}
    end

    def verify_replacement_runner(_url, _token, _maintenance_token, runner_release_id) do
      send(self(), {:runner_replacement_verify, runner_release_id})
      {:ok, %{"runner_release_id" => runner_release_id}}
    end

    def finish_runner_replacement(_url, _token, _maintenance_token) do
      send(self(), :runner_replacement_finish)
      :ok
    end
  end

  defmodule RecoveringOrchestratorClient do
    def in_flight_runs(_url, _token, _context), do: {:ok, []}
    def diagnostics(_url, _token, _context), do: {:ok, %{"status" => "ok"}}

    def begin_runner_replacement(_url, _service_token, maintenance_token) do
      %{attempt: attempt, parent: parent} =
        Agent.get_and_update(__MODULE__, fn state ->
          attempt = state.attempts + 1
          {%{attempt: attempt, parent: state.parent}, %{state | attempts: attempt}}
        end)

      send(parent, {:begin_attempt, attempt, maintenance_token})

      if attempt == 1,
        do: {:error, :begin_response_lost},
        else: {:ok, maintenance_token}
    end

    def runner_replacement_status(_url, _token) do
      {:ok,
       %{
         "maintenance?" => true,
         "maintenance_kind" => "runner_replacement",
         "active_admissions" => 0
       }}
    end

    def verify_replacement_runner(_url, _token, _maintenance_token, runner_release_id) do
      {:ok, %{"runner_release_id" => runner_release_id}}
    end

    def finish_runner_replacement(_url, _token, _maintenance_token), do: :ok
  end

  defmodule KillingOrchestratorClient do
    def begin_runner_replacement(_url, _service_token, maintenance_token) do
      parent = Agent.get(__MODULE__, & &1)
      send(parent, {:begin_before_kill, maintenance_token})
      Process.exit(self(), :kill)
    end
  end

  defp docker_runner(state) do
    fn "docker", args, _opts ->
      Agent.update(state, &Map.update!(&1, :commands, fn commands -> commands ++ [args] end))

      case args do
        ["version", "--format", "{{json .Server}}"] ->
          {JSON.encode!(%{"Os" => "linux", "Arch" => "amd64", "Version" => "28.3.0"}), 0}

        ["compose", "version", "--short"] ->
          {"2.39.1\n", 0}

        ["image", "inspect", reference] ->
          {JSON.encode!([image_inspection(reference, state)]), 0}

        ["image", "rm" | _references] ->
          {"", 0}

        ["compose" | compose_args] ->
          compose_response(compose_args, state)

        other ->
          {"unexpected docker command: #{inspect(other)}", 97}
      end
    end
  end

  defp compose_response(args, state) do
    cond do
      Enum.take(args, -2) == ["config", "--quiet"] ->
        {"", 0}

      Enum.take(args, -4) == ["ps", "--all", "--format", "json"] ->
        {compose_ps(state, true), 0}

      Enum.take(args, -3) == ["ps", "--format", "json"] ->
        {compose_ps(state, false), 0}

      Enum.at(args, -2) == "--quiet" and Enum.at(args, -3) == "ps" ->
        service = List.last(args)
        services = Agent.get(state, & &1.services)

        if Map.get(services, service) == :running,
          do: {"container-#{service}\n", 0},
          else: {"", 0}

      Enum.member?(args, "up") ->
        service = List.last(args)
        set_service(state, service, :running)
        {"", 0}

      Enum.member?(args, "stop") ->
        stop_index = Enum.find_index(args, &(&1 == "stop"))
        services = Enum.drop(args, stop_index + 3)
        Enum.each(services, &set_service(state, &1, :stopped))
        {"", 0}

      Enum.member?(args, "down") ->
        Agent.update(state, &Map.put(&1, :services, %{}))
        {"", 0}

      true ->
        {"", 0}
    end
  end

  defp compose_ps(state, include_stopped?) do
    state
    |> Agent.get(& &1.services)
    |> Enum.filter(fn {_service, status} -> include_stopped? or status == :running end)
    |> Enum.sort()
    |> Enum.map_join("\n", fn {service, status} ->
      JSON.encode!(%{
        "Service" => service,
        "State" => if(status == :running, do: "running", else: "exited"),
        "Health" => if(status == :running, do: "healthy", else: ""),
        "Image" => "sha256:test"
      })
    end)
  end

  defp set_service(state, service, status) do
    if service in ["postgres", "runner", "control-plane"] do
      Agent.update(
        state,
        &update_in(&1.services, fn services -> Map.put(services, service, status) end)
      )
    end
  end

  defp image_inspection(reference, state) do
    if String.starts_with?(reference, "favn-local-runner-") do
      release_id = reference |> String.split(":") |> List.last()

      %{
        "Id" => "sha256:" <> release_id,
        "RepoDigests" => [],
        "Architecture" => "amd64",
        "Os" => "linux",
        "Config" => %{
          "User" => "10001:10001",
          "Labels" => %{
            "io.favn.runner-release-id" => release_id,
            "io.favn.version" => RunnerRelease.current_favn_version(),
            "io.favn.runner-contract-version" =>
              Favn.Manifest.Compatibility.current_runner_contract_version()
              |> Integer.to_string(),
            "io.favn.target" => "linux/amd64"
          }
        }
      }
    else
      control_image_inspection(state)
    end
  end

  defp control_image_inspection(_state) do
    %{
      "Id" => @control_image_id,
      "RepoDigests" => [],
      "Architecture" => "amd64",
      "Os" => "linux",
      "Config" => %{
        "User" => "10001:10001",
        "Labels" => %{
          "org.opencontainers.image.version" => @version,
          "io.favn.control-plane.build-id" => @control_build_id,
          "io.favn.manifest-schema-version" =>
            Favn.Manifest.Compatibility.current_schema_version() |> Integer.to_string(),
          "io.favn.runner-contract-version" =>
            Favn.Manifest.Compatibility.current_runner_contract_version() |> Integer.to_string(),
          "io.favn.target" => "linux/amd64"
        }
      }
    }
  end

  defp descriptor(module_name) do
    {:ok, descriptor} =
      RunnerRelease.new(%{
        schema_version: RunnerRelease.current_schema_version(),
        favn_version: RunnerRelease.current_favn_version(),
        runner_contract_version: Favn.Manifest.Compatibility.current_runner_contract_version(),
        elixir_version: System.version(),
        otp_release: to_string(:erlang.system_info(:otp_release)),
        target: RunnerRelease.current_target(),
        runtime_modules: [
          %{
            module: "Elixir.Fixture.#{module_name}",
            digest: :crypto.hash(:sha256, module_name) |> Base.encode16(case: :lower)
          }
        ],
        runtime_applications: [],
        plugins: [],
        build_profile: "prod"
      })

    descriptor
  end

  defp commands(state), do: Agent.get(state, & &1.commands)
  defp events(state), do: Agent.get(state, & &1.events)

  defp record_event(state, event) do
    Agent.update(state, &Map.update!(&1, :events, fn events -> events ++ [event] end))
  end

  defp event_index(events, event) do
    Enum.find_index(events, &(&1 == event)) || flunk("missing event #{inspect(event)}")
  end

  defp command_index(commands, suffix) do
    Enum.find_index(commands, &command_suffix?(&1, suffix)) || flunk("missing #{inspect(suffix)}")
  end

  defp operation_index(commands, operation) do
    Enum.find_index(commands, fn command ->
      List.last(command) == operation and
        Enum.at(command, -2) in ["control-plane-ops", "control-plane-verify"]
    end) || flunk("missing release operation #{operation}")
  end

  defp command_suffix?(command, suffix), do: Enum.take(command, -length(suffix)) == suffix
end
