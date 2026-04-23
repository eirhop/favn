defmodule Favn.Dev.LifecycleTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias Favn.Dev
  alias Favn.Dev.Lock
  alias Favn.Dev.Paths
  alias Favn.Dev.Process, as: DevProcess
  alias Favn.Dev.State

  @run_real_stack_lifecycle? System.get_env("FAVN_RUN_DEV_LIFECYCLE") == "1" and
                               System.get_env("FAVN_RUN_DEV_LIFECYCLE_STACK") == "1"
  @real_stack_skip_reason "set FAVN_RUN_DEV_LIFECYCLE=1 and FAVN_RUN_DEV_LIFECYCLE_STACK=1 to run full local stack lifecycle integration"

  setup do
    root_dir =
      Path.join(System.tmp_dir!(), "favn_dev_lifecycle_#{System.unique_integer([:positive])}")

    File.mkdir_p!(root_dir)
    old_path = System.get_env("PATH")

    on_exit(fn ->
      if old_path, do: System.put_env("PATH", old_path), else: System.delete_env("PATH")
      File.rm_rf(root_dir)
    end)

    %{root_dir: root_dir}
  end

  @tag skip: if(@run_real_stack_lifecycle?, do: false, else: @real_stack_skip_reason)
  test "foreground lifecycle leaves lock free and supports second-terminal control" do
    root_dir = Path.expand("../../..", __DIR__)

    task = Task.async(fn -> Dev.dev(root_dir: root_dir) end)

    assert :ok =
             wait_until(fn ->
               match?(
                 {:ok, %{"services" => %{"runner" => _, "orchestrator" => _, "web" => _}}},
                 State.read_runtime(root_dir: root_dir)
               )
             end)

    assert :ok = Lock.with_lock([root_dir: root_dir], fn -> :ok end)
    assert %{stack_status: :running} = Dev.status(root_dir: root_dir)

    assert :ok = Dev.reload(root_dir: root_dir)
    assert %{stack_status: :running} = Dev.status(root_dir: root_dir)

    assert :ok = Dev.stop(root_dir: root_dir)
    assert %{stack_status: :stopped} = Dev.status(root_dir: root_dir)

    _ = Task.await(task, 60_000)
  end

  test "startup failure cleans runtime state", %{root_dir: root_dir} do
    :ok = State.ensure_layout(root_dir: root_dir)

    failing_specs = [
      %{
        name: "runner",
        exec: System.find_executable("bash") || "/bin/bash",
        args: ["-lc", "sleep 30"],
        cwd: root_dir,
        log_path: Paths.runner_log_path(root_dir),
        env: %{}
      },
      %{
        name: "orchestrator",
        exec: "/definitely/missing/executable",
        args: [],
        cwd: root_dir,
        log_path: Paths.orchestrator_log_path(root_dir),
        env: %{}
      }
    ]

    assert {:error, {:start_failed, "orchestrator", _reason}} =
             Dev.dev(
               root_dir: root_dir,
               service_specs_override: failing_specs,
               skip_install_check: true,
               skip_bootstrap: true,
               skip_readiness: true
             )

    assert {:error, :not_found} = State.read_runtime(root_dir: root_dir)
    assert {:ok, %{"error" => _}} = State.read_last_failure(root_dir: root_dir)
  end

  test "dev auto-clears stale runtime before continuing", %{root_dir: root_dir} do
    stale_runtime = %{
      "services" => %{
        "web" => %{"pid" => 999_999},
        "orchestrator" => %{"pid" => 999_998},
        "runner" => %{"pid" => 999_997}
      }
    }

    assert :ok = State.write_runtime(stale_runtime, root_dir: root_dir)

    assert {:error, :install_required} = Dev.dev(root_dir: root_dir, skip_runtime_compile: true)
    assert {:error, :not_found} = State.read_runtime(root_dir: root_dir)
  end

  test "dev reports partial runtime and does not clear state", %{root_dir: root_dir} do
    log_path = Paths.runner_log_path(root_dir)
    assert :ok = File.mkdir_p(Path.dirname(log_path))

    spec = %{
      name: "fixture",
      exec: System.find_executable("bash") || "/bin/bash",
      args: ["-lc", "sleep 30"],
      cwd: root_dir,
      log_path: log_path,
      env: %{}
    }

    assert {:ok, info} = DevProcess.start_service(spec)

    runtime = %{
      "services" => %{
        "web" => %{"pid" => 999_999},
        "orchestrator" => %{"pid" => 999_998},
        "runner" => %{"pid" => info.pid}
      }
    }

    assert :ok = State.write_runtime(runtime, root_dir: root_dir)

    assert {:error, {:stack_partially_running, states}} =
             Dev.dev(root_dir: root_dir, skip_runtime_compile: true)
    assert {"runner", :running} in states
    assert {:ok, _runtime} = State.read_runtime(root_dir: root_dir)

    assert :ok = Dev.stop(root_dir: root_dir)
    refute DevProcess.alive?(info.pid)
  end

  test "dev/1 returns explicit port conflict before startup", %{root_dir: root_dir} do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, {:active, false}, {:reuseaddr, false}])
    {:ok, port} = :inet.port(socket)

    try do
      assert {:error, {:port_conflict, :web, ^port}} =
               Dev.dev(
                root_dir: root_dir,
                web_port: port,
                skip_runtime_compile: true,
                skip_install_check: true,
                skip_bootstrap: true,
                skip_readiness: true
               )
    after
      :ok = :gen_tcp.close(socket)
    end
  end

  test "dev/1 returns explicit postgres unavailable diagnostics", %{root_dir: root_dir} do
    assert {:error, {:postgres_unavailable, "127.0.0.1", 1, _reason}} =
             Dev.dev(
               root_dir: root_dir,
               storage: :postgres,
                postgres: [
                  hostname: "127.0.0.1",
                  port: 1,
                 username: "postgres",
                 password: "postgres",
                 database: "favn",
                  ssl: false,
                  pool_size: 10
                ],
                skip_runtime_compile: true,
                skip_install_check: true,
                skip_bootstrap: true,
                skip_readiness: true
             )
  end

  test "dev/1 returns explicit postgres misconfiguration diagnostics", %{root_dir: root_dir} do
    assert {:error, {:postgres_misconfigured, :hostname}} =
             Dev.dev(
               root_dir: root_dir,
               storage: :postgres,
                postgres: [
                  hostname: "",
                  port: 5432,
                 username: "postgres",
                 password: "postgres",
                 database: "favn",
                  ssl: false,
                  pool_size: 10
                ],
                skip_runtime_compile: true,
                skip_install_check: true,
                skip_bootstrap: true,
                skip_readiness: true
              )
  end

  test "dev/1 fails runtime compile as preflight before startup lock work", %{root_dir: root_dir} do
    result =
      Dev.dev(
        root_dir: root_dir,
        skip_install_check: true,
        skip_bootstrap: true,
        skip_readiness: true
      )

    assert match?({:error, {:runtime_compile_failed, :runtime_root, _status, _output}}, result) or
             match?({:error, {:shortname_host_unavailable, _reason}}, result)

    assert {:error, :not_found} = State.read_runtime(root_dir: root_dir)
    assert :ok = Lock.with_lock([root_dir: root_dir], fn -> :ok end)
  end

  test "dev/1 writes shortname-compatible node names in runtime", %{root_dir: root_dir} do
    task =
      Task.async(fn ->
        Dev.dev(
          root_dir: root_dir,
          skip_install_check: true,
          skip_bootstrap: true,
          skip_readiness: true,
          service_specs_override: service_specs(root_dir)
        )
      end)

    try do
      assert :ok =
               wait_until(fn ->
                 match?(
                   {:ok,
                    %{"node_names" => %{"runner" => _, "orchestrator" => _, "control" => _}}},
                   State.read_runtime(root_dir: root_dir)
                 )
               end)

      assert {:ok, runtime} = State.read_runtime(root_dir: root_dir)

      assert runtime |> get_in(["node_names", "runner"]) |> short_host() |> short_host?()
      assert runtime |> get_in(["node_names", "orchestrator"]) |> short_host() |> short_host?()
      assert runtime |> get_in(["node_names", "control"]) |> short_host() |> short_host?()

      assert runtime
             |> get_in(["services", "runner", "node_name"])
             |> short_host()
             |> short_host?()

      assert runtime
             |> get_in(["services", "orchestrator", "node_name"])
             |> short_host()
             |> short_host?()

      assert :ok = Dev.stop(root_dir: root_dir)
      assert %{stack_status: :stopped} = Dev.status(root_dir: root_dir)
    after
      _ = Dev.stop(root_dir: root_dir)
      _ = Task.await(task, 30_000)
    end
  end

  defp service_specs(root_dir) do
    shell = System.find_executable("bash") || "/bin/bash"

    [
      %{
        name: "runner",
        exec: shell,
        args: ["-lc", "sleep 60"],
        cwd: root_dir,
        log_path: Paths.runner_log_path(root_dir),
        env: %{}
      },
      %{
        name: "orchestrator",
        exec: shell,
        args: ["-lc", "sleep 60"],
        cwd: root_dir,
        log_path: Paths.orchestrator_log_path(root_dir),
        env: %{}
      },
      %{
        name: "web",
        exec: shell,
        args: ["-lc", "sleep 60"],
        cwd: root_dir,
        log_path: Paths.web_log_path(root_dir),
        env: %{}
      }
    ]
  end

  defp short_host(node_name) when is_binary(node_name) do
    case String.split(node_name, "@", parts: 2) do
      [_name, host] -> host
      _other -> ""
    end
  end

  defp short_host?(host) when is_binary(host), do: host != "" and not String.contains?(host, ".")

  defp wait_until(fun, attempts \\ 120)
  defp wait_until(_fun, 0), do: {:error, :timeout}

  defp wait_until(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(250)
      wait_until(fun, attempts - 1)
    end
  end
end
