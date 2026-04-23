defmodule Favn.Dev.StorageVerificationTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias Favn.Dev
  alias Favn.Dev.Paths
  alias Favn.Dev.State

  setup do
    root_dir =
      Path.join(
        System.tmp_dir!(),
        "favn_dev_storage_verification_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(Path.join(root_dir, "web/favn_web"))
    File.mkdir_p!(Path.join(root_dir, "apps/favn_runner"))
    File.mkdir_p!(Path.join(root_dir, "apps/favn_orchestrator"))
    File.mkdir_p!(Path.join(root_dir, "apps/favn_duckdb"))

    File.write!(Path.join(root_dir, "mix.lock"), "lock")
    File.write!(Path.join(root_dir, "web/favn_web/package.json"), "{}")
    File.write!(Path.join(root_dir, "web/favn_web/package-lock.json"), "{}")

    File.write!(
      Path.join(root_dir, "apps/favn_runner/mix.exs"),
      "defmodule Runner.MixProject do end"
    )

    File.write!(
      Path.join(root_dir, "apps/favn_orchestrator/mix.exs"),
      "defmodule Orchestrator.MixProject do end"
    )

    on_exit(fn ->
      File.rm_rf(root_dir)
    end)

    %{root_dir: root_dir}
  end

  test "sqlite verification across install/dev/status/reload/stop/logs/reset/build.single", %{
    root_dir: root_dir
  } do
    web_port = free_port()

    assert {:ok, :installed} =
             Dev.install(
               root_dir: root_dir,
               skip_web_install: true,
               skip_tool_checks: true,
               skip_runtime_deps_install: true
             )

    assert :ok = Dev.ensure_install_ready(root_dir: root_dir, skip_tool_checks: true)

    assert {:ok, %{dist_dir: dist_dir}} =
             Dev.build_single(
               root_dir: root_dir,
               storage: :sqlite,
               skip_compile: true,
               skip_project_root_check: true,
               skip_tool_checks: true
             )

    assert File.exists?(Path.join(dist_dir, "metadata.json"))

    task =
      Task.async(fn ->
        Dev.dev(
          root_dir: root_dir,
          web_port: web_port,
          storage: :sqlite,
          sqlite_path: ".favn/data/storage_verification.sqlite3",
          skip_tool_checks: true,
          skip_bootstrap: true,
          skip_readiness: true,
          service_specs_override: service_specs(root_dir)
        )
      end)

    assert :ok =
             wait_until(fn ->
               match?(
                 {:ok, %{"services" => %{"web" => _, "orchestrator" => _, "runner" => _}}},
                 State.read_runtime(root_dir: root_dir)
               )
             end)

    status = Dev.status(root_dir: root_dir)
    assert status.stack_status == :running
    assert status.storage == "sqlite"

    assert :ok = Dev.logs(root_dir: root_dir, service: :runner, tail: 10, writer: fn _ -> :ok end)

    assert {:error, _reason} = Dev.reload(root_dir: root_dir)

    assert :ok = Dev.stop(root_dir: root_dir)
    _ = Task.await(task, 30_000)
    assert %{stack_status: :stopped} = Dev.status(root_dir: root_dir)

    assert :ok = Dev.reset(root_dir: root_dir)
    refute File.exists?(Path.join(root_dir, ".favn"))
  end

  test "opt-in postgres verification for local dev path", %{root_dir: root_dir} do
    if System.get_env("FAVN_RUN_DEV_POSTGRES_VERIFICATION") != "1" do
      :ok
    else
      assert {:ok, :installed} =
               Dev.install(
                 root_dir: root_dir,
                 skip_web_install: true,
                 skip_tool_checks: true,
                 skip_runtime_deps_install: true
               )

      postgres = [
        hostname: System.get_env("FAVN_DEV_POSTGRES_HOST", "127.0.0.1"),
        port: String.to_integer(System.get_env("FAVN_DEV_POSTGRES_PORT", "5432")),
        username: System.get_env("FAVN_DEV_POSTGRES_USERNAME", "postgres"),
        password: System.get_env("FAVN_DEV_POSTGRES_PASSWORD", "postgres"),
        database: System.get_env("FAVN_DEV_POSTGRES_DATABASE", "favn"),
        ssl: System.get_env("FAVN_DEV_POSTGRES_SSL", "false") == "true",
        pool_size: String.to_integer(System.get_env("FAVN_DEV_POSTGRES_POOL_SIZE", "10"))
      ]

      web_port = free_port()

      task =
        Task.async(fn ->
          Dev.dev(
            root_dir: root_dir,
            web_port: web_port,
            storage: :postgres,
            postgres: postgres,
            skip_tool_checks: true,
            skip_bootstrap: true,
            skip_readiness: true,
            service_specs_override: service_specs(root_dir)
          )
        end)

      assert :ok =
               wait_until(fn ->
                 match?(
                   {:ok, %{"services" => %{"web" => _, "orchestrator" => _, "runner" => _}}},
                   State.read_runtime(root_dir: root_dir)
                 )
               end)

      status = Dev.status(root_dir: root_dir)
      assert status.stack_status == :running
      assert status.storage == "postgres"

      assert :ok = Dev.stop(root_dir: root_dir)
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

  defp wait_until(fun, attempts \\ 80)
  defp wait_until(_fun, 0), do: {:error, :timeout}

  defp wait_until(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(100)
      wait_until(fun, attempts - 1)
    end
  end

  defp free_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, {:active, false}, {:reuseaddr, false}])
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    port
  end
end
