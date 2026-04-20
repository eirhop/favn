defmodule Favn.Dev.LifecycleTest do
  use ExUnit.Case, async: false

  alias Favn.Dev
  alias Favn.Dev.Lock
  alias Favn.Dev.Paths
  alias Favn.Dev.State

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

  test "foreground lifecycle leaves lock free and supports second-terminal control", %{
    root_dir: root_dir
  } do
    root_dir = File.cwd!()

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
               skip_bootstrap: true,
               skip_readiness: true
             )

    assert {:error, :not_found} = State.read_runtime(root_dir: root_dir)
    assert {:ok, %{"error" => _}} = State.read_last_failure(root_dir: root_dir)
  end

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
