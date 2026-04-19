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

  test "foreground start allows status/reload/stop from another process", %{root_dir: root_dir} do
    :ok = State.ensure_layout(root_dir: root_dir)
    write_fake_curl(root_dir)

    specs = service_specs(root_dir)

    task =
      Task.async(fn ->
        Dev.dev(
          root_dir: root_dir,
          service_specs_override: specs,
          skip_bootstrap: true,
          skip_readiness: true
        )
      end)

    assert :ok = wait_until(fn -> match?({:ok, _}, State.read_runtime(root_dir: root_dir)) end)
    assert :ok = Lock.with_lock([root_dir: root_dir], fn -> :ok end)

    assert %{stack_status: :running} = Dev.status(root_dir: root_dir)
    assert :ok = Dev.reload(root_dir: root_dir)
    assert :ok = Dev.stop(root_dir: root_dir)
    assert %{stack_status: :stopped} = Dev.status(root_dir: root_dir)

    _ = Task.await(task, 15_000)
  end

  defp service_specs(root_dir) do
    logs_dir = Paths.logs_dir(root_dir)

    [
      %{
        name: "runner",
        exec: System.find_executable("bash") || "/bin/bash",
        args: ["-lc", "sleep 60"],
        cwd: root_dir,
        log_path: Path.join(logs_dir, "runner.log"),
        env: %{}
      },
      %{
        name: "orchestrator",
        exec: System.find_executable("bash") || "/bin/bash",
        args: ["-lc", "sleep 60"],
        cwd: root_dir,
        log_path: Path.join(logs_dir, "orchestrator.log"),
        env: %{}
      },
      %{
        name: "web",
        exec: System.find_executable("bash") || "/bin/bash",
        args: ["-lc", "sleep 60"],
        cwd: root_dir,
        log_path: Path.join(logs_dir, "web.log"),
        env: %{}
      }
    ]
  end

  defp write_fake_curl(root_dir) do
    bin_dir = Path.join(root_dir, "bin")
    File.mkdir_p!(bin_dir)

    script =
      [
        "#!/usr/bin/env bash",
        "url=\"${@: -1}\"",
        "if [[ \"$url\" == *\"/api/orchestrator/v1/runs/in-flight\"* ]]; then",
        "  printf '%s\\n' '{\"data\":{\"run_ids\":[]}}'",
        "  printf '%s\\n' '200'",
        "elif [[ \"$url\" == *\"/api/orchestrator/v1/manifests/\"*\"/activate\"* ]]; then",
        "  printf '%s\\n' '{\"data\":{\"activated\":true}}'",
        "  printf '%s\\n' '200'",
        "elif [[ \"$url\" == *\"/api/orchestrator/v1/manifests\"* ]]; then",
        "  printf '%s\\n' '{\"data\":{\"manifest\":{\"manifest_version_id\":\"mv_test\"}}}'",
        "  printf '%s\\n' '201'",
        "else",
        "  printf '%s\\n' '{\"error\":{\"code\":\"not_found\"}}'",
        "  printf '%s\\n' '404'",
        "fi"
      ]
      |> Enum.join("\n")

    curl_path = Path.join(bin_dir, "curl")
    File.write!(curl_path, script)
    File.chmod!(curl_path, 0o755)
    System.put_env("PATH", bin_dir <> ":" <> (System.get_env("PATH") || ""))
  end

  defp wait_until(fun, attempts \\ 60)
  defp wait_until(_fun, 0), do: {:error, :timeout}

  defp wait_until(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(100)
      wait_until(fun, attempts - 1)
    end
  end
end
