defmodule Favn.Dev.ProcessTest do
  use ExUnit.Case, async: false

  alias Favn.Dev.Process

  setup do
    root_dir =
      Path.join(System.tmp_dir!(), "favn_dev_process_test_#{System.unique_integer([:positive])}")

    File.mkdir_p!(root_dir)

    on_exit(fn ->
      File.rm_rf(root_dir)
    end)

    %{root_dir: root_dir}
  end

  test "start_service/1 and stop_pid/1 manage service lifecycle", %{root_dir: root_dir} do
    log_path = Path.join(root_dir, "service.log")

    spec = %{
      name: "fixture",
      exec: System.find_executable("bash") || "/bin/bash",
      args: ["-lc", "echo ready; sleep 30"],
      cwd: root_dir,
      log_path: log_path,
      env: %{}
    }

    assert {:ok, info} = Process.start_service(spec)
    assert is_integer(info.pid) and info.pid > 0
    assert Process.alive?(info.pid)

    assert :ok = Process.stop_pid(info.pid)
    refute Process.alive?(info.pid)
  end

  test "nil environment values remove inherited variables", %{root_dir: root_dir} do
    log_path = Path.join(root_dir, "unset-env.log")
    previous = System.get_env("FAVN_DEV_PROCESS_UNSET_TEST")
    System.put_env("FAVN_DEV_PROCESS_UNSET_TEST", "inherited")

    on_exit(fn ->
      if previous,
        do: System.put_env("FAVN_DEV_PROCESS_UNSET_TEST", previous),
        else: System.delete_env("FAVN_DEV_PROCESS_UNSET_TEST")
    end)

    spec = %{
      name: "fixture",
      exec: System.find_executable("bash") || "/bin/bash",
      args: ["-lc", "if [ -z \"${FAVN_DEV_PROCESS_UNSET_TEST+x}\" ]; then echo unset; fi"],
      cwd: root_dir,
      log_path: log_path,
      env: %{"FAVN_DEV_PROCESS_UNSET_TEST" => nil}
    }

    assert {:ok, info} = Process.start_service(spec)
    assert_receive {:service_exit, "fixture", 0}, 2_000
    assert File.read!(log_path) == "unset\n"
    refute Process.alive?(info.pid)
  end

  test "service log write failures are reported without exiting caller", %{root_dir: root_dir} do
    if File.exists?("/dev/full") do
      previous_trap_exit = Elixir.Process.flag(:trap_exit, true)

      try do
        spec = %{
          name: "fixture",
          exec: System.find_executable("bash") || "/bin/bash",
          args: ["-lc", "printf output; sleep 30"],
          cwd: root_dir,
          log_path: "/dev/full",
          env: %{}
        }

        assert {:ok, info} = Process.start_service(spec)
        wrapper_pid = info.wrapper_pid

        assert_receive {:service_exit, "fixture", {:log_write_failed, :enospc}}, 2_000
        refute_receive {:EXIT, ^wrapper_pid, _reason}, 100
      after
        Elixir.Process.flag(:trap_exit, previous_trap_exit)
      end
    end
  end
end
