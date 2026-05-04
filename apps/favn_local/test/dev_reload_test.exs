defmodule Favn.Dev.ReloadTest do
  use ExUnit.Case, async: true

  alias Favn.Dev
  alias Favn.Dev.Reload
  alias Favn.Dev.Stack
  alias Favn.Dev.State

  setup do
    root_dir =
      Path.join(System.tmp_dir!(), "favn_dev_reload_test_#{System.unique_integer([:positive])}")

    File.mkdir_p!(root_dir)

    on_exit(fn ->
      File.rm_rf(root_dir)
    end)

    %{root_dir: root_dir}
  end

  test "reload/1 fails when stack is not running", %{root_dir: root_dir} do
    assert {:error, :stack_not_running} = Dev.reload(root_dir: root_dir)
  end

  test "reload/1 fails when stack is only partially healthy", %{root_dir: root_dir} do
    pid = :os.getpid() |> List.to_string() |> String.to_integer()

    runtime = %{
      "services" => %{
        "web" => %{"pid" => pid},
        "orchestrator" => %{"pid" => 999_998},
        "runner" => %{"pid" => 999_997}
      },
      "orchestrator_base_url" => "http://127.0.0.1:4101",
      "web_base_url" => "http://127.0.0.1:4173"
    }

    assert :ok = State.write_runtime(runtime, root_dir: root_dir)
    assert {:error, :stack_not_healthy} = Dev.reload(root_dir: root_dir)
  end

  test "runner_sname/1 returns short node name" do
    assert "favn_runner_123" == Reload.runner_sname("favn_runner_123@localhost")
    assert "favn_runner_123" == Reload.runner_sname("favn_runner_123")
  end

  test "runner reachability starts local control node before waiting" do
    caller = self()

    assert :ok =
             Reload.wait_runner_node_reachable(
               "favn_runner_123@localhost",
               %{"rpc_cookie" => "reload-cookie"},
               node_control_fun: fn cookie ->
                 send(caller, {:control_node_started, cookie})
                 :ok
               end,
               runner_node_wait_fun: fn runner_node, timeout_ms ->
                 assert_received {:control_node_started, "reload-cookie"}
                 send(caller, {:runner_wait, runner_node, timeout_ms})
                 :ok
               end,
               runner_wait_timeout_ms: 250
             )

    assert_received {:runner_wait, "favn_runner_123@localhost", 250}
  end

  test "runner reachability rejects missing RPC cookie" do
    assert {:error, :missing_rpc_cookie} =
             Reload.wait_runner_node_reachable("favn_runner_123@localhost", %{})
  end

  test "runner_replacement_marker/3 captures old runner identity and next generation" do
    runtime = %{
      "services" => %{
        "runner" => %{
          "pid" => 12_345,
          "node_name" => "favn_runner_123@localhost",
          "generation" => 3
        }
      }
    }

    marker = Reload.runner_replacement_marker(runtime, "stopping_old", 4)

    assert %{
             "status" => "stopping_old",
             "generation" => 4,
             "old_generation" => 3,
             "old_pid" => 12_345,
             "old_node" => "favn_runner_123@localhost",
             "started_at" => started_at,
             "updated_at" => updated_at
           } = marker

    assert is_binary(started_at)
    assert is_binary(updated_at)
  end

  test "runner_replacement_exit?/3 accepts only clean old-runner exits for active markers" do
    startup_runner = %{pid: 12_345, generation: 3}

    marker = %{
      "status" => "completed",
      "old_pid" => 12_345,
      "old_generation" => 3
    }

    assert Stack.runner_replacement_exit?(startup_runner, marker, 0)
    refute Stack.runner_replacement_exit?(startup_runner, %{marker | "status" => "failed"}, 0)
    refute Stack.runner_replacement_exit?(startup_runner, marker, 1)
    refute Stack.runner_replacement_exit?(%{startup_runner | pid: 54_321}, marker, 0)
    refute Stack.runner_replacement_exit?(%{startup_runner | generation: 4}, marker, 0)
  end

  test "foreground monitor deterministically validates completed runner replacement" do
    runtime = %{
      "services" => %{"runner" => %{"pid" => 54_321, "generation" => 4}},
      "reload" => %{
        "runner_replacement" => %{
          "status" => "completed",
          "old_pid" => 12_345,
          "old_generation" => 3,
          "new_pid" => 54_321,
          "generation" => 4
        }
      }
    }

    assert :ok = Stack.runner_replacement_monitor_status(runtime, &(&1 == 54_321))

    assert {:error, {:service_exit, "runner", :replacement_not_running}} =
             Stack.runner_replacement_monitor_status(runtime, fn _pid -> false end)

    failed = put_in(runtime, ["reload", "runner_replacement", "status"], "failed")
    failed = put_in(failed, ["reload", "runner_replacement", "error"], ":boom")

    assert {:error, {:runner_replacement_failed, ":boom"}} =
             Stack.runner_replacement_monitor_status(failed, &(&1 == 54_321))
  end
end
