defmodule Favn.Dev.StatusTest do
  use ExUnit.Case, async: true

  alias Favn.Dev.State
  alias Favn.Dev.Status

  setup do
    root_dir =
      Path.join(System.tmp_dir!(), "favn_dev_status_test_#{System.unique_integer([:positive])}")

    File.mkdir_p!(root_dir)

    on_exit(fn ->
      File.rm_rf(root_dir)
    end)

    %{root_dir: root_dir}
  end

  test "inspect_stack/1 reports stopped state when runtime file is missing", %{root_dir: root_dir} do
    status = Status.inspect_stack(root_dir: root_dir)

    assert status.stack_status == :stopped
    assert status.active_manifest_version_id == nil
  end

  test "inspect_stack/1 returns local stack shape fields", %{root_dir: root_dir} do
    pid = :os.getpid() |> List.to_string() |> String.to_integer()

    runtime = %{
      "storage" => "memory",
      "active_manifest_version_id" => "mv_test",
      "orchestrator_base_url" => "http://127.0.0.1:4101",
      "web_base_url" => "http://127.0.0.1:4173",
      "node_names" => %{
        "runner" => "favn_runner_test@localhost",
        "orchestrator" => "favn_orchestrator_test@localhost",
        "control" => "favn_local_ctl_test@localhost"
      },
      "distribution_ports" => %{
        "runner" => 45_001,
        "orchestrator" => 45_002,
        "control" => 45_003
      },
      "services" => %{
        "web" => %{"pid" => pid},
        "orchestrator" => %{
          "pid" => pid,
          "node_name" => "favn_orchestrator_test@localhost",
          "distribution_port" => 45_002
        },
        "runner" => %{
          "pid" => pid,
          "node_name" => "favn_runner_test@localhost",
          "distribution_port" => 45_001
        }
      }
    }

    assert :ok = State.write_runtime(runtime, root_dir: root_dir)

    status = Status.inspect_stack(root_dir: root_dir)

    assert status.stack_status == :running
    assert status.active_manifest_version_id == "mv_test"
    assert status.services.web.status == :running
    assert status.services.orchestrator.status == :running
    assert status.services.runner.status == :running
    assert status.user_urls.web == "http://127.0.0.1:4173"
    assert status.user_urls.orchestrator_api == "http://127.0.0.1:4101"
    assert status.internal_control.runner_node.node_name == "favn_runner_test@localhost"
    assert status.internal_control.runner_node.distribution_port == 45_001
    assert status.internal_control.orchestrator_node.node_name == "favn_orchestrator_test@localhost"
    assert status.internal_control.orchestrator_node.distribution_port == 45_002
    assert status.internal_control.control_node.node_name == "favn_local_ctl_test@localhost"
    assert status.internal_control.control_node.distribution_port == 45_003
  end

  test "inspect_stack/1 reports stale when runtime exists but services are dead", %{
    root_dir: root_dir
  } do
    runtime = %{
      "storage" => "memory",
      "services" => %{
        "web" => %{"pid" => 999_999},
        "orchestrator" => %{"pid" => 999_998},
        "runner" => %{"pid" => 999_997}
      }
    }

    assert :ok = State.write_runtime(runtime, root_dir: root_dir)

    status = Status.inspect_stack(root_dir: root_dir)

    assert status.stack_status == :stale
    assert status.services.web.status == :dead
    assert status.services.orchestrator.status == :dead
    assert status.services.runner.status == :dead
  end
end
