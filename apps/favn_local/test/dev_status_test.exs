defmodule Favn.Dev.StatusTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureIO

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

  test "mix favn.status prints local stack shape", %{root_dir: root_dir} do
    pid = :os.getpid() |> List.to_string() |> String.to_integer()

    runtime = %{
      "storage" => "memory",
      "active_manifest_version_id" => "mv_test",
      "services" => %{
        "web" => %{"pid" => pid},
        "orchestrator" => %{"pid" => pid},
        "runner" => %{"pid" => pid}
      }
    }

    assert :ok = State.write_runtime(runtime, root_dir: root_dir)

    output =
      capture_io(fn ->
        Mix.Tasks.Favn.Status.run(["--root-dir", root_dir])
      end)

    assert output =~ "Favn local dev stack"
    assert output =~ "manifest: mv_test"
    assert output =~ "web:"
    assert output =~ "orchestrator:"
    assert output =~ "runner:"
  end
end
