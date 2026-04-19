defmodule Favn.Dev.ReloadTest do
  use ExUnit.Case, async: true

  alias Favn.Dev
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
end
