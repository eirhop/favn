defmodule Favn.Dev.OrchestratorClientTest do
  use ExUnit.Case, async: false

  alias Favn.Dev.OrchestratorClient

  setup do
    root_dir =
      Path.join(
        System.tmp_dir!(),
        "favn_dev_orchestrator_client_test_#{System.unique_integer([:positive])}"
      )

    bin_dir = Path.join(root_dir, "bin")
    File.mkdir_p!(bin_dir)

    old_path = System.get_env("PATH")

    on_exit(fn ->
      System.put_env("PATH", old_path || "")
      File.rm_rf(root_dir)
    end)

    %{root_dir: root_dir, bin_dir: bin_dir}
  end

  test "in_flight_runs/2 parses run ids", %{bin_dir: bin_dir} do
    write_fake_curl(bin_dir, ~s({"data":{"run_ids":["run_a","run_b"]}}), 200)

    assert {:ok, ["run_a", "run_b"]} =
             OrchestratorClient.in_flight_runs("http://127.0.0.1:4101", "token")
  end

  test "in_flight_runs/2 returns error on non-2xx response", %{bin_dir: bin_dir} do
    write_fake_curl(bin_dir, ~s({"error":{"code":"bad_request"}}), 400)

    assert {:error, {:http_error, 400, _decoded}} =
             OrchestratorClient.in_flight_runs("http://127.0.0.1:4101", "token")
  end

  defp write_fake_curl(bin_dir, body, status) do
    script =
      [
        "#!/usr/bin/env bash",
        "printf '%s\\n' '#{body}'",
        "printf '%s\\n' '#{status}'"
      ]
      |> Enum.join("\n")

    path = Path.join(bin_dir, "curl")
    File.write!(path, script)
    File.chmod!(path, 0o755)

    System.put_env("PATH", bin_dir <> ":" <> (System.get_env("PATH") || ""))
  end
end
