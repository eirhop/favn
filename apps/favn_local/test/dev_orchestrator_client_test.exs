defmodule Favn.Dev.OrchestratorClientTest do
  use ExUnit.Case, async: false

  alias Favn.Dev.OrchestratorClient
  alias Favn.Manifest.Version

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

  test "publish_manifest/3 serializes manifest structs before JSON encoding", %{bin_dir: bin_dir} do
    args_path = Path.join(bin_dir, "curl_args.txt")
    write_fake_curl(bin_dir, ~s({"data":{"ok":true}}), 200, args_path)

    manifest = %{
      schema_version: 1,
      runner_contract_version: 1,
      assets: [],
      pipelines: [],
      schedules: [],
      graph: %{},
      metadata: %{}
    }

    {:ok, version} = Version.new(manifest, manifest_version_id: "mv_orchestrator_client_test")

    assert {:ok, %{"data" => %{"ok" => true}}} =
             OrchestratorClient.publish_manifest("http://127.0.0.1:4101", "token", %{
               manifest_version_id: version.manifest_version_id,
               manifest: version.manifest
             })

    assert {:ok, args} = File.read(args_path)
    assert args =~ ~s("manifest_version_id":"mv_orchestrator_client_test")
    assert args =~ ~s("manifest":{"assets":[])
    refute args =~ ~s("__struct__")
  end

  defp write_fake_curl(bin_dir, body, status, args_path \\ nil) do
    args_line =
      case args_path do
        path when is_binary(path) -> "printf '%s\n' \"$*\" > \"#{path}\""
        _ -> ""
      end

    script =
      [
        "#!/usr/bin/env bash",
        args_line,
        "printf '%s\\n' '#{body}'",
        "printf '%s\\n' '#{status}'"
      ]
      |> Enum.reject(&(&1 == ""))
      |> Enum.join("\n")

    path = Path.join(bin_dir, "curl")
    File.write!(path, script)
    File.chmod!(path, 0o755)

    System.put_env("PATH", bin_dir <> ":" <> (System.get_env("PATH") || ""))
  end
end
