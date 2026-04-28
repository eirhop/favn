defmodule Favn.Dev.OrchestratorClientTest do
  use ExUnit.Case, async: false

  alias Favn.Dev.OrchestratorClient
  alias Favn.Manifest.Version

  test "in_flight_runs/2 parses run ids" do
    {:ok, base_url, _server} = start_server(~s({"data":{"run_ids":["run_a","run_b"]}}), 200)

    assert {:ok, ["run_a", "run_b"]} = OrchestratorClient.in_flight_runs(base_url, "token")
  end

  test "in_flight_runs/2 returns operation context on non-2xx response" do
    {:ok, base_url, _server} = start_server(~s({"error":{"code":"bad_request"}}), 400)

    assert {:error,
            %{
              operation: :list_in_flight_runs,
              method: :get,
              url: url,
              reason: {:http_error, 400, _decoded}
            }} = OrchestratorClient.in_flight_runs(base_url, "token")

    assert url == base_url <> "/api/orchestrator/v1/runs/in-flight"
  end

  test "connect failures are structured and include operation context" do
    base_url = "http://127.0.0.1:#{unused_port()}"

    assert {:error,
            %{
              operation: :list_in_flight_runs,
              method: :get,
              url: url,
              reason: {:connect_failed, _reason}
            }} = OrchestratorClient.in_flight_runs(base_url, "token")

    assert url == base_url <> "/api/orchestrator/v1/runs/in-flight"
  end

  test "publish_manifest/3 serializes manifest structs before JSON encoding" do
    parent = self()
    {:ok, base_url, _server} = start_server(~s({"data":{"ok":true}}), 200, parent: parent)

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
             OrchestratorClient.publish_manifest(base_url, "token", %{
               manifest_version_id: version.manifest_version_id,
               manifest: version.manifest
             })

    assert_receive {:request_body, body}
    assert body =~ ~s("manifest_version_id":"mv_orchestrator_client_test")
    assert body =~ ~s("manifest":{"assets":[])
    refute body =~ ~s("__struct__")
  end

  test "health/1 checks the orchestrator health endpoint" do
    parent = self()
    {:ok, base_url, _server} = start_server(~s({"data":{"status":"ok"}}), 200, parent: parent)

    assert :ok = OrchestratorClient.health(base_url)
    assert_receive {:request_path, "/api/orchestrator/v1/health"}
  end

  test "password_login/4 returns forwarded session context" do
    body = ~s({"data":{"session":{"id":"sess_1"},"actor":{"id":"act_1"}}})
    {:ok, base_url, _server} = start_server(body, 201)

    assert {:ok, %{"actor_id" => "act_1", "session_id" => "sess_1"}} =
             OrchestratorClient.password_login(base_url, "token", "user", "password-1")
  end

  test "submit_run/4 sends forwarded actor and session headers" do
    parent = self()
    body = ~s({"data":{"run":{"id":"run_1","status":"running"}}})
    {:ok, base_url, _server} = start_server(body, 201, parent: parent)

    assert {:ok, %{"id" => "run_1"}} =
             OrchestratorClient.submit_run(
               base_url,
               "token",
               %{"actor_id" => "act_1", "session_id" => "sess_1"},
               %{target: %{type: "pipeline", id: "pipeline:Elixir.MyApp.Pipeline"}}
             )

    assert_receive {:request_headers, headers}
    assert headers["x-favn-actor-id"] == "act_1"
    assert headers["x-favn-session-id"] == "sess_1"
  end

  test "backfill list helpers parse item responses and encode filters" do
    parent = self()

    {:ok, base_url, _server} =
      start_server(~s({"data":{"items":[{"baseline_id":"base_1"}]}}), 200, parent: parent)

    assert {:ok, [%{"baseline_id" => "base_1"}]} =
             OrchestratorClient.list_coverage_baselines(
               base_url,
               "token",
               %{"actor_id" => "act_1", "session_id" => "sess_1"},
               pipeline_module: "MyApp.Pipeline",
               status: "ok"
             )

    assert_receive {:request_path,
                    "/api/orchestrator/v1/backfills/coverage-baselines?pipeline_module=MyApp.Pipeline&status=ok"}
  end

  test "asset window states helper parses item responses" do
    parent = self()

    {:ok, base_url, _server} =
      start_server(~s({"data":{"items":[{"window_key":"day:2026-01-01:Etc/UTC"}]}}), 200,
        parent: parent
      )

    assert {:ok, [%{"window_key" => "day:2026-01-01:Etc/UTC"}]} =
             OrchestratorClient.list_asset_window_states(
               base_url,
               "token",
               %{"actor_id" => "act_1", "session_id" => "sess_1"},
               asset_ref_module: "MyApp.Asset",
               asset_ref_name: "asset"
             )

    assert_receive {:request_path,
                    "/api/orchestrator/v1/assets/window-states?asset_ref_module=MyApp.Asset&asset_ref_name=asset"}
  end

  defp start_server(body, status, opts \\ []) when is_binary(body) and is_integer(status) do
    parent = Keyword.get(opts, :parent)
    {:ok, listen_socket} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, {_addr, port}} = :inet.sockname(listen_socket)

    server =
      spawn_link(fn ->
        {:ok, socket} = :gen_tcp.accept(listen_socket)
        request = receive_request(socket, "")
        :ok = :gen_tcp.send(socket, response(status, body))
        :ok = :gen_tcp.close(socket)
        :ok = :gen_tcp.close(listen_socket)

        if parent do
          send(parent, {:request_path, request_path(request)})
          send(parent, {:request_headers, request_headers(request)})
          send(parent, {:request_body, request_body(request)})
        end
      end)

    {:ok, "http://127.0.0.1:#{port}", server}
  end

  defp receive_request(socket, acc) do
    {:ok, chunk} = :gen_tcp.recv(socket, 0, 2_000)
    acc = acc <> chunk

    if request_complete?(acc) do
      acc
    else
      receive_request(socket, acc)
    end
  end

  defp request_complete?(request) do
    case String.split(request, "\r\n\r\n", parts: 2) do
      [headers, body] -> byte_size(body) >= content_length(headers)
      _other -> false
    end
  end

  defp content_length(headers) do
    headers
    |> String.split("\r\n")
    |> Enum.find_value(0, fn line ->
      case String.split(line, ":", parts: 2) do
        [key, value] ->
          if String.downcase(key) == "content-length" do
            value |> String.trim() |> String.to_integer()
          end

        _other ->
          nil
      end
    end)
  end

  defp request_path(request) do
    request
    |> String.split("\r\n", parts: 2)
    |> hd()
    |> String.split(" ")
    |> Enum.at(1)
  end

  defp request_body(request) do
    case String.split(request, "\r\n\r\n", parts: 2) do
      [_headers, body] -> body
      _other -> ""
    end
  end

  defp request_headers(request) do
    request
    |> String.split("\r\n\r\n", parts: 2)
    |> hd()
    |> String.split("\r\n")
    |> Enum.drop(1)
    |> Enum.reduce(%{}, fn line, acc ->
      case String.split(line, ":", parts: 2) do
        [key, value] -> Map.put(acc, String.downcase(key), String.trim(value))
        _other -> acc
      end
    end)
  end

  defp response(status, body) do
    [
      "HTTP/1.1 #{status} #{reason(status)}\r\n",
      "content-type: application/json\r\n",
      "content-length: #{byte_size(body)}\r\n",
      "connection: close\r\n",
      "\r\n",
      body
    ]
  end

  defp reason(status) when status in 200..299, do: "OK"
  defp reason(_status), do: "Error"

  defp unused_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, {_addr, port}} = :inet.sockname(socket)
    :ok = :gen_tcp.close(socket)
    port
  end
end
