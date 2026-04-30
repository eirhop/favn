defmodule Favn.Dev.BackfillTest do
  use ExUnit.Case, async: true

  alias Favn.Dev.Backfill
  alias Favn.Dev.State

  setup do
    root_dir =
      Path.join(System.tmp_dir!(), "favn_dev_backfill_test_#{System.unique_integer([:positive])}")

    File.mkdir_p!(root_dir)

    on_exit(fn -> File.rm_rf(root_dir) end)

    %{root_dir: root_dir}
  end

  test "build_submit_payload/3 builds active-manifest pipeline backfill payload" do
    assert {:ok, payload} =
             Backfill.build_submit_payload(
               %{"target_id" => "pipeline:Elixir.MyApp.Pipeline"},
               %{from: "2026-01-01", to: "2026-01-03", kind: "day", timezone: "Etc/UTC"},
               coverage_baseline_id: "baseline_1",
               max_attempts: 2
             )

    assert payload == %{
             target: %{type: "pipeline", id: "pipeline:Elixir.MyApp.Pipeline"},
             manifest_selection: %{mode: "active"},
             range: %{from: "2026-01-01", to: "2026-01-03", kind: "day", timezone: "Etc/UTC"},
             coverage_baseline_id: "baseline_1",
             max_attempts: 2
           }
  end

  test "build_submit_payload/3 sends run timeout separately from wait timeout" do
    assert {:ok, payload} =
             Backfill.build_submit_payload(
               %{"target_id" => "pipeline:Elixir.MyApp.Pipeline"},
               %{from: "2026-01-01", to: "2026-01-03", kind: "day", timezone: "Etc/UTC"},
               wait_timeout_ms: 120_000,
               run_timeout_ms: 5_000
             )

    assert payload.timeout_ms == 5_000
  end

  test "submit_pipeline/2 treats waited partial parent as terminal failure", %{root_dir: root_dir} do
    parent = self()

    {:ok, base_url, _server} =
      start_server(
        [
          {201, ~s({"data":{"session":{"id":"sess_1"},"actor":{"id":"act_1"}}})},
          {200,
           ~s({"data":{"manifest":{"manifest_version_id":"mv_1"},"targets":{"pipelines":[{"target_id":"pipeline:Elixir.MyApp.Pipeline","label":"MyApp.Pipeline"}]}}})},
          {201,
           ~s({"data":{"run":{"id":"backfill_1","status":"partial","manifest_version_id":"mv_1"}}})}
        ],
        parent: parent
      )

    write_running_state(root_dir, base_url)

    assert {:error, {:run_failed, "backfill parent run finished with status partial", %{"id" => "backfill_1"}}} =
             Backfill.submit_pipeline(MyApp.Pipeline,
               root_dir: root_dir,
               from: "2026-01-01",
               to: "2026-01-02",
               kind: "day"
             )
  end

  test "build_range/1 validates explicit range options" do
    assert {:ok, %{from: "2026-01-01", to: "2026-01-02", kind: "day", timezone: "Etc/UTC"}} =
             Backfill.build_range(from: "2026-01-01", to: "2026-01-02", kind: :day)

    assert {:error, {:missing_option, :from}} = Backfill.build_range(to: "2026-01-02", kind: :day)
  end

  test "submit_pipeline/2 resolves active manifest target and posts backfill", %{
    root_dir: root_dir
  } do
    parent = self()

    {:ok, base_url, _server} =
      start_server(
        [
          {201, ~s({"data":{"session":{"id":"sess_1"},"actor":{"id":"act_1"}}})},
          {200,
           ~s({"data":{"manifest":{"manifest_version_id":"mv_1"},"targets":{"pipelines":[{"target_id":"pipeline:Elixir.MyApp.Pipeline","label":"MyApp.Pipeline"}]}}})},
          {201,
           ~s({"data":{"run":{"id":"backfill_1","status":"running","manifest_version_id":"mv_1"}}})}
        ],
        parent: parent
      )

    write_running_state(root_dir, base_url)

    assert {:ok, %{"id" => "backfill_1", "status" => "running"}} =
             Backfill.submit_pipeline(MyApp.Pipeline,
               root_dir: root_dir,
               from: "2026-01-01",
               to: "2026-01-02",
               kind: "day",
               timezone: "Europe/Oslo",
               wait: false
             )

    assert_receive {:request, %{path: "/api/orchestrator/v1/backfills", body: body}}
    decoded = JSON.decode!(body)

    assert decoded["target"] == %{"type" => "pipeline", "id" => "pipeline:Elixir.MyApp.Pipeline"}
    assert decoded["manifest_selection"] == %{"mode" => "active"}

    assert decoded["range"] == %{
             "from" => "2026-01-01",
             "to" => "2026-01-02",
             "kind" => "day",
             "timezone" => "Europe/Oslo"
           }
  end

  test "list and rerun workflows parse orchestrator responses", %{root_dir: root_dir} do
    parent = self()

    {:ok, base_url, _server} =
      start_server(
        [
          {201, ~s({"data":{"session":{"id":"sess_1"},"actor":{"id":"act_1"}}})},
          {200, ~s({"data":{"items":[{"window_key":"day:2026-01-01:Etc/UTC"}]}})},
          {201, ~s({"data":{"session":{"id":"sess_2"},"actor":{"id":"act_1"}}})},
          {201, ~s({"data":{"run":{"id":"rerun_1","status":"running"}}})}
        ],
        parent: parent
      )

    write_running_state(root_dir, base_url)

    assert {:ok, [%{"window_key" => "day:2026-01-01:Etc/UTC"}]} =
             Backfill.list_windows("backfill_1", root_dir: root_dir, status: "error")

    assert {:ok, %{"id" => "rerun_1"}} =
             Backfill.rerun_window("backfill_1", "day:2026-01-01:Etc/UTC", root_dir: root_dir)

    assert_receive {:request,
                    %{path: "/api/orchestrator/v1/backfills/backfill_1/windows?status=error"}}

    assert_receive {:request,
                    %{path: "/api/orchestrator/v1/backfills/backfill_1/windows/rerun", body: body}}

    assert JSON.decode!(body) == %{"window_key" => "day:2026-01-01:Etc/UTC"}
  end

  defp write_running_state(root_dir, base_url) do
    pid = :os.getpid() |> List.to_string() |> String.to_integer()

    assert :ok =
             State.write_runtime(
               %{
                 "orchestrator_base_url" => base_url,
                 "services" => %{
                   "web" => %{"pid" => pid},
                   "orchestrator" => %{"pid" => pid},
                   "runner" => %{"pid" => pid}
                 }
               },
               root_dir: root_dir
             )

    assert :ok =
             State.write_secrets(
               %{
                 "service_token" => "token",
                 "local_operator_username" => "operator",
                 "local_operator_password" => "operator-password"
               },
               root_dir: root_dir
             )
  end

  defp start_server(responses, opts) when is_list(responses) do
    parent = Keyword.fetch!(opts, :parent)
    {:ok, listen_socket} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, {_addr, port}} = :inet.sockname(listen_socket)

    server =
      spawn_link(fn ->
        serve_responses(listen_socket, responses, parent)
        :ok = :gen_tcp.close(listen_socket)
      end)

    {:ok, "http://127.0.0.1:#{port}", server}
  end

  defp serve_responses(_listen_socket, [], _parent), do: :ok

  defp serve_responses(listen_socket, [{status, body} | rest], parent) do
    {:ok, socket} = :gen_tcp.accept(listen_socket)
    request = receive_request(socket, "")
    :ok = :gen_tcp.send(socket, response(status, body))
    :ok = :gen_tcp.close(socket)
    send(parent, {:request, %{path: request_path(request), body: request_body(request)}})
    serve_responses(listen_socket, rest, parent)
  end

  defp receive_request(socket, acc) do
    {:ok, chunk} = :gen_tcp.recv(socket, 0, 2_000)
    acc = acc <> chunk

    if request_complete?(acc), do: acc, else: receive_request(socket, acc)
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
          if String.downcase(key) == "content-length", do: String.to_integer(String.trim(value))

        _other ->
          nil
      end
    end)
  end

  defp request_path(request) do
    request |> String.split("\r\n", parts: 2) |> hd() |> String.split(" ") |> Enum.at(1)
  end

  defp request_body(request) do
    case String.split(request, "\r\n\r\n", parts: 2) do
      [_headers, body] -> body
      _other -> ""
    end
  end

  defp response(status, body) do
    [
      "HTTP/1.1 #{status} OK\r\n",
      "content-type: application/json\r\n",
      "content-length: #{byte_size(body)}\r\n",
      "connection: close\r\n",
      "\r\n",
      body
    ]
  end
end
