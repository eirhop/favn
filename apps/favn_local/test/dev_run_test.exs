defmodule Favn.Dev.RunTest do
  use ExUnit.Case, async: true

  alias Favn.Dev
  alias Favn.Dev.Run
  alias Favn.Dev.State

  setup do
    root_dir =
      Path.join(System.tmp_dir!(), "favn_dev_run_test_#{System.unique_integer([:positive])}")

    File.mkdir_p!(root_dir)

    on_exit(fn -> File.rm_rf(root_dir) end)

    %{root_dir: root_dir}
  end

  test "run_pipeline/2 fails when stack is not running", %{root_dir: root_dir} do
    assert {:error, :stack_not_running} = Dev.run_pipeline(MyApp.Pipeline, root_dir: root_dir)
  end

  test "resolve_pipeline_target/2 finds active manifest pipeline by module label" do
    active_manifest = %{
      "targets" => %{
        "pipelines" => [
          %{
            "target_id" => "pipeline:Elixir.MyApp.Pipeline",
            "label" => "MyApp.Pipeline"
          }
        ]
      }
    }

    assert {:ok, %{"target_id" => "pipeline:Elixir.MyApp.Pipeline"}} =
             Run.resolve_pipeline_target(active_manifest, MyApp.Pipeline)
  end

  test "resolve_pipeline_target/2 reports available pipelines on miss" do
    active_manifest = %{
      "targets" => %{
        "pipelines" => [%{"target_id" => "pipeline:Elixir.Other", "label" => "Other"}]
      }
    }

    assert {:error, {:pipeline_not_found, "Missing.Pipeline", ["Other"]}} =
             Run.resolve_pipeline_target(active_manifest, "Missing.Pipeline")
  end

  test "run_pipeline/2 submits with local-dev context and no password login", %{
    root_dir: root_dir
  } do
    {:ok, base_url, _server} =
      start_server([
        {200,
         ~s({"data":{"manifest":{"manifest_version_id":"mv_1"},"targets":{"pipelines":[{"target_id":"pipeline:Elixir.MyApp.Pipeline","label":"MyApp.Pipeline"}]}}})},
        {201,
         ~s({"data":{"run":{"id":"run_1","status":"running","manifest_version_id":"mv_1"}}})},
        {200, ~s({"data":{"run":{"id":"run_1","status":"ok","manifest_version_id":"mv_1"}}})}
      ])

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

    assert {:ok, %{"id" => "run_1", "status" => "ok"}} =
             Dev.run_pipeline(MyApp.Pipeline,
               root_dir: root_dir,
               timeout_ms: 1_000,
               poll_interval_ms: 1
             )
  end

  test "run_pipeline/2 validates polling options", %{root_dir: root_dir} do
    assert {:error, {:invalid_option, :timeout_ms}} =
             Dev.run_pipeline(MyApp.Pipeline, root_dir: root_dir, timeout_ms: 0)

    assert {:error, {:invalid_option, :poll_interval_ms}} =
             Dev.run_pipeline(MyApp.Pipeline, root_dir: root_dir, poll_interval_ms: -1)
  end

  test "run_pipeline/2 validates local window options before submission", %{root_dir: root_dir} do
    assert {:error, {:invalid_option, :timezone_without_window}} =
             Dev.run_pipeline(MyApp.Pipeline, root_dir: root_dir, timezone: "Europe/Oslo")

    assert {:error, {:invalid_window_request, {:invalid_window_value, :month, "2026-99"}}} =
             Dev.run_pipeline(MyApp.Pipeline, root_dir: root_dir, window: "month:2026-99")
  end

  test "run_pipeline/2 surfaces orchestrator validation messages", %{root_dir: root_dir} do
    {:ok, base_url, _server} =
      start_server([
        {200,
         ~s({"data":{"manifest":{"manifest_version_id":"mv_1"},"targets":{"pipelines":[{"target_id":"pipeline:Elixir.MyApp.Pipeline","label":"MyApp.Pipeline"}]}}})},
        {422,
         ~s({"error":{"code":"validation_failed","message":"Pipeline requires an explicit month window"}})}
      ])

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

    assert {:error,
            {:orchestrator_validation_failed, "Pipeline requires an explicit month window"}} =
             Dev.run_pipeline(MyApp.Pipeline, root_dir: root_dir)
  end

  defp start_server(responses) when is_list(responses) do
    {:ok, listen_socket} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, {_addr, port}} = :inet.sockname(listen_socket)

    server =
      spawn_link(fn ->
        serve_responses(listen_socket, responses)
        :ok = :gen_tcp.close(listen_socket)
      end)

    {:ok, "http://127.0.0.1:#{port}", server}
  end

  defp serve_responses(_listen_socket, []), do: :ok

  defp serve_responses(listen_socket, [{status, body} | rest]) do
    {:ok, socket} = :gen_tcp.accept(listen_socket)
    _request = receive_request(socket, "")
    :ok = :gen_tcp.send(socket, response(status, body))
    :ok = :gen_tcp.close(socket)
    serve_responses(listen_socket, rest)
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
