defmodule Mix.Tasks.Favn.BackfillTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureIO

  alias Mix.Tasks.Favn.Backfill

  setup do
    root_dir =
      Path.join(
        System.tmp_dir!(),
        "favn_backfill_task_test_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(root_dir)
    on_exit(fn -> File.rm_rf(root_dir) end)
    %{root_dir: root_dir}
  end

  test "parses bounded status inspection" do
    assert {:ok, {:status, "bf_1", [limit: 5]}} =
             Backfill.parse_args(["status", "bf_1", "--limit", "5"])

    assert {:error, "missing BACKFILL_ID; usage: mix favn.backfill status BACKFILL_ID"} =
             Backfill.parse_args(["status"])
  end

  test "prints admission failure details when no child run was created" do
    status = %{
      "backfill" => %{
        "backfill_id" => "bf_admission",
        "root_run_id" => "run_backfill_admission",
        "status" => "failed",
        "target_id" => "asset:Elixir.Example.Core.Output:asset",
        "expected_window_count" => 1,
        "progress" => %{"succeeded_count" => 0, "failed_count" => 1}
      },
      "failed_windows" => [
        %{
          "window_key" => "month:Etc/UTC:2026-07-01T00:00:00Z",
          "run_id" => nil,
          "last_error" => %{
            "reason" => "operator_decision_required",
            "details" => %{
              "target_id" => "asset:Elixir.Example.Core.Output:asset",
              "compatibility_status" => "operator_decision",
              "reason_code" => "unmanaged_physical_relation"
            }
          }
        }
      ],
      "failed_windows_pagination" => %{"has_more" => false}
    }

    output = capture_io(fn -> Backfill.print_status(status) end)

    assert output =~ "Backfill: bf_admission"
    assert output =~ "Root run: run_backfill_admission"
    assert output =~ "Windows: 1"
    assert output =~ "Succeeded: 0"
    assert output =~ "Failed: 1"
    assert output =~ "child run: not created"
    assert output =~ "reason: operator_decision_required"
    assert output =~ "compatibility: operator_decision"
    assert output =~ "compatibility reason: unmanaged_physical_relation"
  end

  test "failed submit prints the reason and working inspection command", %{root_dir: root_dir} do
    parent = self()

    backfill = %{
      "backfill_id" => "bf_admission",
      "root_run_id" => "run_backfill_admission",
      "manifest_version_id" => "mv_1",
      "status" => "failed",
      "target_id" => "asset:Elixir.Example.Core.Output:asset",
      "expected_window_count" => 1,
      "progress" => %{"succeeded_count" => 0, "failed_count" => 1}
    }

    failed_window = %{
      "window_key" => "month:Etc/UTC:2026-07-01T00:00:00Z",
      "run_id" => nil,
      "last_error" => %{
        "reason" => "operator_decision_required",
        "details" => %{
          "target_id" => "asset:Elixir.Example.Core.Output:asset",
          "compatibility_status" => "operator_decision",
          "reason_code" => "unmanaged_physical_relation"
        }
      }
    }

    {:ok, base_url, _server} =
      start_server(
        [
          {200,
           %{
             data: %{
               manifest: %{manifest_version_id: "mv_1"},
               targets: %{
                 pipelines: [
                   %{
                     target_id: "pipeline:Elixir.MyApp.Pipeline",
                     label: "MyApp.Pipeline"
                   }
                 ]
               }
             }
           }},
          {202, %{data: %{backfill: Map.put(backfill, "status", "ready")}}},
          {200, %{data: %{backfill: backfill}}},
          {200, %{data: %{backfill: backfill}}},
          {200,
           %{
             data: %{
               items: [failed_window],
               pagination: %{limit: 20, has_more: false, next_cursor: nil}
             }
           }}
        ],
        parent
      )

    write_running_state(root_dir, base_url)

    output =
      capture_io(fn ->
        assert_raise Mix.Error,
                     ~r/Inspect it with mix favn.backfill status bf_admission/,
                     fn ->
                       Backfill.run([
                         "submit",
                         "MyApp.Pipeline",
                         "--from",
                         "2026-07-01",
                         "--to",
                         "2026-07-01",
                         "--kind",
                         "month",
                         "--poll-interval-ms",
                         "1",
                         "--root-dir",
                         root_dir
                       ])
                     end
      end)

    assert output =~ "reason: operator_decision_required"
    assert output =~ "child run: not created"

    assert_receive {:request,
                    "/api/orchestrator/v1/backfills/bf_admission/windows?status=failed&limit=20"}
  end

  defp write_running_state(root_dir, base_url) do
    state_dir = Path.join([root_dir, ".favn", "local"])
    File.mkdir_p!(state_dir)

    File.write!(
      Path.join(state_dir, "state.json"),
      JSON.encode!(%{
        "schema_version" => 1,
        "project_root" => root_dir,
        "operator_node" => "unused@127.0.0.1",
        "orchestrator_url" => base_url,
        "workspace_id" => "local-dev",
        "runner_release_id" => "rr_" <> String.duplicate("a", 64)
      })
    )

    File.write!(
      Path.join(state_dir, "credentials.json"),
      JSON.encode!(%{"cookie" => "unused", "service_token" => "test-token"})
    )
  end

  defp start_server(responses, parent) do
    {:ok, listen_socket} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, {_addr, port}} = :inet.sockname(listen_socket)

    server =
      spawn_link(fn ->
        Enum.each(responses, fn {status, payload} ->
          {:ok, socket} = :gen_tcp.accept(listen_socket)
          request = receive_request(socket, "")
          body = JSON.encode!(payload)
          :ok = :gen_tcp.send(socket, response(status, body))
          :ok = :gen_tcp.close(socket)
          send(parent, {:request, request_path(request)})
        end)

        :ok = :gen_tcp.close(listen_socket)
      end)

    {:ok, "http://127.0.0.1:#{port}", server}
  end

  defp receive_request(socket, acc) do
    {:ok, chunk} = :gen_tcp.recv(socket, 0, 2_000)
    request = acc <> chunk

    if request_complete?(request), do: request, else: receive_request(socket, request)
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
