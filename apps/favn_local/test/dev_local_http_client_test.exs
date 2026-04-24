defmodule Favn.Dev.LocalHttpClientTest do
  use ExUnit.Case, async: false

  alias Favn.Dev.LocalHttpClient

  test "returns decoded JSON for successful responses" do
    {:ok, base_url, _server} = start_server(~s({"data":{"ok":true}}), 200)

    assert {:ok, %{"data" => %{"ok" => true}}} = LocalHttpClient.request(:get, base_url)
  end

  test "normalizes HTTP errors with decoded bodies" do
    {:ok, base_url, _server} = start_server(~s({"error":{"code":"bad_request"}}), 400)

    assert {:error, {:http_error, 400, %{"error" => %{"code" => "bad_request"}}}} =
             LocalHttpClient.request(:get, base_url)
  end

  test "normalizes invalid JSON responses" do
    {:ok, base_url, _server} = start_server("not json", 200)

    assert {:error, {:invalid_json, "not json"}} = LocalHttpClient.request(:get, base_url)
  end

  test "normalizes connect failures" do
    assert {:error, {:connect_failed, _reason}} =
             LocalHttpClient.request(:get, "http://127.0.0.1:#{unused_port()}", [], nil,
               connect_timeout_ms: 100,
               timeout_ms: 100
             )
  end

  defp start_server(body, status) when is_binary(body) and is_integer(status) do
    {:ok, listen_socket} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, {_addr, port}} = :inet.sockname(listen_socket)

    server =
      spawn_link(fn ->
        {:ok, socket} = :gen_tcp.accept(listen_socket)
        {:ok, _request} = :gen_tcp.recv(socket, 0, 2_000)
        :ok = :gen_tcp.send(socket, response(status, body))
        :ok = :gen_tcp.close(socket)
        :ok = :gen_tcp.close(listen_socket)
      end)

    {:ok, "http://127.0.0.1:#{port}", server}
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
