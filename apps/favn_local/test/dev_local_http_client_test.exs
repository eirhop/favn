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

  test "sends JSON POST requests to loopback services" do
    {:ok, base_url, _server} = start_capturing_server(~s({"data":{"created":true}}), 201, self())

    assert {:ok, %{"data" => %{"created" => true}}} =
             LocalHttpClient.request(:post, base_url <> "/runs?dry_run=false", override_headers(),
               ~s({"ok":true})
             )

    assert_receive {:request, request}
    assert request =~ "POST /runs?dry_run=false HTTP/1.1\r\n"
    assert request =~ "authorization: Bearer token\r\n"
    assert request =~ "connection: close\r\n"
    assert request =~ "content-type: application/json\r\n"
    assert request =~ "content-length: 11\r\n"
    refute request =~ "connection: keep-alive\r\n"
    refute request =~ "content-type: text/plain\r\n"
    refute request =~ "content-length: 999\r\n"
    assert request =~ "\r\n\r\n{\"ok\":true}"
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

  test "rejects non-loopback HTTP URLs before connecting" do
    assert {:error, {:unsupported_url, :non_loopback_host}} =
             LocalHttpClient.request(:get, "http://example.com/api")
  end

  test "rejects HTTPS URLs" do
    assert {:error, {:unsupported_url, {:scheme, "https"}}} =
             LocalHttpClient.request(:get, "https://127.0.0.1/api")
  end

  defp start_server(body, status) when is_binary(body) and is_integer(status) do
    start_capturing_server(body, status, nil)
  end

  defp start_capturing_server(body, status, recipient) when is_binary(body) and is_integer(status) do
    {:ok, listen_socket} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, {_addr, port}} = :inet.sockname(listen_socket)

    server =
      spawn_link(fn ->
        {:ok, socket} = :gen_tcp.accept(listen_socket)
        {:ok, request} = recv_request(socket)
        if recipient, do: send(recipient, {:request, request})
        :ok = :gen_tcp.send(socket, response(status, body))
        :ok = :gen_tcp.close(socket)
        :ok = :gen_tcp.close(listen_socket)
      end)

    {:ok, "http://127.0.0.1:#{port}", server}
  end

  defp override_headers do
    [
      {"authorization", "Bearer token"},
      {"connection", "keep-alive"},
      {"content-type", "text/plain"},
      {"content-length", "999"}
    ]
  end

  defp recv_request(socket, buffer \\ "") do
    case :gen_tcp.recv(socket, 0, 2_000) do
      {:ok, chunk} ->
        request = buffer <> chunk

        if complete_request?(request) do
          {:ok, request}
        else
          recv_request(socket, request)
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp complete_request?(request) do
    case :binary.split(request, "\r\n\r\n") do
      [headers, body] -> byte_size(body) >= content_length(headers)
      _other -> false
    end
  end

  defp content_length(headers) do
    headers
    |> String.split("\r\n")
    |> Enum.find_value(0, fn header ->
      case String.split(header, ":", parts: 2) do
        [key, value] -> parse_content_length(key, value)
        _other -> nil
      end
    end)
  end

  defp parse_content_length(key, value) do
    if String.downcase(key) == "content-length" do
      value
      |> String.trim()
      |> Integer.parse()
      |> case do
        {length, ""} -> length
        _other -> 0
      end
    end
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
