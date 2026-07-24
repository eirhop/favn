defmodule Favn.CLI.HttpClientTest do
  use ExUnit.Case, async: false

  alias Favn.CLI.HttpClient

  test "accepts HTTPS targets instead of applying the loopback-only local policy" do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)

    assert {:error, {:connect_failed, _reason}} =
             HttpClient.request(:get, "https://127.0.0.1:#{port}/health", [], nil,
               connect_timeout_ms: 100,
               timeout_ms: 100
             )
  end

  test "rejects credentials embedded in a URL" do
    assert {:error, {:unsupported_url, :userinfo}} =
             HttpClient.request(:get, "https://operator:secret@control.internal/api")
  end

  test "POST defaults to JSON content type without overriding the caller" do
    {url, first} = start_server(200, [], ~s({"ok":true}))

    assert {:ok, %{"ok" => true}} = HttpClient.request(:post, url, [], "{}")
    assert_receive {:request, first_request}
    assert request_headers(first_request)["content-type"] == "application/json"
    Task.await(first)

    {url, second} = start_server(200, [], ~s({"ok":true}))

    assert {:ok, %{"ok" => true}} =
             HttpClient.request(:post, url, [{"Content-Type", "application/problem+json"}], "{}")

    assert_receive {:request, second_request}
    assert request_headers(second_request)["content-type"] == "application/problem+json"
    Task.await(second)
  end

  test "does not follow redirects or forward request headers" do
    {:ok, target_socket} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, {_address, target_port}} = :inet.sockname(target_socket)

    {url, server} =
      start_server(
        302,
        [{"location", "http://127.0.0.1:#{target_port}/captured"}],
        ~s({"error":{"code":"redirect"}})
      )

    assert {:error, {:http_error, 302, %{error_code: "redirect"}}} =
             HttpClient.request(:get, url, [{"authorization", "Bearer never-forward"}])

    assert {:error, :timeout} = :gen_tcp.accept(target_socket, 100)
    :ok = :gen_tcp.close(target_socket)
    Task.await(server)
  end

  test "does not retry retryable-looking responses" do
    parent = self()
    {:ok, listen_socket} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, {_address, port}} = :inet.sockname(listen_socket)

    server =
      Task.async(fn ->
        {:ok, socket} = :gen_tcp.accept(listen_socket)
        send(parent, {:request, receive_request(socket, "")})
        :ok = :gen_tcp.send(socket, response(503, [{"retry-after", "0"}], ~s({"error":{}})))
        :ok = :gen_tcp.close(socket)
        result = :gen_tcp.accept(listen_socket, 100)
        :ok = :gen_tcp.close(listen_socket)
        result
      end)

    assert {:error, {:http_error, 503, _summary}} =
             HttpClient.request(:post, "http://127.0.0.1:#{port}/", [], "{}")

    assert_receive {:request, _request}
    assert {:error, :timeout} = Task.await(server)
  end

  test "bounds response bytes and redacts error bodies" do
    {large_url, large_server} =
      start_server(200, [], JSON.encode!(%{data: String.duplicate("x", 32)}))

    assert {:error, {:response_too_large, 16}} =
             HttpClient.request(:get, large_url, [], nil, max_response_bytes: 16)

    Task.await(large_server)

    body = ~s({"error":{"code":"invalid","message":"sensitive response body"}})
    {error_url, error_server} = start_server(400, [], body)

    assert {:error, {:http_error, 400, %{body_size: body_size, error_code: "invalid"}} = reason} =
             HttpClient.request(:get, error_url)

    assert body_size == byte_size(body)
    refute inspect(reason) =~ "sensitive response body"
    Task.await(error_server)
  end

  test "starts the TLS application before an HTTPS connection" do
    assert {:error, {:connect_failed, _reason}} =
             HttpClient.request(:get, "https://127.0.0.1:#{unused_port()}/", [], nil,
               connect_timeout_ms: 100,
               timeout_ms: 100
             )

    assert Enum.any?(Application.started_applications(), &match?({:ssl, _, _}, &1))
  end

  defp start_server(status, headers, body) do
    parent = self()
    {:ok, listen_socket} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, {_address, port}} = :inet.sockname(listen_socket)

    server =
      Task.async(fn ->
        {:ok, socket} = :gen_tcp.accept(listen_socket)
        send(parent, {:request, receive_request(socket, "")})
        :ok = :gen_tcp.send(socket, response(status, headers, body))
        :ok = :gen_tcp.close(socket)
        :ok = :gen_tcp.close(listen_socket)
      end)

    {"http://127.0.0.1:#{port}/", server}
  end

  defp receive_request(socket, acc) do
    {:ok, chunk} = :gen_tcp.recv(socket, 0, 2_000)
    request = acc <> chunk

    case String.split(request, "\r\n\r\n", parts: 2) do
      [headers, body] ->
        if byte_size(body) >= content_length(headers),
          do: request,
          else: receive_request(socket, request)

      _incomplete ->
        receive_request(socket, request)
    end
  end

  defp content_length(headers) do
    headers
    |> String.split("\r\n")
    |> Enum.find_value(0, fn line ->
      case String.split(line, ":", parts: 2) do
        [key, value] ->
          if String.downcase(key) == "content-length",
            do: value |> String.trim() |> String.to_integer()

        _other ->
          nil
      end
    end)
  end

  defp request_headers(request) do
    request
    |> String.split("\r\n\r\n", parts: 2)
    |> hd()
    |> String.split("\r\n")
    |> Enum.drop(1)
    |> Map.new(fn line ->
      [key, value] = String.split(line, ":", parts: 2)
      {String.downcase(key), String.trim(value)}
    end)
  end

  defp response(status, headers, body) do
    [
      "HTTP/1.1 #{status} Response\r\n",
      Enum.map(headers, fn {key, value} -> "#{key}: #{value}\r\n" end),
      "content-type: application/json\r\n",
      "content-length: #{byte_size(body)}\r\n",
      "connection: close\r\n\r\n",
      body
    ]
  end

  defp unused_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, {_address, port}} = :inet.sockname(socket)
    :ok = :gen_tcp.close(socket)
    port
  end
end
