defmodule Favn.Dev.LocalHttpClient do
  @moduledoc false

  @default_connect_timeout_ms 1_000
  @default_timeout_ms 5_000
  @default_http_port 80
  @loopback_hosts ["127.0.0.1", "::1", "localhost"]

  @type method :: :get | :post
  @type header :: {String.t(), String.t()}
  @type response ::
          {:ok, map() | list()}
          | {:error, {:http_error, non_neg_integer(), term()}}
          | {:error, {:connect_failed, term()}}
          | {:error, {:timeout, :request}}
          | {:error, {:invalid_url, term()}}
          | {:error, {:unsupported_url, term()}}
          | {:error, {:invalid_json, binary()}}
          | {:error, {:invalid_response, term()}}

  @spec request(method(), String.t(), [header()], iodata() | nil, keyword()) :: response()
  def request(method, url, headers \\ [], body \\ nil, opts \\ [])
      when method in [:get, :post] and is_binary(url) and is_list(headers) do
    method
    |> do_request(url, headers, body, opts)
    |> normalize_response()
  end

  defp do_request(method, url, headers, body, opts) do
    with {:ok, target} <- parse_target(url),
         {:ok, socket} <- connect(target, opts) do
      try do
        with :ok <- send_request(socket, method, target, headers, body),
             {:ok, raw_response} <- receive_response(socket, opts) do
          parse_response(raw_response)
        end
      after
        :gen_tcp.close(socket)
      end
    end
  end

  defp parse_target(url) do
    case URI.parse(url) do
      %URI{scheme: "http", host: host} = uri when is_binary(host) ->
        if loopback_host?(host) do
          {:ok,
           %{
             host: host,
             connect_host: connect_host(host),
             host_header: host_header(host, uri.port),
             port: uri.port || @default_http_port,
             request_target: request_target(uri)
           }}
        else
          {:error, {:unsupported_url, :non_loopback_host}}
        end

      %URI{scheme: "https"} ->
        {:error, {:unsupported_url, {:scheme, "https"}}}

      %URI{scheme: "http"} ->
        {:error, {:invalid_url, :missing_host}}

      %URI{scheme: scheme} when is_binary(scheme) ->
        {:error, {:unsupported_url, {:scheme, scheme}}}

      _other ->
        {:error, {:invalid_url, url}}
    end
  end

  defp loopback_host?(host) when host in @loopback_hosts, do: true

  defp loopback_host?(host) do
    case :inet.parse_address(String.to_charlist(host)) do
      {:ok, {127, _second, _third, _fourth}} -> true
      {:ok, {0, 0, 0, 0, 0, 0, 0, 1}} -> true
      _other -> false
    end
  end

  defp connect_host("localhost"), do: ~c"localhost"

  defp connect_host(host) do
    case :inet.parse_address(String.to_charlist(host)) do
      {:ok, address} -> address
      _other -> String.to_charlist(host)
    end
  end

  defp host_header(host, nil), do: format_host(host)
  defp host_header(host, @default_http_port), do: format_host(host)
  defp host_header(host, port), do: format_host(host) <> ":#{port}"

  defp format_host(host) do
    if String.contains?(host, ":"), do: "[#{host}]", else: host
  end

  defp request_target(%URI{path: path, query: nil}), do: path_or_root(path)
  defp request_target(%URI{path: path, query: query}), do: path_or_root(path) <> "?" <> query

  defp path_or_root(nil), do: "/"
  defp path_or_root(""), do: "/"
  defp path_or_root(path), do: path

  defp connect(%{connect_host: host, port: port}, opts) do
    timeout_ms = Keyword.get(opts, :connect_timeout_ms, @default_connect_timeout_ms)

    case :gen_tcp.connect(host, port, connect_options(host), timeout_ms) do
      {:ok, socket} -> {:ok, socket}
      {:error, reason} -> {:error, reason}
    end
  end

  defp connect_options(host) when tuple_size(host) == 8 do
    [:binary, :inet6, active: false, packet: :raw]
  end

  defp connect_options(_host), do: [:binary, active: false, packet: :raw]

  defp send_request(socket, method, target, headers, body) do
    request_body = IO.iodata_to_binary(body || "")

    :gen_tcp.send(socket, [
      request_line(method, target.request_target),
      header_lines(request_headers(method, target, headers, request_body)),
      "\r\n",
      request_body
    ])
  end

  defp request_line(method, request_target) do
    method
    |> Atom.to_string()
    |> String.upcase()
    |> then(&[&1, " ", request_target, " HTTP/1.1\r\n"])
  end

  defp request_headers(:get, target, headers, _body) do
    headers
    |> normalize_headers()
    |> put_header("host", target.host_header)
    |> put_header("connection", "close")
  end

  defp request_headers(:post, target, headers, body) do
    headers
    |> normalize_headers()
    |> put_header("host", target.host_header)
    |> put_header("connection", "close")
    |> put_header("content-type", "application/json")
    |> put_header("content-length", Integer.to_string(byte_size(body)))
  end

  defp normalize_headers(headers) do
    Enum.map(headers, fn {key, value} -> {to_string(key), to_string(value)} end)
  end

  defp put_header(headers, key, value) do
    if Enum.any?(headers, fn {existing_key, _value} -> String.downcase(existing_key) == key end) do
      headers
    else
      headers ++ [{key, value}]
    end
  end

  defp header_lines(headers) do
    Enum.map(headers, fn {key, value} -> [key, ": ", value, "\r\n"] end)
  end

  defp receive_response(socket, opts) do
    recv_all(socket, [], Keyword.get(opts, :timeout_ms, @default_timeout_ms))
  end

  defp recv_all(socket, chunks, timeout_ms) do
    case :gen_tcp.recv(socket, 0, timeout_ms) do
      {:ok, chunk} -> recv_all(socket, [chunk | chunks], timeout_ms)
      {:error, :closed} -> {:ok, IO.iodata_to_binary(Enum.reverse(chunks))}
      {:error, :timeout} -> {:error, :timeout}
      {:error, reason} -> {:error, reason}
    end
  end

  defp parse_response(raw_response) when is_binary(raw_response) do
    case :binary.split(raw_response, "\r\n\r\n") do
      [raw_headers, body] -> parse_response_headers(raw_headers, body)
      _other -> {:error, {:invalid_response, raw_response}}
    end
  end

  defp parse_response_headers(raw_headers, body) do
    raw_headers
    |> String.split("\r\n")
    |> case do
      [status_line | _headers] -> parse_status_line(status_line, body)
      _other -> {:error, {:invalid_response, raw_headers}}
    end
  end

  defp parse_status_line(status_line, body) do
    case String.split(status_line, " ", parts: 3) do
      ["HTTP/" <> _version, status, _reason] ->
        case Integer.parse(status) do
          {status_code, ""} -> {:ok, {status_code, body}}
          _other -> {:error, {:invalid_response, status_line}}
        end

      _other ->
        {:error, {:invalid_response, status_line}}
    end
  end

  defp normalize_response({:ok, {status, body}}) when is_integer(status) and is_binary(body) do
    if status >= 200 and status < 300 do
      decode_json(body)
    else
      {:error, {:http_error, status, decode_json_or_body(body)}}
    end
  end

  defp normalize_response({:ok, raw}), do: {:error, {:invalid_response, raw}}

  defp normalize_response({:error, {:invalid_url, _reason}} = error), do: error
  defp normalize_response({:error, {:unsupported_url, _reason}} = error), do: error
  defp normalize_response({:error, {:invalid_response, _reason}} = error), do: error

  defp normalize_response({:error, reason}) do
    {:error, classify_transport_error(reason)}
  end

  defp decode_json(body) do
    case JSON.decode(body) do
      {:ok, decoded} -> {:ok, decoded}
      {:error, _reason} -> {:error, {:invalid_json, body}}
    end
  end

  defp decode_json_or_body(body) do
    case JSON.decode(body) do
      {:ok, decoded} -> decoded
      {:error, _reason} -> body
    end
  end

  defp classify_transport_error(reason) do
    cond do
      timeout_reason?(reason) -> {:timeout, :request}
      connect_failed_reason?(reason) -> {:connect_failed, reason}
      true -> {:invalid_response, reason}
    end
  end

  defp timeout_reason?(:timeout), do: true
  defp timeout_reason?({:timeout, _}), do: true
  defp timeout_reason?(_reason), do: false

  defp connect_failed_reason?(:econnrefused), do: true
  defp connect_failed_reason?(:nxdomain), do: true
  defp connect_failed_reason?(:enetunreach), do: true
  defp connect_failed_reason?(_reason), do: false
end
