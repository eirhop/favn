defmodule Favn.Dev.HttpClient do
  @moduledoc false

  @default_connect_timeout_ms 5_000
  @default_timeout_ms 60_000
  @default_max_response_bytes 4 * 1024 * 1024
  @maximum_response_bytes 16 * 1024 * 1024

  @type response ::
          {:ok, map() | list()}
          | {:error, {:http_error, non_neg_integer(), map()}}
          | {:error, {:connect_failed, term()}}
          | {:error, {:timeout, :request}}
          | {:error, {:invalid_url, term()}}
          | {:error, {:unsupported_url, term()}}
          | {:error, {:invalid_json, map()}}
          | {:error, {:invalid_response, term()}}
          | {:error, {:response_too_large, pos_integer()}}

  @spec request(
          :delete | :get | :post,
          String.t(),
          [{String.t(), String.t()}],
          iodata() | nil,
          keyword()
        ) ::
          response()
  def request(method, url, headers \\ [], body \\ nil, opts \\ [])
      when method in [:delete, :get, :post] and is_binary(url) and is_list(headers) do
    with {:ok, uri} <- validate_url(url),
         {:ok, limits} <- limits(opts),
         {:ok, response} <- perform(method, uri, headers, body, limits) do
      decode_response(response)
    end
  end

  defp validate_url(url) do
    case URI.parse(url) do
      %URI{scheme: scheme, host: host, userinfo: nil} = uri
      when scheme in ["http", "https"] and is_binary(host) and host != "" ->
        {:ok, uri}

      %URI{scheme: scheme} when is_binary(scheme) and scheme not in ["http", "https"] ->
        {:error, {:unsupported_url, {:scheme, scheme}}}

      %URI{userinfo: userinfo} when is_binary(userinfo) ->
        {:error, {:unsupported_url, :userinfo}}

      _invalid ->
        {:error, {:invalid_url, :malformed}}
    end
  end

  defp limits(opts) do
    connect_timeout = Keyword.get(opts, :connect_timeout_ms, @default_connect_timeout_ms)
    timeout = Keyword.get(opts, :timeout_ms, @default_timeout_ms)
    max_bytes = Keyword.get(opts, :max_response_bytes, @default_max_response_bytes)

    if positive_integer?(connect_timeout) and positive_integer?(timeout) and
         max_bytes in 1..@maximum_response_bytes do
      {:ok,
       %{connect_timeout_ms: connect_timeout, timeout_ms: timeout, max_response_bytes: max_bytes}}
    else
      {:error, {:invalid_response, :invalid_http_limits}}
    end
  end

  defp positive_integer?(value), do: is_integer(value) and value > 0

  defp perform(method, uri, headers, body, limits) do
    task = Task.async(fn -> perform_isolated(method, uri, headers, body, limits) end)
    task_timeout = limits.connect_timeout_ms + limits.timeout_ms + 1_000

    case Task.yield(task, task_timeout) || Task.shutdown(task, :brutal_kill) do
      {:ok, result} -> result
      nil -> {:error, {:timeout, :request}}
    end
  end

  defp perform_isolated(method, uri, headers, body, limits) do
    scheme = String.to_existing_atom(uri.scheme)
    port = uri.port || if(scheme == :https, do: 443, else: 80)
    headers = request_headers(method, headers)

    with :ok <- ensure_transport_started(scheme) do
      connect_opts = [
        protocols: [:http1],
        transport_opts: transport_options(scheme, limits.connect_timeout_ms)
      ]

      case Mint.HTTP.connect(scheme, uri.host, port, connect_opts) do
        {:ok, conn} -> request_and_receive(conn, method, uri, headers, body, limits)
        {:error, reason} -> {:error, {:connect_failed, normalize_transport_error(reason)}}
      end
    end
  rescue
    _error -> {:error, {:connect_failed, :transport_error}}
  end

  defp ensure_transport_started(:http), do: :ok

  defp ensure_transport_started(:https) do
    case Application.ensure_all_started(:ssl) do
      {:ok, _applications} -> :ok
      {:error, _reason} -> {:error, {:connect_failed, :tls_unavailable}}
    end
  end

  defp request_headers(:post, headers) do
    if Enum.any?(headers, fn {name, _value} -> String.downcase(name) == "content-type" end) do
      headers
    else
      [{"content-type", "application/json"} | headers]
    end
  end

  defp request_headers(:get, headers), do: headers
  defp request_headers(:delete, headers), do: headers

  defp transport_options(:https, timeout) do
    [timeout: timeout, cacerts: :public_key.cacerts_get(), verify: :verify_peer]
  end

  defp transport_options(:http, timeout), do: [timeout: timeout]

  defp request_and_receive(conn, method, uri, headers, body, limits) do
    method = method |> Atom.to_string() |> String.upcase()

    case Mint.HTTP.request(conn, method, request_target(uri), headers, body) do
      {:ok, conn, request_ref} ->
        deadline = System.monotonic_time(:millisecond) + limits.timeout_ms

        {result, conn} =
          receive_response(conn, request_ref, deadline, limits.max_response_bytes, %{
            status: nil,
            headers: [],
            body: [],
            size: 0
          })

        _ = Mint.HTTP.close(conn)
        result

      {:error, conn, reason} ->
        _ = Mint.HTTP.close(conn)
        {:error, {:connect_failed, normalize_transport_error(reason)}}
    end
  end

  defp receive_response(conn, request_ref, deadline, max_bytes, state) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      message ->
        case Mint.HTTP.stream(conn, message) do
          {:ok, conn, responses} ->
            handle_responses(conn, request_ref, deadline, max_bytes, state, responses)

          {:error, conn, reason, _responses} ->
            {{:error, {:connect_failed, normalize_transport_error(reason)}}, conn}

          :unknown ->
            receive_response(conn, request_ref, deadline, max_bytes, state)
        end
    after
      remaining -> {{:error, {:timeout, :request}}, conn}
    end
  end

  defp handle_responses(conn, request_ref, deadline, max_bytes, state, responses) do
    Enum.reduce_while(responses, {:continue, state}, fn
      {:status, ^request_ref, status}, {:continue, acc} ->
        {:cont, {:continue, %{acc | status: status}}}

      {:headers, ^request_ref, headers}, {:continue, acc} ->
        {:cont, {:continue, %{acc | headers: headers}}}

      {:data, ^request_ref, bytes}, {:continue, acc} ->
        size = acc.size + byte_size(bytes)

        if size <= max_bytes do
          {:cont, {:continue, %{acc | body: [acc.body, bytes], size: size}}}
        else
          {:halt, {:error, {:response_too_large, max_bytes}}}
        end

      {:done, ^request_ref}, {:continue, acc} ->
        body = IO.iodata_to_binary(acc.body)
        {:halt, {:done, %{status: acc.status, headers: acc.headers, body: body}}}

      {:error, ^request_ref, reason}, {:continue, _acc} ->
        {:halt, {:error, {:connect_failed, normalize_transport_error(reason)}}}

      _unrelated, result ->
        {:cont, result}
    end)
    |> case do
      {:continue, state} -> receive_response(conn, request_ref, deadline, max_bytes, state)
      {:done, response} -> {{:ok, response}, conn}
      {:error, reason} -> {{:error, reason}, conn}
    end
  end

  defp request_target(%URI{path: path, query: query}) do
    path = if path in [nil, ""], do: "/", else: path
    if is_binary(query), do: path <> "?" <> query, else: path
  end

  defp decode_response(%{status: status, body: body}) when status in 200..299 do
    case JSON.decode(body) do
      {:ok, decoded} when is_map(decoded) or is_list(decoded) -> {:ok, decoded}
      {:ok, decoded} -> {:error, {:invalid_response, response_shape(decoded)}}
      {:error, _reason} -> {:error, {:invalid_json, %{body_size: byte_size(body)}}}
    end
  end

  defp decode_response(%{status: status, body: body}) when is_integer(status) do
    {:error, {:http_error, status, error_summary(body)}}
  end

  defp decode_response(_response), do: {:error, {:invalid_response, :missing_status}}

  defp error_summary(body) do
    code =
      case JSON.decode(body) do
        {:ok, %{"error" => %{"code" => code}}} when is_binary(code) -> code
        _other -> nil
      end

    %{body_size: byte_size(body), error_code: code}
  end

  defp response_shape(value) when is_binary(value), do: :string
  defp response_shape(value) when is_number(value), do: :number
  defp response_shape(value) when is_boolean(value), do: :boolean
  defp response_shape(nil), do: :null
  defp response_shape(_value), do: :unknown

  defp normalize_transport_error({reason, _details}) when is_atom(reason), do: reason
  defp normalize_transport_error(reason) when is_atom(reason), do: reason
  defp normalize_transport_error(_reason), do: :transport_error
end
