defmodule Favn.Dev.LocalHttpClient do
  @moduledoc false

  @default_connect_timeout_ms 1_000
  @default_timeout_ms 5_000

  @type method :: :get | :post
  @type header :: {String.t(), String.t()}
  @type response ::
          {:ok, map() | list()}
          | {:error, {:http_error, non_neg_integer(), term()}}
          | {:error, {:connect_failed, term()}}
          | {:error, {:timeout, :connect | :request}}
          | {:error, {:invalid_json, binary()}}
          | {:error, {:invalid_response, term()}}

  @spec request(method(), String.t(), [header()], iodata() | nil, keyword()) :: response()
  def request(method, url, headers \\ [], body \\ nil, opts \\ [])
      when method in [:get, :post] and is_binary(url) and is_list(headers) do
    case Application.ensure_all_started(:inets) do
      {:ok, _apps} ->
        method
        |> do_request(url, headers, body, opts)
        |> normalize_response()

      {:error, reason} -> {:error, {:invalid_response, {:inets_start_failed, reason}}}
    end
  end

  defp do_request(:get, url, headers, _body, opts) do
    :httpc.request(
      :get,
      {String.to_charlist(url), normalize_headers(headers)},
      http_options(opts),
      request_options()
    )
  end

  defp do_request(:post, url, headers, body, opts) do
    :httpc.request(
      :post,
      {String.to_charlist(url), normalize_headers(headers), ~c"application/json", IO.iodata_to_binary(body || "")},
      http_options(opts),
      request_options()
    )
  end

  defp normalize_headers(headers) do
    Enum.map(headers, fn {key, value} -> {String.to_charlist(key), String.to_charlist(value)} end)
  end

  defp http_options(opts) do
    [
      connect_timeout: Keyword.get(opts, :connect_timeout_ms, @default_connect_timeout_ms),
      timeout: Keyword.get(opts, :timeout_ms, @default_timeout_ms),
      autoredirect: false
    ]
  end

  defp request_options do
    [body_format: :binary]
  end

  defp normalize_response({:ok, {{_http_version, status, _reason}, _headers, body}})
       when is_integer(status) and is_binary(body) do
    if status >= 200 and status < 300 do
      decode_json(body)
    else
      {:error, {:http_error, status, decode_json_or_body(body)}}
    end
  end

  defp normalize_response({:ok, raw}), do: {:error, {:invalid_response, raw}}

  defp normalize_response({:error, reason}) do
    cond do
      timeout_reason?(reason) -> {:error, {:timeout, :request}}
      connect_failed_reason?(reason) -> {:error, {:connect_failed, reason}}
      true -> {:error, {:invalid_response, reason}}
    end
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

  defp timeout_reason?(:timeout), do: true
  defp timeout_reason?({:timeout, _}), do: true
  defp timeout_reason?({:failed_connect, details}), do: Enum.any?(List.wrap(details), &timeout_reason?/1)
  defp timeout_reason?(_reason), do: false

  defp connect_failed_reason?({:failed_connect, _details}), do: true
  defp connect_failed_reason?(:econnrefused), do: true
  defp connect_failed_reason?(:nxdomain), do: true
  defp connect_failed_reason?(:enetunreach), do: true
  defp connect_failed_reason?(_reason), do: false
end
