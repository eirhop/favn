defmodule FavnOrchestrator.API.ManifestPublication do
  @moduledoc """
  Parses bounded manifest-artifact publication requests before the general API parser.

  The publication endpoint accepts plain or gzip JSON. Service authentication
  happens before the body is read, compressed input and expanded JSON have
  independent limits, and malformed or oversized requests receive stable JSON
  errors.
  """

  @behaviour Plug

  import Plug.Conn,
    only: [get_req_header: 2, halt: 1, put_resp_header: 3, read_body: 2]

  alias FavnOrchestrator.API.Authentication
  alias FavnOrchestrator.API.ManifestPublication.Config
  alias FavnOrchestrator.RuntimeConfig
  alias FavnOrchestrator.API.Response

  @publication_paths %{
    "/api/orchestrator/v1/manifests" => true,
    "/api/orchestrator/v1/execution-packages" => true,
    "/api/orchestrator/v1/execution-packages/missing" => true
  }
  @read_length_bytes 1 * 1024 * 1024
  @gzip_window_bits 16 + 15

  @impl true
  def init(opts), do: opts

  @impl true
  def call(
        %Plug.Conn{
          method: "POST",
          request_path: request_path,
          body_params: %Plug.Conn.Unfetched{}
        } = conn,
        _opts
      )
      when is_map_key(@publication_paths, request_path) do
    case Authentication.ensure_service(conn) do
      :ok -> parse_authenticated(conn)
      {:error, :service_unauthorized} -> service_unauthorized(conn)
    end
  end

  def call(conn, _opts), do: conn

  defp parse_authenticated(conn) do
    config = RuntimeConfig.manifest_publication()

    with :ok <- validate_content_type(conn),
         {:ok, encoding} <- content_encoding(conn),
         :ok <- validate_declared_size(conn, encoding, config) do
      read_and_parse(conn, encoding, config)
    else
      {:error, {:unsupported_media_type, media_type}} ->
        unsupported_media_type(conn, media_type)

      {:error, {:unsupported_content_encoding, encoding}} ->
        unsupported_content_encoding(conn, encoding)

      {:error, {:too_large, kind, size, limit, source, encoding}} ->
        payload_too_large(conn, kind, size, limit, source, encoding)
    end
  end

  defp read_and_parse(conn, encoding, config) do
    limit = encoded_limit(encoding, config)
    read_limit = limit + 1

    case read_body(conn,
           length: read_limit,
           read_length: min(read_limit, @read_length_bytes),
           read_timeout: RuntimeConfig.http_server().request_timeout_ms
         ) do
      {:ok, body, conn} when byte_size(body) <= limit ->
        decode_and_parse(conn, body, encoding, config)

      {:ok, body, conn} ->
        payload_too_large(
          conn,
          encoded_limit_kind(encoding),
          byte_size(body),
          limit,
          :observed_at_least,
          encoding
        )

      {:more, body, conn} ->
        payload_too_large(
          conn,
          encoded_limit_kind(encoding),
          max(byte_size(body), limit + 1),
          limit,
          :observed_at_least,
          encoding
        )

      {:error, :timeout} ->
        request_read_error(conn, 408, "request_timeout", "Manifest publication body timed out")

      {:error, _reason} ->
        request_read_error(conn, 400, "invalid_request_body", "Manifest publication body failed")
    end
  end

  defp decode_and_parse(conn, encoded, encoding, config) do
    case decode(encoded, encoding, config.decompressed_limit_bytes) do
      {:ok, json} ->
        decode_json(conn, json)

      {:error, {:too_large, size}} ->
        payload_too_large(
          conn,
          :decompressed,
          size,
          config.decompressed_limit_bytes,
          :observed_at_least,
          encoding
        )

      {:error, :malformed_gzip} ->
        conn
        |> Response.error(
          400,
          "invalid_compression",
          "Manifest publication gzip body is malformed"
        )
        |> halt()
    end
  end

  defp decode_json(conn, json) do
    case Jason.decode(json) do
      {:ok, params} when is_map(params) ->
        %{conn | body_params: params}

      {:ok, value} ->
        %{conn | body_params: %{"_json" => value}}

      {:error, _reason} ->
        conn
        |> Response.error(400, "invalid_json", "Manifest publication body is not valid JSON")
        |> halt()
    end
  end

  defp validate_content_type(conn) do
    case get_req_header(conn, "content-type") do
      [value] ->
        media_type =
          value
          |> String.split(";", parts: 2)
          |> hd()
          |> String.trim()
          |> String.downcase()

        if media_type == "application/json" or
             (String.starts_with?(media_type, "application/") and
                String.ends_with?(media_type, "+json")) do
          :ok
        else
          {:error, {:unsupported_media_type, media_type}}
        end

      _other ->
        {:error, {:unsupported_media_type, nil}}
    end
  end

  defp content_encoding(conn) do
    case get_req_header(conn, "content-encoding") do
      [] -> {:ok, :identity}
      [value] -> normalize_content_encoding(value)
      values -> {:error, {:unsupported_content_encoding, Enum.join(values, ",")}}
    end
  end

  defp normalize_content_encoding(value) do
    case value |> String.trim() |> String.downcase() do
      "" -> {:ok, :identity}
      "identity" -> {:ok, :identity}
      "gzip" -> {:ok, :gzip}
      unsupported -> {:error, {:unsupported_content_encoding, unsupported}}
    end
  end

  defp validate_declared_size(conn, encoding, config) do
    limit = encoded_limit(encoding, config)

    case declared_content_length(conn) do
      size when is_integer(size) and size > limit ->
        {:error,
         {:too_large, encoded_limit_kind(encoding), size, limit, :content_length, encoding}}

      _size ->
        :ok
    end
  end

  defp declared_content_length(conn) do
    case get_req_header(conn, "content-length") do
      [value] ->
        case Integer.parse(String.trim(value)) do
          {size, ""} when size >= 0 -> size
          _other -> nil
        end

      _other ->
        nil
    end
  end

  defp encoded_limit(:gzip, %Config{} = config), do: config.compressed_limit_bytes
  defp encoded_limit(:identity, %Config{} = config), do: config.decompressed_limit_bytes

  defp encoded_limit_kind(:gzip), do: :compressed
  defp encoded_limit_kind(:identity), do: :uncompressed

  defp decode(body, :identity, limit) when byte_size(body) <= limit, do: {:ok, body}
  defp decode(body, :identity, _limit), do: {:error, {:too_large, byte_size(body)}}
  defp decode(body, :gzip, limit), do: gunzip_bounded(body, limit)

  defp gunzip_bounded(body, limit) do
    stream = :zlib.open()

    try do
      :ok = :zlib.inflateInit(stream, @gzip_window_bits, :reset)

      case inflate_bounded(stream, :zlib.safeInflate(stream, body), [], 0, limit) do
        {:ok, decoded} ->
          :ok = :zlib.inflateEnd(stream)
          {:ok, decoded}

        {:error, _reason} = error ->
          error
      end
    rescue
      _error -> {:error, :malformed_gzip}
    catch
      _kind, _reason -> {:error, :malformed_gzip}
    after
      :zlib.close(stream)
    end
  end

  defp inflate_bounded(stream, {status, output}, chunks, size, limit)
       when status in [:continue, :finished] do
    next_size = size + :erlang.iolist_size(output)

    cond do
      next_size > limit ->
        {:error, {:too_large, next_size}}

      status == :continue ->
        inflate_bounded(
          stream,
          :zlib.safeInflate(stream, []),
          [output | chunks],
          next_size,
          limit
        )

      status == :finished ->
        {:ok, IO.iodata_to_binary(Enum.reverse([output | chunks]))}
    end
  end

  defp inflate_bounded(_stream, _result, _chunks, _size, _limit),
    do: {:error, :malformed_gzip}

  defp payload_too_large(conn, kind, size, limit, source, encoding) do
    conn
    |> put_resp_header("connection", "close")
    |> Response.error(
      413,
      "manifest_payload_too_large",
      "Manifest publication payload exceeds its configured size limit",
      %{
        encoding: Atom.to_string(encoding),
        limit_kind: Atom.to_string(kind),
        limit_bytes: limit,
        observed_size_bytes: size,
        size_source: Atom.to_string(source)
      }
    )
    |> halt()
  end

  defp unsupported_media_type(conn, media_type) do
    conn
    |> close_connection()
    |> Response.error(
      415,
      "unsupported_media_type",
      "Manifest publication requires application/json",
      %{media_type: media_type}
    )
    |> halt()
  end

  defp unsupported_content_encoding(conn, encoding) do
    conn
    |> close_connection()
    |> Response.error(
      415,
      "unsupported_content_encoding",
      "Manifest publication supports only identity or gzip content encoding",
      %{content_encoding: encoding}
    )
    |> halt()
  end

  defp service_unauthorized(conn) do
    conn
    |> close_connection()
    |> Response.error(401, "service_unauthorized", "Invalid service credentials")
    |> halt()
  end

  defp request_read_error(conn, status, code, message) do
    conn
    |> close_connection()
    |> Response.error(status, code, message)
    |> halt()
  end

  defp close_connection(conn), do: put_resp_header(conn, "connection", "close")
end
