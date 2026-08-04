defmodule FavnView.Plugs.TrustedProxyHeaders do
  @moduledoc false

  @behaviour Plug

  import Plug.Conn, only: [delete_req_header: 2, get_peer_data: 1, get_req_header: 2]

  alias FavnView.ProductionRuntimeConfig

  @forwarded_headers [
    "forwarded",
    "x-forwarded-for",
    "x-forwarded-host",
    "x-forwarded-port",
    "x-forwarded-proto"
  ]

  @max_forwarded_for_bytes 4_096
  @max_forwarded_for_entries 32

  @impl true
  def init(opts), do: opts

  @impl true
  def call(conn, _opts) do
    case get_peer_data(conn) do
      %{address: address} ->
        if ProductionRuntimeConfig.trusted_proxy?(address) do
          apply_trusted_headers(conn)
        else
          strip_forwarded_headers(conn)
        end
    end
  end

  defp apply_trusted_headers(conn) do
    remote_ip =
      forwarded_client_ip(
        get_req_header(conn, "x-forwarded-for"),
        ProductionRuntimeConfig.forwarded_for_policy()
      )

    scheme = forwarded_scheme(get_req_header(conn, "x-forwarded-proto"))

    conn
    |> strip_forwarded_headers()
    |> maybe_put_remote_ip(remote_ip)
    |> maybe_put_scheme(scheme)
  end

  defp forwarded_client_ip(_headers, :ignore), do: :invalid

  defp forwarded_client_ip([header], :replace)
       when byte_size(header) <= @max_forwarded_for_bytes do
    if String.contains?(header, ",") do
      :invalid
    else
      parse_ip(header)
    end
  end

  defp forwarded_client_ip(_headers, :replace), do: :invalid

  defp forwarded_client_ip([header], :append)
       when byte_size(header) <= @max_forwarded_for_bytes do
    entries = String.split(header, ",", trim: false)

    if entries != [] and length(entries) <= @max_forwarded_for_entries do
      entries |> List.last() |> parse_ip()
    else
      :invalid
    end
  end

  defp forwarded_client_ip(_headers, :append), do: :invalid

  defp forwarded_scheme([header]) when byte_size(header) <= 8 do
    case header |> String.trim() |> String.downcase() do
      "https" -> :https
      "http" -> :http
      _invalid -> :invalid
    end
  end

  defp forwarded_scheme(_headers), do: :invalid

  defp parse_ip(value) do
    value
    |> String.trim()
    |> String.to_charlist()
    |> :inet.parse_address()
    |> case do
      {:ok, address} -> {:ok, address}
      {:error, _reason} -> :invalid
    end
  end

  defp maybe_put_remote_ip(conn, {:ok, remote_ip}), do: %{conn | remote_ip: remote_ip}
  defp maybe_put_remote_ip(conn, :invalid), do: conn

  defp maybe_put_scheme(conn, :https) do
    case ProductionRuntimeConfig.public_origin_connection() do
      {host, port} -> %{conn | scheme: :https, host: host, port: port}
      nil -> %{conn | scheme: :https}
    end
  end

  defp maybe_put_scheme(conn, :http), do: %{conn | scheme: :http}
  defp maybe_put_scheme(conn, :invalid), do: conn

  defp strip_forwarded_headers(conn) do
    Enum.reduce(@forwarded_headers, conn, &delete_req_header(&2, &1))
  end
end
