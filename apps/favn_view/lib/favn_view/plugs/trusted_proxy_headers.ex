defmodule FavnView.Plugs.TrustedProxyHeaders do
  @moduledoc false

  @behaviour Plug

  import Plug.Conn, only: [delete_req_header: 2, get_peer_data: 1]

  alias FavnView.ProductionRuntimeConfig

  @forwarded_headers [
    "x-forwarded-for",
    "x-forwarded-host",
    "x-forwarded-port",
    "x-forwarded-proto"
  ]

  @rewrite_on Plug.RewriteOn.init([
                :x_forwarded_for,
                :x_forwarded_host,
                :x_forwarded_port,
                :x_forwarded_proto
              ])

  @impl true
  def init(opts), do: opts

  @impl true
  def call(conn, _opts) do
    case get_peer_data(conn) do
      %{address: address} ->
        if ProductionRuntimeConfig.trusted_proxy?(address) do
          Plug.RewriteOn.call(conn, @rewrite_on)
        else
          Enum.reduce(@forwarded_headers, conn, &delete_req_header(&2, &1))
        end

      _unknown_peer ->
        Enum.reduce(@forwarded_headers, conn, &delete_req_header(&2, &1))
    end
  end
end
