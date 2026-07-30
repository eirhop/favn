defmodule FavnView.Plugs.RuntimeTransportSecurity do
  @moduledoc false

  @behaviour Plug

  alias FavnView.Plugs.TrustedProxyHeaders
  alias FavnView.ProductionRuntimeConfig

  @impl true
  def init(opts), do: opts

  @impl true
  def call(conn, _opts) do
    if ProductionRuntimeConfig.force_ssl?() do
      conn = TrustedProxyHeaders.call(conn, [])

      if local_readiness_probe?(conn) do
        conn
      else
        Plug.SSL.call(conn, ProductionRuntimeConfig.ssl_options())
      end
    else
      conn
    end
  end

  # The immutable image healthcheck exercises the real View listener from the
  # same network namespace. Keep this exception bound to the exact loopback
  # peer, GET method, and readiness path; the Host header grants no authority.
  defp local_readiness_probe?(%{method: "GET", request_path: "/api/web/v1/health/ready"} = conn) do
    case Plug.Conn.get_peer_data(conn) do
      %{address: {127, 0, 0, 1}} -> true
      %{address: {0, 0, 0, 0, 0, 0, 0, 1}} -> true
      _other -> false
    end
  end

  defp local_readiness_probe?(_conn), do: false
end
