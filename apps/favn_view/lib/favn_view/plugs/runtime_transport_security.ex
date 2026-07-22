defmodule FavnView.Plugs.RuntimeTransportSecurity do
  @moduledoc false

  @behaviour Plug

  alias FavnView.Plugs.TrustedProxyHeaders
  alias FavnView.ProductionRuntimeConfig

  @ssl_options Plug.SSL.init(host: nil, hsts: true)

  @impl true
  def init(opts), do: opts

  @impl true
  def call(conn, _opts) do
    if ProductionRuntimeConfig.force_ssl?() do
      conn
      |> TrustedProxyHeaders.call([])
      |> Plug.SSL.call(@ssl_options)
    else
      conn
    end
  end
end
