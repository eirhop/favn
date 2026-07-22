defmodule FavnView.Plugs.RuntimeSession do
  @moduledoc false

  @behaviour Plug

  alias FavnView.ProductionRuntimeConfig

  @impl true
  def init(opts), do: opts

  @impl true
  def call(conn, _opts) do
    options = ProductionRuntimeConfig.session_cookie_options()
    Plug.Session.call(conn, Plug.Session.init(options))
  end
end
