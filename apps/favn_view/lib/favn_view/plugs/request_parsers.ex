defmodule FavnView.Plugs.RequestParsers do
  @moduledoc false

  @behaviour Plug

  alias FavnView.ProductionRuntimeConfig

  @impl true
  def init(opts), do: opts

  @impl true
  def call(conn, _opts) do
    http = ProductionRuntimeConfig.http_server()

    parser_opts =
      Plug.Parsers.init(
        parsers: [:urlencoded, :json],
        pass: ["*/*"],
        length: http.body_limit_bytes,
        read_timeout: http.request_timeout_ms,
        json_decoder: Phoenix.json_library()
      )

    Plug.Parsers.call(conn, parser_opts)
  end
end
