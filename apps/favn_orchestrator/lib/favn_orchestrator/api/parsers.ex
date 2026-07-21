defmodule FavnOrchestrator.API.Parsers do
  @moduledoc false

  @behaviour Plug

  alias FavnOrchestrator.RuntimeConfig

  @impl true
  def init(opts), do: opts

  @impl true
  def call(conn, _opts) do
    limits = RuntimeConfig.http_server()

    parser_opts =
      Plug.Parsers.init(
        parsers: [:json],
        pass: ["application/json"],
        length: limits.body_limit_bytes,
        read_timeout: limits.request_timeout_ms,
        json_decoder: Jason
      )

    Plug.Parsers.call(conn, parser_opts)
  end
end
