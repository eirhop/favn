defmodule FavnView.Endpoint do
  use Phoenix.Endpoint, otp_app: :favn_view

  @session_options [
    store: :cookie,
    key: "_favn_view_key",
    signing_salt: "viewsession"
  ]

  socket("/live", Phoenix.LiveView.Socket,
    websocket: [connect_info: [session: @session_options]],
    longpoll: false
  )

  plug(Plug.Static,
    at: "/",
    from: :favn_view,
    gzip: false,
    only: FavnView.static_paths()
  )

  plug(Plug.RequestId)
  plug(Plug.Telemetry, event_prefix: [:phoenix, :endpoint])

  plug(Plug.Parsers,
    parsers: [:urlencoded, :multipart, :json],
    pass: ["*/*"],
    json_decoder: Phoenix.json_library()
  )

  plug(Plug.MethodOverride)
  plug(Plug.Head)
  plug(Plug.Session, @session_options)
  plug(FavnViewWeb.Router)
end
