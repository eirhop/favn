defmodule FavnViewWeb.Router do
  use Phoenix.Router

  import Phoenix.Controller

  import Phoenix.LiveView.Router

  pipeline :browser do
    plug(:accepts, ["html"])
    plug(:fetch_session)
    plug(:fetch_live_flash)
    plug(:put_root_layout, html: {FavnViewWeb.Layouts, :root})
    plug(:protect_from_forgery)
    plug(:put_secure_browser_headers)
  end

  scope "/", FavnViewWeb do
    pipe_through(:browser)

    live_session :default do
      live("/", DashboardLive)
      live("/runs", Runs.IndexLive)
      live("/runs/:run_id", Runs.ShowLive)
      live("/manifests", Manifests.IndexLive)
      live("/manifests/:manifest_version_id", Manifests.ShowLive)
      live("/scheduler", Scheduler.IndexLive)
    end
  end
end
