defmodule FavnView.Router do
  use FavnView, :router

  pipeline :browser do
    plug :accepts, ["html"]
    plug :fetch_session
    plug :fetch_live_flash
    plug :put_root_layout, html: {FavnView.Layouts, :root}
    plug :protect_from_forgery
    plug :put_secure_browser_headers
  end

  pipeline :api do
    plug :accepts, ["json"]
  end

  scope "/", FavnView do
    pipe_through :browser

    live "/", PageLive, :home
    live "/assets", AssetCatalogueLive, :index
    live "/assets/:asset_id", AssetDetailLive, :show
    live "/pipelines", PipelinesLive, :index
    live "/logs", LogsLive, :index
    live "/runs", RunsListLive, :index
    live "/runs/:run_id", RunDetailLive, :show
    live "/runs/:run_id/logs", RunLogsLive, :show
    live "/runs/:run_id/assets/:asset_step_id/logs", AssetRunLogsLive, :show
  end

  # Other scopes may use custom stacks.
  # scope "/api", FavnView do
  #   pipe_through :api
  # end

  # Enable LiveDashboard in development.
  if Application.compile_env(:favn_view, :dev_routes) do
    # If you want to use the LiveDashboard in production, you should put
    # it behind authentication and allow only admins to access it.
    # If your application does not have an admins-only section yet,
    # you can use Plug.BasicAuth to set up some basic authentication
    # as long as you are also using SSL (which you should anyway).
    import Phoenix.LiveDashboard.Router
    import PhoenixStorybook.Router

    scope "/" do
      storybook_assets()
    end

    scope "/" do
      pipe_through :browser

      live_storybook("/storybook", backend_module: FavnView.Storybook)
    end

    scope "/dev" do
      pipe_through :browser

      live_dashboard "/dashboard", metrics: FavnView.Telemetry
    end
  end
end
