# This file is responsible for configuring your umbrella
# and **all applications** and their dependencies with the
# help of the Config module.
#
# Note that all applications in your umbrella share the
# same configuration and dependencies, which is why they
# all use the same configuration file. If you want different
# configurations or dependencies per app, it is best to
# move said applications out of the umbrella.
import Config

config :favn_view, FavnView.Endpoint,
  url: [host: "localhost"],
  adapter: Plug.Cowboy,
  render_errors: [
    formats: [html: FavnViewWeb.ErrorHTML, json: FavnViewWeb.ErrorJSON],
    layout: false
  ],
  pubsub_server: FavnView.PubSub,
  live_view: [signing_salt: System.get_env("FAVN_VIEW_SIGNING_SALT", "change-me-in-runtime")],
  secret_key_base:
    System.get_env(
      "FAVN_VIEW_SECRET_KEY_BASE",
      "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    ),
  server: false,
  check_origin: Mix.env() != :test

# Sample configuration:
#
#     config :logger, :default_handler,
#       level: :info
#
#     config :logger, :default_formatter,
#       format: "$date $time [$level] $metadata$message\n",
#       metadata: [:user_id]
#
