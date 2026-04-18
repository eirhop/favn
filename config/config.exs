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

config :favn_orchestrator,
  api_server: [
    enabled: System.get_env("FAVN_ORCHESTRATOR_API_ENABLED") in ["1", "true", "TRUE"],
    port: String.to_integer(System.get_env("FAVN_ORCHESTRATOR_API_PORT", "4101"))
  ],
  api_service_tokens:
    String.split(
      System.get_env(
        "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS",
        if(Mix.env() == :test, do: "test-service-token", else: "")
      ),
      ",",
      trim: true
    ),
  auth_session_ttl_seconds:
    String.to_integer(System.get_env("FAVN_ORCHESTRATOR_AUTH_SESSION_TTL", "43200")),
  auth_login_failure_delay_ms:
    String.to_integer(System.get_env("FAVN_ORCHESTRATOR_AUTH_LOGIN_FAILURE_DELAY_MS", "100")),
  auth_rate_limit_window_seconds:
    String.to_integer(System.get_env("FAVN_ORCHESTRATOR_AUTH_RATE_LIMIT_WINDOW_SECONDS", "300")),
  auth_rate_limit_max_attempts:
    String.to_integer(System.get_env("FAVN_ORCHESTRATOR_AUTH_RATE_LIMIT_MAX_ATTEMPTS", "8")),
  auth_rate_limit_block_seconds:
    String.to_integer(System.get_env("FAVN_ORCHESTRATOR_AUTH_RATE_LIMIT_BLOCK_SECONDS", "60")),
  api_idempotency_ttl_seconds:
    String.to_integer(System.get_env("FAVN_ORCHESTRATOR_API_IDEMPOTENCY_TTL_SECONDS", "86400")),
  auth_bootstrap_username: System.get_env("FAVN_ORCHESTRATOR_BOOTSTRAP_USERNAME", ""),
  auth_bootstrap_password: System.get_env("FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD", ""),
  auth_bootstrap_display_name:
    System.get_env("FAVN_ORCHESTRATOR_BOOTSTRAP_DISPLAY_NAME", "Favn Admin")

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
