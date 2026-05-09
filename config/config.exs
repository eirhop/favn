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

config :favn_view,
  generators: [context_app: false]

# Configures the endpoint
config :favn_view, FavnView.Endpoint,
  url: [host: "localhost"],
  adapter: Bandit.PhoenixAdapter,
  render_errors: [
    formats: [html: FavnView.ErrorHTML, json: FavnView.ErrorJSON],
    layout: false
  ],
  pubsub_server: FavnView.PubSub,
  live_view: [signing_salt: "Pqi8zx5Q"]

# Configure esbuild (the version is required)
config :esbuild,
  version: "0.25.4",
  favn_view: [
    args:
      ~w(js/app.js --bundle --target=es2022 --outdir=../priv/static/assets/js --external:/fonts/* --external:/images/* --alias:@=.),
    cd: Path.expand("../apps/favn_view/assets", __DIR__),
    env: %{"NODE_PATH" => [Path.expand("../deps", __DIR__), Mix.Project.build_path()]}
  ]

# Configure tailwind (the version is required)
config :tailwind,
  version: "4.1.12",
  favn_view: [
    args: ~w(
      --input=assets/css/app.css
      --output=priv/static/assets/css/app.css
    ),
    cd: Path.expand("../apps/favn_view", __DIR__)
  ]

if Mix.env() == :test do
  config :tzdata, :autoupdate, :disabled
end

service_token_env_default =
  if Mix.env() == :test,
    do: "favn_view:favn-view-local-credential-1234567890abcdef",
    else: ""

config :argon2_elixir,
  argon2_type: 2,
  t_cost: if(Mix.env() == :test, do: 1, else: 2),
  m_cost: if(Mix.env() == :test, do: 8, else: 15),
  parallelism: 1

config :favn_orchestrator,
  api_server: [
    enabled: System.get_env("FAVN_ORCHESTRATOR_API_ENABLED") in ["1", "true", "TRUE"],
    port: String.to_integer(System.get_env("FAVN_ORCHESTRATOR_API_PORT", "4101"))
  ],
  api_service_tokens: [],
  api_service_tokens_env:
    System.get_env("FAVN_ORCHESTRATOR_API_SERVICE_TOKENS", service_token_env_default),
  auth_session_ttl_seconds:
    String.to_integer(System.get_env("FAVN_ORCHESTRATOR_AUTH_SESSION_TTL", "43200")),
  auth_bootstrap_username: System.get_env("FAVN_ORCHESTRATOR_BOOTSTRAP_USERNAME", ""),
  auth_bootstrap_password: System.get_env("FAVN_ORCHESTRATOR_BOOTSTRAP_PASSWORD", ""),
  auth_bootstrap_display_name:
    System.get_env("FAVN_ORCHESTRATOR_BOOTSTRAP_DISPLAY_NAME", "Favn Admin"),
  auth_bootstrap_roles:
    System.get_env("FAVN_ORCHESTRATOR_BOOTSTRAP_ROLES", "admin")
    |> String.split(",", trim: true)

config :phoenix, :json_library, Jason

config :favn_view, FavnView.Endpoint,
  url: [host: "localhost"],
  adapter: Bandit.PhoenixAdapter,
  render_errors: [
    formats: [html: FavnView.ErrorHTML, json: FavnView.ErrorJSON],
    layout: false
  ],
  pubsub_server: FavnView.PubSub,
  live_view: [signing_salt: "snQDvb+z"]

config :esbuild,
  version: "0.25.4",
  favn_view: [
    args:
      ~w(js/app.js --bundle --target=es2022 --outdir=../priv/static/assets/js --external:/fonts/* --external:/images/*),
    cd: Path.expand("../apps/favn_view/assets", __DIR__),
    env: %{"NODE_PATH" => Path.expand("../deps", __DIR__)}
  ]

config :tailwind,
  version: "4.1.7",
  favn_view: [
    args: ~w(
      --input=assets/css/app.css
      --output=priv/static/assets/css/app.css
    ),
    cd: Path.expand("../apps/favn_view", __DIR__)
  ]

if config_env() == :dev do
  config :favn_view, FavnView.Endpoint,
    http: [
      ip: {127, 0, 0, 1},
      port:
        System.get_env("FAVN_VIEW_PORT", System.get_env("PORT", "4173"))
        |> String.to_integer()
    ],
    check_origin: false,
    code_reloader: true,
    debug_errors: true,
    secret_key_base: "favn_view_dev_secret_key_base_for_private_local_development_only_64_chars",
    watchers: [
      esbuild: {Esbuild, :install_and_run, [:favn_view, ~w(--sourcemap=inline --watch)]},
      tailwind: {Tailwind, :install_and_run, [:favn_view, ~w(--watch)]}
    ]

  config :favn_view, :dev_routes, true
end

if config_env() == :test do
  config :favn_view, FavnView.Endpoint,
    http: [ip: {127, 0, 0, 1}, port: 0],
    secret_key_base: "favn_view_test_secret_key_base_for_private_test_environment_only_64_chars",
    server: false

  config :favn_view, :dev_routes, false
end

# Sample configuration:
#
#     config :logger, :default_handler,
#       level: :info
#
#     config :logger, :default_formatter,
#       format: "$date $time [$level] $metadata$message\n",
#       metadata: [:user_id]
#
import Config

# Configure Elixir's Logger
config :logger, :default_formatter,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id]

# Use Jason for JSON parsing in Phoenix
config :phoenix, :json_library, Jason

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
import_config "#{config_env()}.exs"
