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

config :favn_view,
  session_cookie_options: [
    store: :cookie,
    key: "_favn_view_key",
    signing_salt: "zqy+dPTK",
    encryption_salt: "favn-view-session-v1",
    same_site: "Lax",
    http_only: true,
    secure: Mix.env() == :prod
  ]

config :phoenix, :json_library, Jason

config :favn_storage_postgres,
  environment: config_env(),
  enforce_runtime_role: config_env() == :prod

config :favn_orchestrator,
  persistence_backend: Module.concat([FavnStoragePostgres, Backend]),
  persistence_options: []

config :favn_view, FavnView.Endpoint,
  url: [host: "localhost"],
  adapter: Bandit.PhoenixAdapter,
  render_errors: [
    formats: [html: FavnView.ErrorHTML, json: FavnView.ErrorJSON],
    layout: false
  ],
  pubsub_server: FavnView.PubSub,
  live_view: [signing_salt: "Pqi8zx5Q"]

config :esbuild,
  version: "0.25.4",
  favn_view: [
    args:
      ~w(js/app.js --bundle --target=es2022 --outdir=../priv/static/assets/js --external:/fonts/* --external:/images/* --alias:@=.),
    cd: Path.expand("../apps/favn_view/assets", __DIR__),
    env: %{"NODE_PATH" => [Path.expand("../deps", __DIR__), Mix.Project.build_path()]}
  ]

config :tailwind,
  version: "4.1.12",
  favn_view: [
    args: ~w(
      --input=assets/css/app.css
      --output=priv/static/assets/css/app.css
    ),
    cd: Path.expand("../apps/favn_view", __DIR__)
  ]

config :argon2_elixir,
  argon2_type: 2,
  t_cost: if(Mix.env() == :test, do: 1, else: 2),
  m_cost: if(Mix.env() == :test, do: 8, else: 15),
  parallelism: 1

config :favn_orchestrator,
  api_server: [
    enabled: false,
    host: "127.0.0.1",
    port: 4101
  ],
  manifest_publication: [
    compressed_limit_bytes: 8 * 1024 * 1024,
    decompressed_limit_bytes: 32 * 1024 * 1024
  ],
  api_service_tokens: [],
  api_service_tokens_env: "",
  auth_session_ttl_seconds: 43_200,
  auth_bootstrap_username: "",
  auth_bootstrap_password: "",
  auth_bootstrap_display_name: "Favn Admin",
  auth_bootstrap_roles: [:admin]

# Sample configuration:
#
#     config :logger, :default_handler,
#       level: :info
#
#     config :logger, :default_formatter,
#       format: "$date $time [$level] $metadata$message\n",
#       metadata: [:user_id]
config :logger, :default_formatter,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id]

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
import_config "#{config_env()}.exs"
