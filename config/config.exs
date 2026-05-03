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

if Mix.env() == :test do
  config :tzdata, :autoupdate, :disabled
end

service_token_env_default =
  if Mix.env() == :test,
    do: "favn_web:favn-web-local-credential-1234567890abcdef",
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

# Sample configuration:
#
#     config :logger, :default_handler,
#       level: :info
#
#     config :logger, :default_formatter,
#       format: "$date $time [$level] $metadata$message\n",
#       metadata: [:user_id]
#
