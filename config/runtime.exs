import Config

# Each production release validates only its role-owned environment before its
# supervision tree starts. Both flags are present in the shared release config;
# only the application started by the selected release consumes its flag.
if config_env() == :prod do
  config :favn_orchestrator, production_runtime_config: true

  config :favn_view, production_runtime_config: true
end
