import Config

# The production release uses one typed loader after this runtime configuration
# has been evaluated and before either control-plane supervision tree starts.
# Keep deployment environment parsing in that loader so System.get_env/0 is read
# exactly once for the combined Orchestrator/View configuration.
if config_env() == :prod do
  config :favn_orchestrator,
    production_runtime_config: true,
    control_plane_runtime_config: true

  config :favn_view, production_runtime_config: true
end
