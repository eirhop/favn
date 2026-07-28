import Config

# Every value below has a working default so the tutorial runs with no setup.
# Override an environment variable when you want a second isolated stack.

if driver = System.get_env("DUCKDB_ADBC_DRIVER") do
  config :favn, :duckdb_adbc, driver: driver, entrypoint: "duckdb_adbc_init"
end

case config_env() do
  :test ->
    config :logger, level: :warning
    config :crm_demo, landing_dir: ".data/test/landing"
    config :favn, connections: [warehouse: [open: [database: "crm_demo_test.duckdb"]]]

  _other ->
    config :crm_demo, landing_dir: ".data/landing"
    config :favn, connections: [warehouse: [open: [database: "crm_demo.duckdb"]]]
end

config :favn,
  discovery: [
    apps: [:crm_demo],
    assets: :all,
    pipelines: :all,
    schedules: :all,
    connections: :all
  ],
  # One slot serializes every write to the single file-backed DuckDB catalog.
  execution_pools: [duckdb: [max_concurrency: 1]],
  dev: [workspace_id: System.get_env("CRM_DEMO_WORKSPACE_ID", "local-dev")],
  runner_plugins: [{FavnDuckdbADBC, execution_mode: :in_process}]

config :favn_runner, build_profile: "source"

if config_env() == :test do
  System.put_env(
    "FAVN_RUNNER_RELEASE_ID",
    "rr_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  )
end
