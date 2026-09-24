import Config

config :crm_demo, landing_dir: "/var/lib/favn/stress/landing"
config :favn_runner, build_profile: "prod"
config :favn, :duckdb_adbc,
  driver: "/opt/duckdb/1.5.5/libduckdb.so",
  entrypoint: "duckdb_adbc_init"

config :favn, execution_pools: [duckdb: [max_concurrency: 5]]

config :favn, pipeline_modules: [CrmDemo.RegistrationStress.Pipeline], schedule_modules: []

config :favn,
  discovery: [apps: [:crm_demo], assets: :all, connections: :all],
  runner_plugins: [{FavnDuckdbADBC, execution_mode: :in_process}]
