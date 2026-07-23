import Config

config :favn,
  asset_modules: [],
  pipeline_modules: [],
  schedule_modules: [],
  connection_modules: [],
  runner_plugins: [{FavnDuckdbADBC, execution_mode: :in_process}]

config :favn, :duckdb_adbc,
  driver: "/opt/duckdb/1.5.4/libduckdb.so",
  entrypoint: "duckdb_adbc_init"
