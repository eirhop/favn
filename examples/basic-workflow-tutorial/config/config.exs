import Config

case System.get_env("DUCKDB_ADBC_DRIVER") do
  driver when is_binary(driver) and driver != "" ->
    config :favn, :duckdb_adbc,
      driver: driver,
      entrypoint: "duckdb_adbc_init"

  _missing ->
    :ok
end

config :favn,
  discovery: [
    apps: [:basic_workflow_tutorial],
    assets: :all,
    pipelines: :all,
    schedules: :all,
    connections: :all
  ],
  connections: [
    warehouse: [open: [database: "generic_crm.duckdb"]]
  ],
  execution_pools: [
    local_landing_write: [max_concurrency: 1]
  ],
  local: [workspace_id: "local-dev"],
  runner_plugins: [
    {FavnDuckdbADBC, execution_mode: :in_process}
  ]
