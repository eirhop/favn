import Config

workspace_id = System.get_env("CRM_EXAMPLE_WORKSPACE_ID", "local-dev")

duckdb_path =
  case config_env() do
    :test -> System.get_env("CRM_EXAMPLE_TEST_DUCKDB_PATH", "generic_crm_test.duckdb")
    _other -> System.get_env("CRM_EXAMPLE_DUCKDB_PATH", "generic_crm.duckdb")
  end

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
    warehouse: [
      open: [database: duckdb_path],
      pool: [enabled: true, max_idle_per_key: 1]
    ]
  ],
  execution_pools: [
    local_duckdb: [max_concurrency: 1]
  ],
  dev: [workspace_id: workspace_id],
  runner_plugins: [
    {FavnDuckdbADBC, execution_mode: :in_process}
  ]

config :favn_runner, build_profile: "source"

if config_env() == :test do
  System.put_env(
    "FAVN_RUNNER_RELEASE_ID",
    "rr_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  )
end
