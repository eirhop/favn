import Config

# Every value below has a working default so the tutorial runs with no setup.
# Override an environment variable when you want a second isolated stack.

if driver = System.get_env("DUCKDB_ADBC_DRIVER") do
  config :favn, :duckdb_adbc, driver: driver, entrypoint: "duckdb_adbc_init"
end

# One DuckDB catalog per warehouse phase, so a relation addresses as
# `catalog.schema.name` — see the README's "The data flow". A catalog is a real
# attached database, so each phase needs its own file and its own trusted ATTACH
# script; the namespace modules under `lib/crm_demo/warehouse/` declare which is
# which.
#
# The connection itself opens `:memory:`. Every asset lives in one of the three
# attached catalogs, so a file here would only ever be an empty fourth database.
warehouse_connection = fn prefix ->
  [
    open: [database: ":memory:"],
    duckdb: [
      resources: [
        source_catalog: [
          file: {:priv, :crm_demo, "duckdb/source_catalog.sql"},
          params: [database_path: "#{prefix}_source.duckdb"]
        ],
        core_catalog: [
          file: {:priv, :crm_demo, "duckdb/core_catalog.sql"},
          params: [database_path: "#{prefix}_core.duckdb"]
        ],
        mart_catalog: [
          file: {:priv, :crm_demo, "duckdb/mart_catalog.sql"},
          params: [database_path: "#{prefix}_mart.duckdb"]
        ]
      ],
      catalogs: [
        source: [resource: :source_catalog],
        core: [resource: :core_catalog],
        mart: [resource: :mart_catalog]
      ]
    ]
  ]
end

case config_env() do
  :test ->
    config :logger, level: :warning
    config :crm_demo, landing_dir: ".data/test/landing"
    config :favn, connections: [warehouse: warehouse_connection.("crm_demo_test")]

  _other ->
    config :crm_demo, landing_dir: ".data/landing"
    config :favn, connections: [warehouse: warehouse_connection.("crm_demo")]
end

config :favn,
  discovery: [
    apps: [:crm_demo],
    assets: :all,
    pipelines: :all,
    schedules: :all,
    connections: :all
  ],
  # One slot serializes every write across the file-backed DuckDB catalogs.
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
