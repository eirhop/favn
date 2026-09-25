import Config

resources =
  for catalog <- [:source, :core, :mart] do
    {String.to_atom("#{catalog}_catalog"),
     [file: {:priv, :crm_demo, "duckdb/#{catalog}_catalog.sql"},
      params: [metadata: Favn.RuntimeConfig.Ref.secret_env!("FAVN_STRESS_CATALOG")]]}
  end

config :favn,
  execution_pools: [duckdb: [max_concurrency: 5]],
  connections: [
    warehouse: [
      open: [database: ":memory:"],
      duckdb: [
        startup: [file: {:priv, :crm_demo, "duckdb/stress_startup.sql"}],
        resources: resources,
        catalogs: [
          source: [resource: :source_catalog, write_concurrency: 5, write_scope: "stress-source"],
          core: [resource: :core_catalog, write_concurrency: 5, write_scope: "stress-core"],
          mart: [resource: :mart_catalog, write_concurrency: 5, write_scope: "stress-mart"]
        ]
      ]
    ]
  ]
