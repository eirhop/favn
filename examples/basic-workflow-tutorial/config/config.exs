import Config

config :favn,
  discovery: [
    apps: [:basic_workflow_tutorial],
    assets: :all,
    pipelines: :all,
    schedules: :all,
    connections: :all
  ],
  connections: [
    warehouse: [database: ".favn/data/reference_workload.duckdb", write_concurrency: 1]
  ],
  local: [workspace_id: "local-dev"],
  runner_plugins: [
    {FavnDuckdb, execution_mode: :in_process}
  ]
